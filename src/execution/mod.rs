// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`ExecutionContext`]: DataFusion based execution context for running SQL queries
//!
use std::sync::Arc;

use color_eyre::eyre::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor};
use datafusion::prelude::*;
use datafusion::sql::parser::Statement;
use log::info;
use tokio_stream::StreamExt;
#[cfg(feature = "flightsql")]
use {
    arrow_flight::sql::client::FlightSqlServiceClient, tokio::sync::Mutex,
    tonic::transport::Channel,
};

use crate::config::ExecutionConfig;
use crate::extensions::{enabled_extensions, DftSessionStateBuilder};

/// Structure for executing queries either locally or remotely (via FlightSQL)
///
/// This context includes both:
///
/// 1. The configuration of a [`SessionContext`] with  various extensions enabled
///
/// 2. The code for running SQL queries
///
/// The design goals for this module are to serve as an example of how to integrate
/// DataFusion into an application and to provide a simple interface for running SQL queries
/// with the various extensions enabled.
///
/// Thus it is important (eventually) not depend on the code in the app crate
pub struct ExecutionContext {
    session_ctx: SessionContext,
    #[cfg(feature = "flightsql")]
    flightsql_client: Mutex<Option<FlightSqlServiceClient<Channel>>>,
}

impl std::fmt::Debug for ExecutionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionContext").finish()
    }
}

impl ExecutionContext {
    /// Construct a new `ExecutionContext` with the specified configuration
    pub fn try_new(config: &ExecutionConfig) -> Result<Self> {
        let mut builder = DftSessionStateBuilder::new();
        for extension in enabled_extensions() {
            builder = extension.register(config, builder)?;
        }

        let state = builder.build()?;
        let session_ctx = SessionContext::new_with_state(state);

        Ok(Self {
            session_ctx,
            #[cfg(feature = "flightsql")]
            flightsql_client: Mutex::new(None),
        })
    }

    pub fn create_tables(&mut self) -> Result<()> {
        Ok(())
    }

    /// Return the inner DataFusion [`SessionContext`]
    pub fn session_ctx(&self) -> &SessionContext {
        &self.session_ctx
    }

    /// Return a handle to the underlying FlightSQL client, if any
    #[cfg(feature = "flightsql")]
    pub fn flightsql_client(&self) -> &Mutex<Option<FlightSqlServiceClient<Channel>>> {
        &self.flightsql_client
    }

    /// Executes the specified sql string, driving it to completion but discarding any results
    pub async fn execute_sql_and_discard_results(
        &self,
        sql: &str,
    ) -> datafusion::error::Result<()> {
        let mut stream = self.execute_sql(sql).await?;
        // note we don't call collect() to avoid buffering data
        while let Some(maybe_batch) = stream.next().await {
            maybe_batch?; // check for errors
        }
        Ok(())
    }

    /// Executes the specified sql string, returning the resulting
    /// [`SendableRecordBatchStream`] of results
    pub async fn execute_sql(
        &self,
        sql: &str,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        self.session_ctx.sql(sql).await?.execute_stream().await
    }

    /// Executes the a pre-parsed DataFusion [`Statement`], returning the
    /// resulting [`SendableRecordBatchStream`] of results
    pub async fn execute_statement(
        &self,
        statement: Statement,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let plan = self
            .session_ctx
            .state()
            .statement_to_plan(statement)
            .await?;
        self.session_ctx
            .execute_logical_plan(plan)
            .await?
            .execute_stream()
            .await
    }
}

// #[derive(Debug, Clone)]
// pub struct ExecMetrics {
//     name: String,
//     bytes_scanned: usize,
// }

#[derive(Clone, Debug)]
pub struct ExecutionStats {
    bytes_scanned: usize,
    // exec_metrics: Vec<ExecMetrics>,
}

impl ExecutionStats {
    pub fn bytes_scanned(&self) -> usize {
        self.bytes_scanned
    }
}

#[derive(Default)]
struct PlanVisitor {
    total_bytes_scanned: usize,
    // exec_metrics: Vec<ExecMetrics>,
}

impl From<PlanVisitor> for ExecutionStats {
    fn from(value: PlanVisitor) -> Self {
        Self {
            bytes_scanned: value.total_bytes_scanned,
        }
    }
}

impl ExecutionPlanVisitor for PlanVisitor {
    type Error = datafusion_common::DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        match plan.metrics() {
            Some(metrics) => match metrics.sum_by_name("bytes_scanned") {
                Some(bytes_scanned) => {
                    info!("Adding {} to total_bytes_scanned", bytes_scanned.as_usize());
                    self.total_bytes_scanned += bytes_scanned.as_usize();
                }
                None => {
                    info!("No bytes_scanned for {}", plan.name())
                }
            },
            None => {
                info!("No MetricsSet for {}", plan.name())
            }
        }
        Ok(true)
    }
}

pub fn collect_plan_stats(plan: Arc<dyn ExecutionPlan>) -> Option<ExecutionStats> {
    let mut visitor = PlanVisitor::default();
    if visit_execution_plan(plan.as_ref(), &mut visitor).is_ok() {
        Some(visitor.into())
    } else {
        None
    }
}

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

use std::sync::Arc;
use std::time::Duration;

use color_eyre::eyre::Result;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{execute_stream, visit_execution_plan, ExecutionPlanVisitor};
use datafusion::prelude::*;
use datafusion::sql::parser::Statement;
use datafusion::{arrow::util::pretty::pretty_format_batches, physical_plan::ExecutionPlan};
#[cfg(feature = "deltalake")]
use deltalake::delta_datafusion::DeltaTableFactory;
use log::info;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "flightsql")]
use {
    arrow_flight::sql::client::FlightSqlServiceClient, tokio::sync::Mutex,
    tonic::transport::Channel,
};
#[cfg(feature = "s3")]
use {log::error, url::Url};

use super::config::ExecutionConfig;
use super::state::tabs::sql::Query;
use super::AppEvent;

pub struct ExecutionContext {
    pub session_ctx: SessionContext,
    pub config: ExecutionConfig,
    pub cancellation_token: CancellationToken,
    #[cfg(feature = "flightsql")]
    pub flightsql_client: Mutex<Option<FlightSqlServiceClient<Channel>>>,
}

impl ExecutionContext {
    #[allow(unused_mut)]
    pub fn new(config: ExecutionConfig) -> Self {
        let cfg = SessionConfig::default()
            .with_batch_size(1)
            .with_information_schema(true);

        let runtime_env = RuntimeEnv::default();

        #[cfg(feature = "s3")]
        {
            if let Some(object_store_config) = &config.object_store {
                if let Some(s3_configs) = &object_store_config.s3 {
                    info!("S3 configs exists");
                    for s3_config in s3_configs {
                        match s3_config.to_object_store() {
                            Ok(object_store) => {
                                info!("Created object store");
                                if let Some(object_store_url) = s3_config.object_store_url() {
                                    info!("Endpoint exists");
                                    if let Ok(parsed_endpoint) = Url::parse(object_store_url) {
                                        info!("Parsed endpoint");
                                        runtime_env.register_object_store(
                                            &parsed_endpoint,
                                            Arc::new(object_store),
                                        );
                                        info!("Registered s3 object store");
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Error creating object store: {:?}", e);
                            }
                        }
                    }
                }
            }
        }

        let mut state = SessionStateBuilder::new()
            .with_default_features()
            .with_runtime_env(runtime_env.into())
            .with_config(cfg)
            .build();

        #[cfg(feature = "deltalake")]
        {
            state
                .table_factories_mut()
                .insert("DELTATABLE".to_string(), Arc::new(DeltaTableFactory {}));
        }

        {
            let session_ctx = SessionContext::new_with_state(state);
            let cancellation_token = CancellationToken::new();

            Self {
                config,
                session_ctx,
                cancellation_token,
                #[cfg(feature = "flightsql")]
                flightsql_client: Mutex::new(None),
            }
        }
    }

    pub fn create_tables(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn session_ctx(&self) -> &SessionContext {
        &self.session_ctx
    }

    pub async fn run_sqls(&self, sqls: Vec<&str>, sender: UnboundedSender<AppEvent>) -> Result<()> {
        let statement_count = sqls.len();
        for (i, sql) in sqls.into_iter().enumerate() {
            if sql.is_empty() {
                info!("Query is empty, skipping");
                continue;
            }
            info!("Running query {}", i);
            let _sender = sender.clone();
            let mut query =
                Query::new(sql.to_string(), None, None, None, Duration::default(), None);
            let start = std::time::Instant::now();
            if i == statement_count - 1 {
                info!("Executing last query and display results");
                if let Ok(df) = self.session_ctx.sql(sql).await {
                    if let Ok(physical_plan) = df.create_physical_plan().await {
                        let stream_cfg = SessionConfig::default();
                        let stream_task_ctx =
                            TaskContext::default().with_session_config(stream_cfg);
                        let mut stream =
                            execute_stream(physical_plan, stream_task_ctx.into()).unwrap();
                        let mut batches = Vec::new();
                        while let Some(maybe_batch) = stream.next().await {
                            match maybe_batch {
                                Ok(batch) => {
                                    batches.push(batch);
                                }
                                Err(e) => {
                                    let elapsed = start.elapsed();
                                    query.set_error(Some(e.to_string()));
                                    query.set_execution_time(elapsed);
                                    break;
                                }
                            }
                        }
                        let elapsed = start.elapsed();
                        let rows: usize = batches.iter().map(|r| r.num_rows()).sum();
                        query.set_results(Some(batches));
                        query.set_num_rows(Some(rows));
                        query.set_execution_time(elapsed);
                    }
                } else {
                    info!("Executing query and discarding results");
                    self.execute_sql(sql, false).await?;
                    let elapsed = start.elapsed();
                    query.set_execution_time(elapsed);
                }
            } else {
                self.execute_sql(sql, false).await?;
                let elapsed = start.elapsed();
                query.set_execution_time(elapsed);
            }
            _sender.send(AppEvent::QueryResult(query)).unwrap();
        }
        Ok(())
    }

    /// Execcutes the specified parsed DataFusion statement and discards the result
    pub async fn execute_sql(&self, sql: &str, print: bool) -> Result<()> {
        let df = self.session_ctx.sql(sql).await?;
        self.execute_stream_dataframe(df, print).await
    }

    /// Executes the specified parsed DataFusion statement and prints the result to stdout
    pub async fn execute_and_print_statement(&self, statement: Statement) -> Result<()> {
        let plan = self
            .session_ctx
            .state()
            .statement_to_plan(statement)
            .await?;
        let df = self.session_ctx.execute_logical_plan(plan).await?;
        self.execute_stream_dataframe(df, true).await
    }

    /// Executes the specified query and prints the result to stdout
    pub async fn execute_and_print_stream_sql(&self, query: &str) -> Result<()> {
        let df = self.session_ctx.sql(query).await?;
        self.execute_stream_dataframe(df, true).await
    }

    pub async fn execute_stream_dataframe(&self, df: DataFrame, print: bool) -> Result<()> {
        let physical_plan = df.create_physical_plan().await?;
        let stream_cfg = SessionConfig::default();
        let stream_task_ctx = TaskContext::default().with_session_config(stream_cfg);
        let mut stream = execute_stream(physical_plan, stream_task_ctx.into()).unwrap();

        while let Some(maybe_batch) = stream.next().await {
            if print {
                let batch = maybe_batch.unwrap();
                let d = pretty_format_batches(&[batch]).unwrap();
                println!("{}", d);
            } else {
                let _ = maybe_batch.unwrap();
                info!("Discarding batch");
            }
        }
        Ok(())
    }

    pub async fn show_catalog(&self) -> Result<()> {
        let tables = self.session_ctx.sql("SHOW tables").await?.collect().await?;
        let formatted = pretty_format_batches(&tables).unwrap();
        println!("{}", formatted);
        Ok(())
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

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

pub use datafusion_app::{collect_plan_io_stats, ExecutionStats};

use color_eyre::Result;
use datafusion::prelude::*;
#[cfg(feature = "flightsql")]
use datafusion_app::flightsql::{FlightSQLClient, FlightSQLContext};
use datafusion_app::{local::ExecutionContext, ExecOptions, ExecResult};

/// Provides all core execution functionality for execution queries from either a local
/// `SessionContext` or a remote `FlightSQL` service
#[derive(Clone, Debug)]
pub struct AppExecution {
    local: ExecutionContext,
    #[cfg(feature = "flightsql")]
    flightsql: FlightSQLContext,
}

impl AppExecution {
    pub fn new(local: ExecutionContext) -> Self {
        Self {
            local,
            #[cfg(feature = "flightsql")]
            flightsql: FlightSQLContext::default(),
        }
    }

    pub fn execution_ctx(&self) -> &ExecutionContext {
        &self.local
    }

    pub fn session_ctx(&self) -> &SessionContext {
        self.local.session_ctx()
    }

    #[cfg(feature = "flightsql")]
    pub fn flightsql_client(&self) -> &FlightSQLClient {
        self.flightsql.client()
    }

    #[cfg(feature = "flightsql")]
    pub fn flightsql_ctx(&self) -> &FlightSQLContext {
        &self.flightsql
    }

    #[cfg(feature = "flightsql")]
    pub fn with_flightsql_ctx(&mut self, flightsql_ctx: FlightSQLContext) {
        self.flightsql = flightsql_ctx;
    }

    pub async fn execute_sql_with_opts(&self, sql: &str, opts: ExecOptions) -> Result<ExecResult> {
        #[cfg(feature = "flightsql")]
        if opts.flightsql {
            return self
                .flightsql
                .execute_sql_with_opts(sql, opts)
                .await
                .map_err(|e| e.into());
        }

        // If flightsql is not enabled or `opts.flightsql` is false, fall back to local:
        self.local
            .execute_sql_with_opts(sql, opts)
            .await
            .map_err(|e| e.into())
    }
}

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

//! Tests for extensions (stored in the `extension_cases` directory)

#[cfg(feature = "deltalake")]
mod deltalake;
#[cfg(feature = "flightsql")]
mod flightsql;
#[cfg(feature = "functions-json")]
mod functions_json;
// #[cfg(feature = "hudi")]
// mod hudi;
#[cfg(feature = "flightsql")]
mod auth_basic;
#[cfg(feature = "flightsql")]
mod auth_bearer;
#[cfg(feature = "huggingface")]
mod huggingface;
#[cfg(feature = "s3")]
mod s3;
#[cfg(feature = "udfs-wasm")]
mod udfs_wasm;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::Result;
use datafusion::sql::parser::DFParser;
use datafusion_app::{extensions::DftSessionStateBuilder, local::ExecutionContext};
use datafusion_dft::config::AppConfig;
use futures::{StreamExt, TryStreamExt};
use log::debug;

/// Encapsulates an `ExecutionContext` for running queries in tests
pub struct TestExecution {
    execution: ExecutionContext,
}

#[allow(dead_code)]
impl TestExecution {
    pub async fn new() -> Self {
        let config = AppConfig::default();

        let session_state = DftSessionStateBuilder::try_new(Some(config.cli.execution.clone()))
            .unwrap()
            .with_extensions()
            .await
            .unwrap()
            .build()
            .unwrap();
        let execution = ExecutionContext::try_new(
            &config.cli.execution,
            session_state,
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
        )
        .unwrap();
        Self { execution }
    }

    /// Run the setup SQL query, discarding the result
    pub async fn with_setup(self, sql: &str) -> Self {
        debug!("Running setup query: {sql}");
        let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
        let statements =
            DFParser::parse_sql_with_dialect(sql, &dialect).expect("Error parsing setup query");
        for statement in statements {
            debug!("Running setup statement: {statement}");

            let mut stream = self
                .execution
                .execute_sql(sql)
                .await
                .expect("Error planning setup failed");
            while let Some(batch) = stream.next().await {
                let _ = batch.expect("Error executing setup query");
            }
        }
        self
    }

    /// run the specified SQL query, returning the result as a Vec of [`RecordBatch`]
    pub async fn run(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        debug!("Running query: {sql}");
        self.execution
            .execute_sql(sql)
            .await
            .expect("Error planning query failed")
            .try_collect()
            .await
    }

    /// Runs the specified SQL query, returning the result as a Vec<String>
    /// suitable for comparison with insta
    pub async fn run_and_format(&self, sql: &str) -> Vec<String> {
        format_results(&self.run(sql).await.expect("Error running query"))
    }
}

/// Formats the record batches into a Vec<String> suitable for comparison with insta
#[allow(dead_code)]
fn format_results(results: &[RecordBatch]) -> Vec<String> {
    let formatted = pretty_format_batches(results).unwrap().to_string();

    formatted.lines().map(|s| s.to_string()).collect()
}

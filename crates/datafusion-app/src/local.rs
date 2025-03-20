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

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use color_eyre::eyre::eyre;
use datafusion::catalog::MemoryCatalogProvider;
use datafusion::logical_expr::LogicalPlan;
use futures::TryFutureExt;
use log::{debug, error, info};

use crate::catalog::create_catalog;
use crate::config::ExecutionConfig;
use crate::{ExecOptions, ExecResult};
use color_eyre::eyre::{self, Result};
use datafusion::common::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, SessionState};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::*;
use datafusion::sql::parser::{DFParser, Statement};
use tokio_stream::StreamExt;

use super::executor::dedicated::DedicatedExecutor;
use super::local_benchmarks::LocalBenchmarkStats;
use super::stats::{ExecutionDurationStats, ExecutionStats};
#[cfg(feature = "udfs-wasm")]
use super::wasm::create_wasm_udfs;
#[cfg(feature = "observability")]
use crate::observability::ObservabilityContext;

/// Structure for executing queries locally
///
/// This context includes both:
///
/// 1. The configuration of a [`SessionContext`] with various extensions enabled
///
/// 2. The code for running SQL queries
///
/// The design goals for this module are to serve as an example of how to integrate
/// DataFusion into an application and to provide a simple interface for running SQL queries
/// with the various extensions enabled.
///
/// Thus it is important (eventually) not depend on the code in the app crate
#[derive(Clone)]
pub struct ExecutionContext {
    config: ExecutionConfig,
    /// Underlying `SessionContext`
    session_ctx: SessionContext,
    /// Path to the configured DDL file
    ddl_path: Option<PathBuf>,
    /// Dedicated executor for running CPU intensive work
    executor: Option<DedicatedExecutor>,
    /// Observability handlers
    #[cfg(feature = "observability")]
    observability: ObservabilityContext,
}

impl std::fmt::Debug for ExecutionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionContext").finish()
    }
}

impl ExecutionContext {
    /// Construct a new `ExecutionContext` with the specified configuration
    pub fn try_new(config: &ExecutionConfig, session_state: SessionState) -> Result<Self> {
        let mut executor = None;
        if config.dedicated_executor_enabled {
            // Ideally we would only use `enable_time` but we are still doing
            // some network requests as part of planning / execution which require network
            // functionality.

            let runtime_builder = tokio::runtime::Builder::new_multi_thread();
            let dedicated_executor =
                DedicatedExecutor::new("cpu_runtime", config.clone(), runtime_builder);
            executor = Some(dedicated_executor)
        }

        #[cfg(any(feature = "udfs-wasm", feature = "observability"))]
        let mut session_ctx = SessionContext::new_with_state(session_state);
        #[cfg(not(feature = "udfs-wasm"))]
        let session_ctx = SessionContext::new_with_state(session_state);

        #[cfg(feature = "functions-json")]
        datafusion_functions_json::register_all(&mut session_ctx)?;

        // Register Parquet Metadata Function
        let session_ctx = session_ctx.enable_url_table();

        #[cfg(feature = "udfs-wasm")]
        {
            let wasm_udfs = create_wasm_udfs(&config.wasm_udf)?;
            for wasm_udf in wasm_udfs {
                session_ctx.register_udf(wasm_udf);
            }
        }

        session_ctx.register_udtf(
            "parquet_metadata",
            Arc::new(datafusion_functions_parquet::ParquetMetadataFunc {}),
        );

        let catalog = create_app_catalog(config);
        session_ctx.register_catalog(&config.catalog.name, catalog);

        // #[cfg(feature = "observability")]
        // {
        //     let obs = ObservabilityContext::new(config.observability.clone());
        // }

        session_ctx.register_catalog("dft", catalog);

        #[cfg(feature = "observability")]
        let ctx = Self {
            config: config.clone(),
            session_ctx,
            ddl_path: config.ddl_path.as_ref().map(PathBuf::from),
            executor,
            observability: ObservabilityContext::default(),
        };

        #[cfg(not(feature = "observability"))]
        let ctx = Self {
            config: config.clone(),
            session_ctx,
            ddl_path: config.ddl_path.as_ref().map(PathBuf::from),
            executor,
        };

        Ok(ctx)
    }

    pub fn config(&self) -> &ExecutionConfig {
        &self.config
    }

    pub fn create_tables(&mut self) -> Result<()> {
        Ok(())
    }

    /// Return the inner DataFusion [`SessionContext`]
    pub fn session_ctx(&self) -> &SessionContext {
        &self.session_ctx
    }

    /// Return the inner [`DedicatedExecutor`]
    pub fn executor(&self) -> &Option<DedicatedExecutor> {
        &self.executor
    }

    /// Return the `ObservabilityCtx`
    #[cfg(feature = "observability")]
    pub fn observability(&self) -> &ObservabilityContext {
        &self.observability
    }

    /// Convert the statement to a `LogicalPlan`.  Uses the [`DedicatedExecutor`] if it is available.
    pub async fn statement_to_logical_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        let ctx = self.session_ctx.clone();
        let task = async move { ctx.state().statement_to_plan(statement).await };
        if let Some(executor) = &self.executor {
            let job = executor.spawn(task).map_err(|e| eyre::eyre!(e));
            let job_res = job.await?;
            job_res.map_err(|e| eyre!(e))
        } else {
            task.await.map_err(|e| eyre!(e))
        }
    }

    /// Executes the provided `LogicalPlan` returning a `SendableRecordBatchStream`.  Uses the [`DedicatedExecutor`] if it is available.
    pub async fn execute_logical_plan(
        &self,
        logical_plan: LogicalPlan,
    ) -> Result<SendableRecordBatchStream> {
        let ctx = self.session_ctx.clone();
        let task = async move {
            let df = ctx.execute_logical_plan(logical_plan).await?;
            df.execute_stream().await
        };
        if let Some(executor) = &self.executor {
            let job = executor.spawn(task).map_err(|e| eyre!(e));
            let job_res = job.await?;
            job_res.map_err(|e| eyre!(e))
        } else {
            task.await.map_err(|e| eyre!(e))
        }
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

    /// Create a physical plan from the specified SQL string.  This is useful if you want to store
    /// the plan and collect metrics from it.
    pub async fn create_physical_plan(
        &self,
        sql: &str,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let df = self.session_ctx.sql(sql).await?;
        df.create_physical_plan().await
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

    /// Load DDL from configured DDL path for execution (so strips out comments and empty lines)
    pub fn load_ddl(&self) -> Option<String> {
        info!("Loading DDL from: {:?}", &self.ddl_path);
        if let Some(ddl_path) = &self.ddl_path {
            if ddl_path.exists() {
                let maybe_ddl = std::fs::read_to_string(ddl_path);
                match maybe_ddl {
                    Ok(ddl) => Some(ddl),
                    Err(err) => {
                        error!("Error reading DDL: {:?}", err);
                        None
                    }
                }
            } else {
                info!("DDL path ({:?}) does not exist", ddl_path);
                None
            }
        } else {
            info!("No DDL file configured");
            None
        }
    }

    /// Save DDL to configured DDL path
    pub fn save_ddl(&self, ddl: String) {
        info!("Loading DDL from: {:?}", &self.ddl_path);
        if let Some(ddl_path) = &self.ddl_path {
            match std::fs::File::create(ddl_path) {
                Ok(mut f) => match f.write_all(ddl.as_bytes()) {
                    Ok(_) => {
                        info!("Saved DDL file")
                    }
                    Err(e) => {
                        error!("Error writing DDL file: {e}")
                    }
                },
                Err(e) => {
                    error!("Error creating or opening DDL file: {e}")
                }
            }
        } else {
            info!("No DDL file configured");
        }
    }

    /// Execute DDL statements sequentially
    pub async fn execute_ddl(&self) {
        match self.load_ddl() {
            Some(ddl) => {
                let ddl_statements = ddl.split(';').collect::<Vec<&str>>();
                for statement in ddl_statements {
                    if statement.trim().is_empty() {
                        continue;
                    }
                    if statement.trim().starts_with("--") {
                        continue;
                    }

                    debug!("Executing DDL statement: {:?}", statement);
                    match self.execute_sql_and_discard_results(statement).await {
                        Ok(_) => {
                            info!("DDL statement executed");
                        }
                        Err(e) => {
                            error!("Error executing DDL statement: {e}");
                        }
                    }
                }
            }
            None => {
                info!("No DDL to execute");
            }
        }
    }

    /// Benchmark the provided query.  Currently, only a single statement can be benchmarked
    pub async fn benchmark_query(
        &self,
        query: &str,
        cli_iterations: Option<usize>,
    ) -> Result<LocalBenchmarkStats> {
        let iterations = cli_iterations.unwrap_or(self.config.benchmark_iterations);
        info!("Benchmarking query with {} iterations", iterations);
        let mut rows_returned = Vec::with_capacity(iterations);
        let mut logical_planning_durations = Vec::with_capacity(iterations);
        let mut physical_planning_durations = Vec::with_capacity(iterations);
        let mut execution_durations = Vec::with_capacity(iterations);
        let mut total_durations = Vec::with_capacity(iterations);
        let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
        let statements = DFParser::parse_sql_with_dialect(query, &dialect)?;
        if statements.len() == 1 {
            for _ in 0..iterations {
                let statement = statements[0].clone();
                let start = std::time::Instant::now();
                let logical_plan = self
                    .session_ctx()
                    .state()
                    .statement_to_plan(statement)
                    .await?;
                let logical_planning_duration = start.elapsed();
                let physical_plan = self
                    .session_ctx()
                    .state()
                    .create_physical_plan(&logical_plan)
                    .await?;
                let physical_planning_duration = start.elapsed();
                let task_ctx = self.session_ctx().task_ctx();
                let mut stream = execute_stream(physical_plan, task_ctx)?;
                let mut rows = 0;
                while let Some(b) = stream.next().await {
                    rows += b?.num_rows();
                }
                rows_returned.push(rows);
                let execution_duration = start.elapsed();
                let total_duration = start.elapsed();
                logical_planning_durations.push(logical_planning_duration);
                physical_planning_durations
                    .push(physical_planning_duration - logical_planning_duration);
                execution_durations.push(execution_duration - physical_planning_duration);
                total_durations.push(total_duration);
            }
        } else {
            return Err(eyre::eyre!("Only a single statement can be benchmarked"));
        }

        Ok(LocalBenchmarkStats::new(
            query.to_string(),
            rows_returned,
            logical_planning_durations,
            physical_planning_durations,
            execution_durations,
            total_durations,
        ))
    }

    pub async fn analyze_query(&self, query: &str) -> Result<ExecutionStats> {
        let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
        let start = std::time::Instant::now();
        let statements = DFParser::parse_sql_with_dialect(query, &dialect)?;
        let parsing_duration = start.elapsed();
        if statements.len() == 1 {
            let statement = statements[0].clone();
            let logical_plan = self
                .session_ctx()
                .state()
                .statement_to_plan(statement.clone())
                .await?;
            let logical_planning_duration = start.elapsed();
            let physical_plan = self
                .session_ctx()
                .state()
                .create_physical_plan(&logical_plan)
                .await?;
            let physical_planning_duration = start.elapsed();
            let task_ctx = self.session_ctx().task_ctx();
            let mut stream = execute_stream(Arc::clone(&physical_plan), task_ctx)?;
            let mut rows = 0;
            let mut batches = 0;
            let mut bytes = 0;
            while let Some(b) = stream.next().await {
                let batch = b?;
                rows += batch.num_rows();
                batches += 1;
                bytes += batch.get_array_memory_size();
            }
            let execution_duration = start.elapsed();
            let durations = ExecutionDurationStats::new(
                parsing_duration,
                logical_planning_duration - parsing_duration,
                physical_planning_duration - logical_planning_duration,
                execution_duration - physical_planning_duration,
                start.elapsed(),
            );
            ExecutionStats::try_new(
                query.to_string(),
                durations,
                rows,
                batches,
                bytes,
                physical_plan,
            )
        } else {
            Err(eyre::eyre!("Only a single statement can be benchmarked"))
        }
    }

    pub async fn execute_sql_with_opts(
        &self,
        sql: &str,
        opts: ExecOptions,
    ) -> DFResult<ExecResult> {
        let df = self.session_ctx.sql(sql).await?;
        let df = if let Some(limit) = opts.limit {
            df.limit(0, Some(limit))?
        } else {
            df
        };
        Ok(ExecResult::RecordBatchStream(df.execute_stream().await?))
    }
}

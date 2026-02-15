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
//! [`CliApp`]: Command Line User Interface

mod progress;

use crate::config::AppConfig;
use crate::db::register_db;
use crate::{args::DftArgs, execution::AppExecution};
use color_eyre::eyre::eyre;
use color_eyre::Result;
use datafusion::arrow::array::{RecordBatch, RecordBatchWriter};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::writer::FileWriter as ArrowIpcWriter;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::arrow::{csv, json};
use datafusion::sql::parser::DFParser;
use datafusion_app::config::merge_configs;
use datafusion_app::extensions::DftSessionStateBuilder;
use datafusion_app::local::ExecutionContext;
use datafusion_app::local_benchmarks::LocalBenchmarkStats;
use futures::{Stream, StreamExt};
use log::info;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(feature = "flightsql")]
use {
    crate::args::{Command, FlightSqlCommand},
    datafusion_app::{
        config::{AuthConfig, FlightSQLConfig},
        flightsql::FlightSQLContext,
        flightsql_benchmarks::FlightSQLBenchmarkStats,
    },
    tonic::IntoRequest,
};
#[cfg(feature = "vortex")]
use {
    vortex::array::{arrow::FromArrowArray, ArrayRef},
    vortex_file::VortexWriteOptions,
    vortex_session::VortexSession,
};

const LOCAL_BENCHMARK_HEADER_ROW: &str =
    "query,runs,logical_planning_min,logical_planning_max,logical_planning_mean,logical_planning_median,logical_planning_percent_of_total,physical_planning_min,physical_planning_max,physical_planning,mean,physical_planning_median,physical_planning_percent_of_total,execution_min,execution_max,execution_execution_mean,execution_median,execution_percent_of_total,total_min,total_max,total_mean,total_median,total_percent_of_total,concurrency_mode";

#[cfg(feature = "flightsql")]
const FLIGHTSQL_BENCHMARK_HEADER_ROW: &str =
    "query,runs,get_flight_info_min,get_flight_info_max,get_flight_info_mean,get_flight_info_median,get_flight_info_percent_of_total,ttfb_min,ttfb_max,ttfb,mean,ttfb_median,ttfb_percent_of_total,do_get_min,do_get_max,do_get_mean,do_get_median,do_get_percent_of_total,total_min,total_max,total_mean,total_median,total_percent_of_total,concurrency_mode";

/// Encapsulates the command line interface
pub struct CliApp {
    /// Execution context for running queries
    app_execution: AppExecution,
    args: DftArgs,
}

impl CliApp {
    pub fn new(app_execution: AppExecution, args: DftArgs) -> Self {
        Self {
            app_execution,
            args,
        }
    }

    fn validate_args(&self) -> color_eyre::Result<()> {
        let more_than_one_command_or_file = (self.args.commands.len() > 1
            || self.args.files.len() > 1)
            && self.args.output.is_some();
        if more_than_one_command_or_file {
            return Err(eyre!(
                "Output can only be saved for a single file or command"
            ));
        }

        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn handle_flightsql_command(&self, command: FlightSqlCommand) -> color_eyre::Result<()> {
        use futures::stream;

        match command {
            FlightSqlCommand::StatementQuery { sql } => self.exec_from_flightsql(sql, 0).await,
            FlightSqlCommand::GetCatalogs => {
                let flight_info = self
                    .app_execution
                    .flightsql_ctx()
                    .get_catalogs_flight_info()
                    .await?;
                let streams = self
                    .app_execution
                    .flightsql_ctx()
                    .do_get(flight_info)
                    .await?;
                let flight_batch_stream = stream::select_all(streams);
                self.print_any_stream(flight_batch_stream).await;
                Ok(())
            }
            FlightSqlCommand::GetDbSchemas {
                catalog,
                db_schema_filter_pattern,
            } => {
                let flight_info = self
                    .app_execution
                    .flightsql_ctx()
                    .get_db_schemas_flight_info(catalog, db_schema_filter_pattern)
                    .await?;
                let streams = self
                    .app_execution
                    .flightsql_ctx()
                    .do_get(flight_info)
                    .await?;
                let flight_batch_stream = stream::select_all(streams);
                self.print_any_stream(flight_batch_stream).await;
                Ok(())
            }

            FlightSqlCommand::GetTables {
                catalog,
                db_schema_filter_pattern,
                table_name_filter_pattern,
                table_types,
            } => {
                let flight_info = self
                    .app_execution
                    .flightsql_ctx()
                    .get_tables_flight_info(
                        catalog,
                        db_schema_filter_pattern,
                        table_name_filter_pattern,
                        table_types.unwrap_or_default(),
                        false,
                    )
                    .await?;
                let streams = self
                    .app_execution
                    .flightsql_ctx()
                    .do_get(flight_info)
                    .await?;
                let flight_batch_stream = stream::select_all(streams);
                self.print_any_stream(flight_batch_stream).await;
                Ok(())
            }
            FlightSqlCommand::GetTableTypes => {
                let flight_info = self
                    .app_execution
                    .flightsql_ctx()
                    .get_table_types_flight_info()
                    .await?;
                let streams = self
                    .app_execution
                    .flightsql_ctx()
                    .do_get(flight_info)
                    .await?;
                let flight_batch_stream = stream::select_all(streams);
                self.print_any_stream(flight_batch_stream).await;
                Ok(())
            }
            FlightSqlCommand::GetSqlInfo { info } => {
                let flight_info = self
                    .app_execution
                    .flightsql_ctx()
                    .get_sql_info_flight_info(info)
                    .await?;
                let streams = self
                    .app_execution
                    .flightsql_ctx()
                    .do_get(flight_info)
                    .await?;
                let flight_batch_stream = stream::select_all(streams);
                self.print_any_stream(flight_batch_stream).await;
                Ok(())
            }
            FlightSqlCommand::GetXdbcTypeInfo { data_type } => {
                let flight_info = self
                    .app_execution
                    .flightsql_ctx()
                    .get_xdbc_type_info_flight_info(data_type)
                    .await?;
                let streams = self
                    .app_execution
                    .flightsql_ctx()
                    .do_get(flight_info)
                    .await?;
                let flight_batch_stream = stream::select_all(streams);
                self.print_any_stream(flight_batch_stream).await;
                Ok(())
            }
        }
    }

    /// Execute the provided sql, which was passed as an argument from CLI.
    ///
    /// Optionally, use the FlightSQL client for execution.
    pub async fn execute_files_or_commands(&self) -> color_eyre::Result<()> {
        if self.args.run_ddl {
            self.app_execution.execution_ctx().execute_ddl().await;
        }

        self.validate_args()?;

        #[cfg(feature = "flightsql")]
        if let Some(Command::FlightSql { command }) = &self.args.command {
            return self.handle_flightsql_command(command.clone()).await;
        };

        #[cfg(not(feature = "flightsql"))]
        match (
            self.args.files.is_empty(),
            self.args.commands.is_empty(),
            self.args.flightsql,
            self.args.bench,
            self.args.analyze || self.args.analyze_raw,
        ) {
            // Error cases
            (_, _, true, _, _) => Err(eyre!(
                "FLightSQL feature isn't enabled. Reinstall `dft` with `--features=flightsql`"
            )),
            (false, false, false, true, _) => {
                Err(eyre!("Cannot benchmark without a command or file"))
            }
            (true, true, _, _, _) => Err(eyre!("No files or commands provided to execute")),
            (false, false, _, false, false) => Err(eyre!(
                "Cannot execute both files and commands at the same time"
            )),
            (_, _, false, true, true) => Err(eyre!(
                "The `benchmark` and `analyze` flags are mutually exclusive"
            )),
            (false, false, _, _, true) => {
                Err(eyre!("Analyze requires exactly one command or file"))
            }

            // Execution cases
            (false, true, _, false, false) => self.execute_files(&self.args.files).await,
            (true, false, _, false, false) => self.execute_commands(&self.args.commands).await,

            // Benchmark cases
            (false, true, _, true, false) => self.benchmark_files(&self.args.files).await,
            (true, false, _, true, false) => self.benchmark_commands(&self.args.commands).await,

            // Analyze cases
            (false, true, _, false, true) => {
                if self.args.files.len() > 1 {
                    return Err(eyre!("Analyze requires exactly one file"));
                }
                self.analyze_files(&self.args.files).await
            }
            (true, false, _, false, true) => {
                if self.args.commands.len() > 1 {
                    return Err(eyre!("Analyze requires exactly one command"));
                }
                self.analyze_commands(&self.args.commands).await
            }
        }
        #[cfg(feature = "flightsql")]
        match (
            self.args.files.is_empty(),
            self.args.commands.is_empty(),
            self.args.flightsql,
            self.args.bench,
            self.args.analyze || self.args.analyze_raw,
        ) {
            // Error cases
            (true, true, _, _, _) => Err(eyre!("No files or commands provided to execute")),
            (false, false, false, true, _) => {
                Err(eyre!("Cannot benchmark without a command or file"))
            }
            (false, false, _, _, false) => Err(eyre!(
                "Cannot execute both files and commands at the same time"
            )),
            (_, _, _, true, true) => Err(eyre!(
                "The `benchmark` and `analyze` flags are mutually exclusive"
            )),
            (false, false, _, _, true) => {
                Err(eyre!("Analyze requires exactly one command or file"))
            }

            // Execution cases
            (true, false, false, false, false) => self.execute_commands(&self.args.commands).await,
            (false, true, false, false, false) => self.execute_files(&self.args.files).await,

            // FlightSQL execution cases
            (false, true, true, false, false) => {
                self.flightsql_execute_files(&self.args.files).await
            }
            (true, false, true, false, false) => {
                self.flightsql_execute_commands(&self.args.commands).await
            }

            // Benchmark cases
            (false, true, false, true, false) => self.benchmark_files(&self.args.files).await,
            (false, true, true, true, false) => {
                self.flightsql_benchmark_files(&self.args.files).await
            }
            (true, false, true, true, false) => {
                self.flightsql_benchmark_commands(&self.args.commands).await
            }
            (true, false, false, true, false) => self.benchmark_commands(&self.args.commands).await,

            // Analyze cases
            (true, false, false, false, true) => {
                if self.args.commands.len() > 1 {
                    return Err(eyre!("Analyze requires exactly one command"));
                }
                self.analyze_commands(&self.args.commands).await
            }
            (false, true, false, false, true) => {
                if self.args.files.len() > 1 {
                    return Err(eyre!("Analyze requires exactly one file"));
                }
                self.analyze_files(&self.args.files).await
            }
            (true, false, true, false, true) => {
                if self.args.commands.len() > 1 {
                    return Err(eyre!("Analyze requires exactly one command"));
                }
                self.flightsql_analyze_commands(&self.args.commands).await
            }
            (false, true, true, false, true) => {
                if self.args.files.len() > 1 {
                    return Err(eyre!("Analyze requires exactly one file"));
                }
                self.flightsql_analyze_files(&self.args.files).await
            }
        }
    }

    async fn execute_files(&self, files: &[PathBuf]) -> Result<()> {
        info!("Executing files: {:?}", files);
        for file in files {
            self.exec_from_file(file).await?
        }

        Ok(())
    }

    async fn benchmark_files(&self, files: &[PathBuf]) -> Result<()> {
        if let Some(run_before_query) = &self.args.run_before {
            self.app_execution
                .execution_ctx()
                .execute_sql_and_discard_results(run_before_query)
                .await?;
        }
        info!("Benchmarking files: {:?}", files);
        for file in files {
            let query = std::fs::read_to_string(file)?;
            let stats = self.benchmark_from_string(&query).await?;
            println!("{}", stats);
        }
        Ok(())
    }

    async fn analyze_files(&self, files: &[PathBuf]) -> Result<()> {
        info!("Analyzing files: {:?}", files);
        for file in files {
            let query = std::fs::read_to_string(file)?;
            self.analyze_from_string(&query).await?;
        }
        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_execute_files(&self, files: &[PathBuf]) -> color_eyre::Result<()> {
        info!("Executing FlightSQL files: {:?}", files);
        for (i, file) in files.iter().enumerate() {
            let file = std::fs::read_to_string(file)?;
            self.exec_from_flightsql(file, i).await?;
        }

        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_benchmark_files(&self, files: &[PathBuf]) -> Result<()> {
        info!("Benchmarking FlightSQL files: {:?}", files);

        let mut open_opts = std::fs::OpenOptions::new();
        let mut results_file = if let Some(p) = &self.args.save {
            if !p.exists() {
                if let Some(parent) = p.parent() {
                    std::fs::DirBuilder::new().recursive(true).create(parent)?;
                }
            };
            if self.args.append && p.exists() {
                open_opts.append(true).create(true);
                Some(open_opts.open(p)?)
            } else {
                open_opts.write(true).create(true).truncate(true);
                let mut file = open_opts.open(p)?;
                writeln!(file, "{}", FLIGHTSQL_BENCHMARK_HEADER_ROW)?;
                Some(file)
            }
        } else {
            None
        };

        for file in files {
            let query = std::fs::read_to_string(file)?;
            let stats = self.flightsql_benchmark_from_string(&query).await?;
            println!("{}", stats);
            if let Some(ref mut results_file) = &mut results_file {
                writeln!(results_file, "{}", stats.to_summary_csv_row())?
            }
        }

        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn exec_from_flightsql(&self, sql: String, i: usize) -> color_eyre::Result<()> {
        let client = self.app_execution.flightsql_client();
        let mut guard = client.lock().await;
        if let Some(client) = guard.as_mut() {
            let start = if self.args.time {
                Some(std::time::Instant::now())
            } else {
                None
            };
            let flight_info = client.execute(sql, None).await?;
            for endpoint in flight_info.endpoint {
                if let Some(ticket) = endpoint.ticket {
                    let stream = client.do_get(ticket.into_request()).await?;
                    if let Some(output_path) = &self.args.output {
                        self.output_stream(stream, output_path).await?
                    } else if let Some(start) = start {
                        self.exec_stream(stream).await;
                        let elapsed = start.elapsed();
                        println!("Query {i} executed in {:?}", elapsed);
                    } else {
                        self.print_any_stream(stream).await;
                    }
                }
            }
        } else {
            println!("No FlightSQL client configured.  Add one in `~/.config/dft/config.toml`");
        }

        Ok(())
    }

    async fn execute_commands(&self, commands: &[String]) -> color_eyre::Result<()> {
        info!("Executing commands: {:?}", commands);
        if let Some(run_before_query) = &self.args.run_before {
            self.app_execution
                .execution_ctx()
                .execute_sql_and_discard_results(run_before_query)
                .await?;
        }

        for command in commands {
            self.exec_from_string(command).await?
        }

        Ok(())
    }

    async fn benchmark_commands(&self, commands: &[String]) -> color_eyre::Result<()> {
        if let Some(run_before_query) = &self.args.run_before {
            self.app_execution
                .execution_ctx()
                .execute_sql_and_discard_results(run_before_query)
                .await?;
        }
        info!("Benchmarking commands: {:?}", commands);
        let mut open_opts = std::fs::OpenOptions::new();
        let mut file = if let Some(p) = &self.args.save {
            if !p.exists() {
                if let Some(parent) = p.parent() {
                    std::fs::DirBuilder::new().recursive(true).create(parent)?;
                }
            };
            if self.args.append && p.exists() {
                open_opts.append(true).create(true);
                Some(open_opts.open(p)?)
            } else {
                open_opts.write(true).create(true).truncate(true);
                let mut file = open_opts.open(p)?;
                writeln!(file, "{}", LOCAL_BENCHMARK_HEADER_ROW)?;
                Some(file)
            }
        } else {
            None
        };

        for command in commands {
            let stats = self.benchmark_from_string(command).await?;
            println!("{}", stats);
            if let Some(ref mut file) = &mut file {
                writeln!(file, "{}", stats.to_summary_csv_row())?;
            }
        }
        Ok(())
    }

    async fn analyze_commands(&self, commands: &[String]) -> color_eyre::Result<()> {
        info!("Analyzing commands: {:?}", commands);
        for command in commands {
            self.analyze_from_string(command).await?;
        }

        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_execute_commands(&self, commands: &[String]) -> color_eyre::Result<()> {
        info!("Executing FlightSQL commands: {:?}", commands);
        for (i, command) in commands.iter().enumerate() {
            self.exec_from_flightsql(command.to_string(), i).await?
        }

        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_benchmark_commands(&self, commands: &[String]) -> color_eyre::Result<()> {
        info!("Benchmark FlightSQL commands: {:?}", commands);

        let mut open_opts = std::fs::OpenOptions::new();
        let mut file = if let Some(p) = &self.args.save {
            if !p.exists() {
                if let Some(parent) = p.parent() {
                    std::fs::DirBuilder::new().recursive(true).create(parent)?;
                }
            };
            if self.args.append && p.exists() {
                open_opts.append(true).create(true);
                Some(open_opts.open(p)?)
            } else {
                open_opts.write(true).create(true).truncate(true);
                let mut file = open_opts.open(p)?;
                writeln!(file, "{}", FLIGHTSQL_BENCHMARK_HEADER_ROW)?;
                Some(file)
            }
        } else {
            None
        };

        for command in commands {
            let stats = self.flightsql_benchmark_from_string(command).await?;
            println!("{}", stats);
            if let Some(ref mut file) = &mut file {
                writeln!(file, "{}", stats.to_summary_csv_row())?
            }
        }

        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_analyze_commands(&self, commands: &[String]) -> Result<()> {
        info!("Analyzing FlightSQL commands: {:?}", commands);
        for command in commands {
            self.flightsql_analyze_from_string(command).await?;
        }
        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_analyze_files(&self, files: &[PathBuf]) -> Result<()> {
        info!("Analyzing FlightSQL files: {:?}", files);
        for file in files {
            let sql = std::fs::read_to_string(file)?;
            self.flightsql_analyze_from_string(&sql).await?;
        }
        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_analyze_from_string(&self, sql: &str) -> Result<()> {
        if self.args.analyze_raw {
            // Raw mode: print metrics table directly
            let (query_str, metrics_batch) = self
                .app_execution
                .flightsql_ctx()
                .analyze_query_raw(sql)
                .await?;

            println!("==================== Query ====================");
            println!("{}", query_str);
            println!("\n==================== Metrics ====================");
            self.print_batch(&metrics_batch)?;
        } else {
            // Normal mode: reconstruct and display ExecutionStats
            let stats = self
                .app_execution
                .flightsql_ctx()
                .analyze_query(sql)
                .await?;

            // Display using existing ExecutionStats::Display implementation
            println!("{}", stats);
        }
        Ok(())
    }

    #[cfg(feature = "flightsql")]
    fn print_batch(&self, batch: &datafusion::arrow::array::RecordBatch) -> Result<()> {
        use datafusion::arrow::util::pretty::print_batches;
        print_batches(std::slice::from_ref(batch))?;
        Ok(())
    }

    async fn exec_from_string(&self, sql: &str) -> Result<()> {
        let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
        let statements = DFParser::parse_sql_with_dialect(sql, &dialect)?;
        let start = if self.args.time {
            Some(std::time::Instant::now())
        } else {
            None
        };
        for (i, statement) in statements.into_iter().enumerate() {
            let stream = self
                .app_execution
                .execution_ctx()
                .execute_statement(statement)
                .await?;
            if let Some(output_path) = &self.args.output {
                self.output_stream(stream, output_path).await?;
            } else if let Some(start) = start {
                self.exec_stream(stream).await;
                let elapsed = start.elapsed();
                println!("Query {i} executed in {:?}", elapsed);
            } else {
                self.print_any_stream(stream).await;
            }
        }
        Ok(())
    }

    async fn benchmark_from_string(&self, sql: &str) -> Result<LocalBenchmarkStats> {
        use std::sync::Arc;

        // Calculate iterations and concurrency
        let iterations = self.args.benchmark_iterations.unwrap_or(
            self.app_execution
                .execution_ctx()
                .config()
                .benchmark_iterations,
        );
        let concurrency = if self.args.concurrent {
            let parallelism = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            std::cmp::min(iterations, parallelism)
        } else {
            1
        };

        // Create progress reporter
        let progress_reporter = Some(Arc::new(progress::IndicatifProgressReporter::new(
            sql,
            iterations,
            self.args.concurrent,
            concurrency,
        ))
            as Arc<dyn datafusion_app::local_benchmarks::BenchmarkProgressReporter>);

        // Call benchmark with reporter
        let stats = self
            .app_execution
            .execution_ctx()
            .benchmark_query(
                sql,
                self.args.benchmark_iterations,
                self.args.concurrent,
                progress_reporter,
            )
            .await?;
        Ok(stats)
    }

    async fn analyze_from_string(&self, sql: &str) -> Result<()> {
        let mut stats = self
            .app_execution
            .execution_ctx()
            .analyze_query(sql)
            .await?;
        stats.collect_stats();
        println!("{}", stats);
        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_benchmark_from_string(&self, sql: &str) -> Result<FlightSQLBenchmarkStats> {
        use std::sync::Arc;

        // Calculate iterations and concurrency
        // Use a default of 10 if not specified (matches default in FlightSQLConfig)
        let iterations = self.args.benchmark_iterations.unwrap_or(10);
        let concurrency = if self.args.concurrent {
            let parallelism = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            std::cmp::min(iterations, parallelism)
        } else {
            1
        };

        // Create progress reporter
        let progress_reporter = Some(Arc::new(progress::IndicatifProgressReporter::new(
            sql,
            iterations,
            self.args.concurrent,
            concurrency,
        ))
            as Arc<dyn datafusion_app::local_benchmarks::BenchmarkProgressReporter>);

        // Call benchmark with reporter
        let stats = self
            .app_execution
            .flightsql_ctx()
            .benchmark_query(
                sql,
                self.args.benchmark_iterations,
                self.args.concurrent,
                progress_reporter,
            )
            .await?;
        Ok(stats)
    }

    /// run and execute SQL statements and commands from a file, against a context
    /// with the given print options
    pub async fn exec_from_file(&self, file: &Path) -> color_eyre::Result<()> {
        let string = std::fs::read_to_string(file)?;

        self.exec_from_string(&string).await?;

        Ok(())
    }

    /// executes a sql statement and prints the result to stdout
    pub async fn execute_and_print_sql(&self, sql: &str) -> color_eyre::Result<()> {
        let stream = self.app_execution.execution_ctx().execute_sql(sql).await?;
        self.print_any_stream(stream).await;
        Ok(())
    }

    async fn exec_stream<S, E>(&self, mut stream: S)
    where
        S: Stream<Item = Result<RecordBatch, E>> + Unpin,
        E: Error,
    {
        while let Some(maybe_batch) = stream.next().await {
            match maybe_batch {
                Ok(_) => {}
                Err(e) => {
                    println!("Error executing SQL: {e}");
                    break;
                }
            }
        }
    }

    async fn print_any_stream<S, E>(&self, mut stream: S)
    where
        S: Stream<Item = Result<RecordBatch, E>> + Unpin,
        E: Error,
    {
        while let Some(maybe_batch) = stream.next().await {
            match maybe_batch {
                Ok(batch) => match pretty_format_batches(&[batch]) {
                    Ok(d) => println!("{}", d),
                    Err(e) => println!("Error formatting batch: {e}"),
                },
                Err(e) => println!("Error executing SQL: {e}"),
            }
        }
    }

    async fn output_stream<S, E>(&self, mut stream: S, path: &Path) -> Result<()>
    where
        S: Stream<Item = Result<RecordBatch, E>> + Unpin,
        E: Error,
    {
        // We get the schema from the first batch and use that for creating the writer
        if let Some(Ok(first_batch)) = stream.next().await {
            let schema = first_batch.schema();
            let mut writer = path_to_writer(path, schema)?;
            writer.write(&first_batch)?;

            while let Some(maybe_batch) = stream.next().await {
                match maybe_batch {
                    Ok(batch) => writer.write(&batch)?,
                    Err(e) => return Err(eyre!("Error executing SQL: {e}")),
                }
            }
            writer.close().await?;
        }

        Ok(())
    }
}

/// Wrapper for Vortex writer to handle Arrow RecordBatch conversion
#[cfg(feature = "vortex")]
struct VortexFileWriter {
    path: PathBuf,
    batches: Vec<RecordBatch>,
}

#[cfg(feature = "vortex")]
impl VortexFileWriter {
    fn new(file: File, _schema: SchemaRef, path: &Path) -> Result<Self> {
        // We need to drop the std::fs::File and use tokio::fs::File later
        drop(file);
        Ok(Self {
            path: path.to_path_buf(),
            batches: Vec::new(),
        })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        // Collect batches to write them all at once when closing
        self.batches.push(batch.clone());
        Ok(())
    }

    async fn close(self) -> Result<()> {
        if self.batches.is_empty() {
            return Ok(());
        }

        // Open file using tokio::fs::File which implements VortexWrite
        let file = tokio::fs::File::create(&self.path).await?;

        // Concatenate all batches into a single batch
        let schema = self.batches[0].schema();
        let concatenated = datafusion::arrow::compute::concat_batches(&schema, &self.batches)?;

        // Convert to Vortex array
        let vortex_array = ArrayRef::from_arrow(concatenated, false);

        // Convert to array stream
        let stream = vortex_array.to_array_stream();

        // Write using async API
        let session = VortexSession::empty();
        VortexWriteOptions::new(session)
            .write(file, stream)
            .await
            .map_err(|e| eyre!("Failed to write Vortex file: {}", e))?;

        Ok(())
    }
}

/// We use an Enum for this because of limitations with using trait objects and the `close` method
/// on a writer taking `self` as an argument which requires a size for the trait object which is
/// not known at compile time.
#[allow(clippy::large_enum_variant)]
enum AnyWriter {
    Csv(csv::writer::Writer<File>),
    Json(json::writer::LineDelimitedWriter<File>),
    Parquet(ArrowWriter<File>),
    Arrow(ArrowIpcWriter<File>),
    #[cfg(feature = "vortex")]
    Vortex(VortexFileWriter),
}

impl AnyWriter {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        match self {
            AnyWriter::Csv(w) => Ok(w.write(batch)?),
            AnyWriter::Json(w) => Ok(w.write(batch)?),
            AnyWriter::Parquet(w) => Ok(w.write(batch)?),
            AnyWriter::Arrow(w) => Ok(w.write(batch)?),
            #[cfg(feature = "vortex")]
            AnyWriter::Vortex(w) => Ok(w.write(batch)?),
        }
    }

    async fn close(mut self) -> Result<()> {
        match self {
            AnyWriter::Csv(w) => Ok(w.close()?),
            AnyWriter::Json(w) => Ok(w.close()?),
            AnyWriter::Parquet(w) => {
                w.close()?;
                Ok(())
            }
            AnyWriter::Arrow(ref mut w) => {
                w.finish()?;
                Ok(())
            }
            #[cfg(feature = "vortex")]
            AnyWriter::Vortex(w) => w.close().await,
        }
    }
}

fn path_to_writer(path: &Path, schema: SchemaRef) -> Result<AnyWriter> {
    if let Some(extension) = path.extension() {
        if let Some(e) = extension.to_ascii_lowercase().to_str() {
            let file = std::fs::File::create(path)?;
            return match e {
                "csv" => Ok(AnyWriter::Csv(csv::writer::Writer::new(file))),
                "json" => Ok(AnyWriter::Json(json::writer::LineDelimitedWriter::new(
                    file,
                ))),
                "parquet" => {
                    let props = WriterProperties::default();
                    let writer = ArrowWriter::try_new(file, schema, Some(props))?;
                    Ok(AnyWriter::Parquet(writer))
                }
                "arrow" | "ipc" => {
                    let writer = ArrowIpcWriter::try_new(file, &schema)?;
                    Ok(AnyWriter::Arrow(writer))
                }
                #[cfg(feature = "vortex")]
                "vortex" => Ok(AnyWriter::Vortex(VortexFileWriter::new(
                    file, schema, path,
                )?)),
                _ => {
                    #[cfg(feature = "vortex")]
                    return Err(eyre!(
                        "Only 'csv', 'parquet', 'json', 'arrow', and 'vortex' file types can be output"
                    ));
                    #[cfg(not(feature = "vortex"))]
                    return Err(eyre!(
                        "Only 'csv', 'parquet', 'json', and 'arrow' file types can be output"
                    ));
                }
            };
        }
    }
    Err(eyre!("Unable to parse extension"))
}

pub async fn try_run(cli: DftArgs, config: AppConfig) -> Result<()> {
    let merged_exec_config = merge_configs(config.shared.clone(), config.cli.execution.clone());
    let session_state_builder = DftSessionStateBuilder::try_new(Some(merged_exec_config.clone()))?
        .with_extensions()
        .await?;

    // CLI mode: executing commands from files or CLI arguments
    let session_state = session_state_builder.build()?;
    let execution_ctx = ExecutionContext::try_new(
        &merged_exec_config,
        session_state,
        crate::APP_NAME,
        env!("CARGO_PKG_VERSION"),
    )?;
    #[allow(unused_mut)]
    let mut app_execution = AppExecution::new(execution_ctx);
    #[cfg(feature = "flightsql")]
    {
        if cli.flightsql || matches!(cli.command, Some(Command::FlightSql { .. })) {
            let auth = AuthConfig {
                basic_auth: config.flightsql_client.auth.basic_auth,
                bearer_token: config.flightsql_client.auth.bearer_token,
            };
            let flightsql_cfg = FlightSQLConfig::new(
                config.flightsql_client.connection_url,
                config.flightsql_client.benchmark_iterations,
                auth,
                config.flightsql_client.headers,
            );
            let flightsql_ctx = FlightSQLContext::new(flightsql_cfg);
            let headers = cli.header.clone().map(|vec| vec.into_iter().collect());
            flightsql_ctx
                .create_client(cli.host.clone(), headers)
                .await?;
            app_execution.with_flightsql_ctx(flightsql_ctx);
        }
    }
    register_db(app_execution.session_ctx(), &config.db).await?;
    let app = CliApp::new(app_execution, cli.clone());
    app.execute_files_or_commands().await?;
    Ok(())
}

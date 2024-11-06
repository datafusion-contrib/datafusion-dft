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

use crate::args::DftArgs;
use crate::execution::{local_benchmarks::LocalBenchmarkStats, AppExecution};
use color_eyre::eyre::eyre;
use color_eyre::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::sql::parser::DFParser;
use futures::{Stream, StreamExt};
use log::info;
use std::error::Error;
use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(feature = "flightsql")]
use {crate::execution::flightsql_benchmarks::FlightSQLBenchmarkStats, tonic::IntoRequest};

const LOCAL_BENCHMARK_HEADER_ROW: &str =
    "query,runs,logical_planning_min,logical_planning_max,logical_planning_mean,logical_planning_median,logical_planning_percent_of_total,physical_planning_min,physical_planning_max,physical_planning,mean,physical_planning_median,physical_planning_percent_of_total,execution_min,execution_max,execution_execution_mean,execution_median,execution_percent_of_total,total_min,total_max,total_mean,total_median,total_percent_of_total";

#[cfg(feature = "flightsql")]
const FLIGHTSQL_BENCHMARK_HEADER_ROW: &str =
    "query,runs,get_flight_info_min,get_flight_info_max,get_flight_info_mean,get_flight_info_median,get_flight_info_percent_of_total,ttfb_min,ttfb_max,ttfb,mean,ttfb_median,ttfb_percent_of_total,do_get_min,do_get_max,do_get_mean,do_get_median,do_get_percent_of_total,total_min,total_max,total_mean,total_median,total_percent_of_total";

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

    /// Execute the provided sql, which was passed as an argument from CLI.
    ///
    /// Optionally, use the FlightSQL client for execution.
    pub async fn execute_files_or_commands(&self) -> color_eyre::Result<()> {
        if self.args.run_ddl {
            self.app_execution.execution_ctx().execute_ddl().await;
        }

        #[cfg(not(feature = "flightsql"))]
        match (
            self.args.files.is_empty(),
            self.args.commands.is_empty(),
            self.args.flightsql,
            self.args.bench,
            self.args.analyze,
        ) {
            // Error cases
            (_, _, true, _, _) => Err(eyre!(
                "FLightSQL feature isn't enabled. Reinstall `dft` with `--features=flightsql`"
            )),
            (false, false, false, true, _) => {
                Err(eyre!("Cannot benchmark without a command or file"))
            }
            (true, true, _, _, _) => Err(eyre!("No files or commands provided to execute")),
            (false, false, _, false, _) => Err(eyre!(
                "Cannot execute both files and commands at the same time"
            )),
            (_, _, false, true, true) => Err(eyre!(
                "The `benchmark` and `analyze` flags are mutually exclusive"
            )),

            // Execution cases
            (false, true, _, false, false) => self.execute_files(&self.args.files).await,
            (true, false, _, false, false) => self.execute_commands(&self.args.commands).await,

            // Benchmark cases
            (false, true, _, true, false) => self.benchmark_files(&self.args.files).await,
            (true, false, _, true, false) => self.benchmark_commands(&self.args.commands).await,

            // Analyze cases
            (false, true, _, false, true) => self.analyze_files(&self.args.files).await,
            (true, false, _, false, true) => self.analyze_commands(&self.args.commands).await,
        }
        #[cfg(feature = "flightsql")]
        match (
            self.args.files.is_empty(),
            self.args.commands.is_empty(),
            self.args.flightsql,
            self.args.bench,
        ) {
            (true, true, _, _) => Err(eyre!("No files or commands provided to execute")),
            (false, true, true, false) => self.flightsql_execute_files(&self.args.files).await,
            (false, true, true, true) => self.flightsql_benchmark_files(&self.args.files).await,
            (false, true, false, false) => self.execute_files(&self.args.files).await,
            (false, true, false, true) => self.benchmark_files(&self.args.files).await,

            (true, false, true, false) => {
                self.flightsql_execute_commands(&self.args.commands).await
            }
            (true, false, true, true) => {
                self.flightsql_benchmark_commands(&self.args.commands).await
            }
            (true, false, false, false) => self.execute_commands(&self.args.commands).await,
            (true, false, false, true) => self.benchmark_commands(&self.args.commands).await,
            (false, false, false, true) => Err(eyre!("Cannot benchmark without a command or file")),
            (false, false, _, _) => Err(eyre!(
                "Cannot execute both files and commands at the same time"
            )),
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
                    if let Some(start) = start {
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
            if let Some(start) = start {
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
        let stats = self
            .app_execution
            .execution_ctx()
            .benchmark_query(sql)
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
        let stats = self
            .app_execution
            .flightsql_ctx()
            .benchmark_query(sql)
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
}

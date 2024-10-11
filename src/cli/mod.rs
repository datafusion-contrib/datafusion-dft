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
use crate::execution::AppExecution;
use color_eyre::eyre::eyre;
use color_eyre::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::sql::parser::DFParser;
use futures::{Stream, StreamExt};
use log::info;
use std::error::Error;
use std::path::{Path, PathBuf};
#[cfg(feature = "flightsql")]
use tonic::IntoRequest;

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
        #[cfg(not(feature = "flightsql"))]
        match (
            self.args.files.is_empty(),
            self.args.commands.is_empty(),
            self.args.flightsql,
        ) {
            (_, _, true) => Err(eyre!(
                "FLightSQL feature isn't enabled. Reinstall `dft` with `--features=flightsql`"
            )),
            (true, true, _) => Err(eyre!("No files or commands provided to execute")),
            (false, true, _) => {
                self.execute_files(&self.args.files, self.args.run_ddl)
                    .await
            }
            (true, false, _) => {
                self.execute_commands(&self.args.commands, self.args.run_ddl)
                    .await
            }
            (false, false, _) => Err(eyre!(
                "Cannot execute both files and commands at the same time"
            )),
        }
        #[cfg(feature = "flightsql")]
        match (
            self.args.files.is_empty(),
            self.args.commands.is_empty(),
            self.args.flightsql,
        ) {
            (true, true, _) => Err(eyre!("No files or commands provided to execute")),
            (false, true, true) => self.flightsql_execute_files(&self.args.files).await,
            (false, true, false) => {
                self.execute_files(&self.args.files, self.args.run_ddl)
                    .await
            }
            (true, false, true) => self.flightsql_execute_commands(&self.args.commands).await,
            (true, false, false) => {
                self.execute_commands(&self.args.commands, self.args.run_ddl)
                    .await
            }
            (false, false, _) => Err(eyre!(
                "Cannot execute both files and commands at the same time"
            )),
        }
    }

    async fn execute_files(&self, files: &[PathBuf], run_ddl: bool) -> color_eyre::Result<()> {
        info!("Executing files: {:?}", files);
        if run_ddl {
            let ddl = self.app_execution.execution_ctx().load_ddl();
            if let Some(ddl) = ddl {
                info!("Executing DDL");
                self.exec_from_string(&ddl).await?;
            } else {
                info!("No DDL to execute");
            }
        }
        for file in files {
            self.exec_from_file(file).await?
        }

        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_execute_files(&self, files: &[PathBuf]) -> color_eyre::Result<()> {
        info!("Executing files: {:?}", files);
        for (i, file) in files.iter().enumerate() {
            let file = std::fs::read_to_string(file)?;
            self.exec_from_flightsql(file, i).await?;
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

    async fn execute_commands(&self, commands: &[String], run_ddl: bool) -> color_eyre::Result<()> {
        info!("Executing commands: {:?}", commands);
        if run_ddl {
            let ddl = self.app_execution.execution_ctx().load_ddl();
            if let Some(ddl) = ddl {
                info!("Executing DDL");
                self.exec_from_string(&ddl).await?;
            } else {
                info!("No DDL to execute");
            }
        }
        for command in commands {
            self.exec_from_string(command).await?
        }

        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_execute_commands(&self, commands: &[String]) -> color_eyre::Result<()> {
        info!("Executing commands: {:?}", commands);
        for (i, command) in commands.iter().enumerate() {
            self.exec_from_flightsql(command.to_string(), i).await?
        }

        Ok(())
    }

    async fn exec_from_string(&self, sql: &str) -> color_eyre::Result<()> {
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

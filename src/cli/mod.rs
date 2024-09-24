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

use crate::execution::ExecutionContext;
use color_eyre::eyre::eyre;
use color_eyre::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::sql::parser::DFParser;
use futures::{Stream, StreamExt};
use log::info;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
#[cfg(feature = "flightsql")]
use tonic::IntoRequest;

/// Encapsulates the command line interface
pub struct CliApp {
    /// Execution context for running queries
    execution: ExecutionContext,
}

impl CliApp {
    pub fn new(execution: ExecutionContext) -> Self {
        Self { execution }
    }

    /// Execute the provide sql, which was passed as an argument from CLI.
    ///
    /// Optionally, use the FlightSQL client for execution.
    pub async fn execute_files_or_commands(
        &self,
        files: Vec<PathBuf>,
        commands: Vec<String>,
        flightsql: bool,
    ) -> color_eyre::Result<()> {
        #[cfg(not(feature = "flightsql"))]
        match (files.is_empty(), commands.is_empty(), flightsql) {
            (_, _, true) => Err(eyre!(
                "FLightSQL feature isn't enabled. Reinstall with `--features=flightsql`"
            )),
            (true, true, _) => Err(eyre!("No files or commands provided to execute")),
            (false, true, _) => self.execute_files(files).await,
            (true, false, _) => self.execute_commands(commands).await,
            (false, false, _) => Err(eyre!(
                "Cannot execute both files and commands at the same time"
            )),
        }
        #[cfg(feature = "flightsql")]
        match (files.is_empty(), commands.is_empty(), flightsql) {
            (true, true, _) => Err(eyre!("No files or commands provided to execute")),
            (false, true, true) => self.flightsql_execute_files(files).await,
            (false, true, false) => self.execute_files(files).await,
            (true, false, true) => self.flightsql_execute_commands(commands).await,
            (true, false, false) => self.execute_commands(commands).await,
            (false, false, _) => Err(eyre!(
                "Cannot execute both files and commands at the same time"
            )),
        }
    }

    async fn execute_files(&self, files: Vec<PathBuf>) -> color_eyre::Result<()> {
        info!("Executing files: {:?}", files);
        for file in files {
            self.exec_from_file(&file).await?
        }

        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_execute_files(&self, files: Vec<PathBuf>) -> color_eyre::Result<()> {
        info!("Executing files: {:?}", files);
        for file in files {
            let file = std::fs::read_to_string(file)?;
            self.exec_from_flightsql(file).await?;
        }

        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn exec_from_flightsql(&self, sql: String) -> color_eyre::Result<()> {
        let client = self.execution.flightsql_client();
        let mut guard = client.lock().await;
        if let Some(client) = guard.as_mut() {
            let flight_info = client.execute(sql, None).await?;
            for endpoint in flight_info.endpoint {
                if let Some(ticket) = endpoint.ticket {
                    let stream = client.do_get(ticket.into_request()).await?;
                    self.print_any_stream(stream).await;
                }
            }
        } else {
            println!("No FlightSQL client configured.  Add one in `~/.config/dft/config.toml`");
        }

        Ok(())
    }

    async fn execute_commands(&self, commands: Vec<String>) -> color_eyre::Result<()> {
        info!("Executing commands: {:?}", commands);
        for command in commands {
            self.exec_from_string(&command).await?
        }

        Ok(())
    }

    #[cfg(feature = "flightsql")]
    async fn flightsql_execute_commands(&self, commands: Vec<String>) -> color_eyre::Result<()> {
        info!("Executing commands: {:?}", commands);
        for command in commands {
            self.exec_from_flightsql(command).await?
        }

        Ok(())
    }

    async fn exec_from_string(&self, sql: &str) -> color_eyre::Result<()> {
        let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
        let statements = DFParser::parse_sql_with_dialect(sql, &dialect)?;
        for statement in statements {
            let stream = self.execution.execute_statement(statement).await?;
            self.print_any_stream(stream).await;
        }
        Ok(())
    }

    /// run and execute SQL statements and commands from a file, against a context
    /// with the given print options
    pub async fn exec_from_file(&self, file: &Path) -> color_eyre::Result<()> {
        let file = File::open(file)?;
        let reader = BufReader::new(file);

        let mut query = String::new();

        for line in reader.lines() {
            let line = line?;
            if line.starts_with("#!") {
                continue;
            }
            if line.starts_with("--") {
                continue;
            }

            let line = line.trim_end();
            query.push_str(line);
            // if we found the end of a query, run it
            if line.ends_with(';') {
                // TODO: if the query errors, should we keep trying to execute
                // the other queries in the file? That is what datafusion-cli does...
                self.execute_and_print_sql(&query).await?;
                query.clear();
            } else {
                query.push('\n');
            }
        }

        // run the last line(s) in file if the last statement doesn't contain ‘;’
        // ignore if it only consists of '\n'
        if query.contains(|c| c != '\n') {
            self.execute_and_print_sql(&query).await?;
        }

        Ok(())
    }

    /// executes a sql statement and prints the result to stdout
    pub async fn execute_and_print_sql(&self, sql: &str) -> color_eyre::Result<()> {
        let stream = self.execution.execute_sql(sql).await?;
        self.print_any_stream(stream).await;
        Ok(())
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

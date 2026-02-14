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

//! Command line argument parsing: [`DftArgs`]

use crate::config::get_data_dir;
use clap::{Parser, Subcommand};
use http::{HeaderName, HeaderValue};
#[cfg(any(feature = "http", feature = "flightsql"))]
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

const LONG_ABOUT: &str = "
dft - DataFusion TUI

CLI and terminal UI data analysis tool using Apache DataFusion as query
execution engine.

dft provides a rich terminal UI as well as a broad array of pre-integrated
data sources and formats for querying and analyzing data.

Environment Variables
RUST_LOG { trace | debug | info | error }: Standard rust logging level.  Default is info.
";

#[derive(Clone, Debug, Parser, Default)]
#[command(author, version, about, long_about = LONG_ABOUT)]
pub struct DftArgs {
    #[clap(
        short,
        long,
        num_args = 0..,
        help = "Execute commands from file(s), then exit",
        value_parser(parse_valid_file)
    )]
    pub files: Vec<PathBuf>,

    #[clap(
        short = 'c',
        long,
        num_args = 0..,
        help = "Execute the given SQL string(s), then exit.",
        value_parser(parse_command)
    )]
    pub commands: Vec<String>,

    #[clap(long, global = true, help = "Path to the configuration file")]
    pub config: Option<String>,

    #[clap(
        long,
        short = 'q',
        help = "Use the FlightSQL client defined in your config"
    )]
    pub flightsql: bool,

    #[clap(long, help = "Run DDL prior to executing")]
    pub run_ddl: bool,

    #[clap(long, short, help = "Only show how long the query took to run")]
    pub time: bool,

    #[clap(long, short, help = "Benchmark the provided query")]
    pub bench: bool,

    #[clap(
        long,
        help = "Print a summary of the query's execution plan and statistics. Works with both local and FlightSQL execution (requires server support)."
    )]
    pub analyze: bool,

    #[clap(
        long,
        help = "Print raw execution metrics as Arrow table without formatting. Useful for debugging and custom analysis. Implies --analyze."
    )]
    pub analyze_raw: bool,

    #[clap(long, help = "Run the provided query before running the benchmark")]
    pub run_before: Option<String>,

    #[clap(long, help = "Save the benchmark results to a file")]
    pub save: Option<PathBuf>,

    #[clap(long, help = "Append the benchmark results to an existing file")]
    pub append: bool,

    #[clap(short = 'n', help = "Set the number of benchmark iterations to run")]
    pub benchmark_iterations: Option<usize>,

    #[clap(long, help = "Run benchmark iterations concurrently/in parallel")]
    pub concurrent: bool,

    #[clap(long, help = "Host address to query. Only used for FlightSQL")]
    pub host: Option<String>,

    #[clap(
        long,
        help = "Header to add to Flight SQL connection. Only used for FlightSQL",
        value_parser(parse_header_line),
        action = clap::ArgAction::Append
    )]
    pub header: Option<Vec<(String, String)>>,

    #[clap(
        long,
        short,
        help = "Path to save output to. Type is inferred from file suffix"
    )]
    pub output: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

impl DftArgs {
    pub fn config_path(&self) -> PathBuf {
        #[cfg(feature = "flightsql")]
        if let Some(Command::ServeFlightSql {
            config: Some(cfg), ..
        }) = &self.command
        {
            return Path::new(cfg).to_path_buf();
        }
        if let Some(config) = self.config.as_ref() {
            Path::new(config).to_path_buf()
        } else {
            let mut config = get_data_dir();
            config.push("config.toml");
            config
        }
    }
}

/// Parameters for each command match to exactly how they are defined in specification (https://arrow.apache.org/docs/format/FlightSql.html#protocol-buffer-definitions)
#[derive(Clone, Debug, Subcommand)]
pub enum FlightSqlCommand {
    /// Executes `CommandStatementQuery` and `DoGet` to return results
    StatementQuery {
        /// The query to execute
        #[clap(long)]
        sql: String,
    },
    /// Executes `CommandGetCatalogs` and `DoGet` to return results
    GetCatalogs,
    /// Executes `CommandGetDbSchemas` and `DoGet` to return results
    GetDbSchemas {
        /// The catalog to retrieve schemas
        #[clap(long)]
        catalog: Option<String>,
        /// Schema filter pattern to apply
        #[clap(long)]
        db_schema_filter_pattern: Option<String>,
    },
    /// Executes `CommandGetDbSchemas` and `DoGet` to return results
    GetTables {
        /// The catalog to retrieve schemas
        #[clap(long)]
        catalog: Option<String>,
        /// Schema filter pattern to apply
        #[clap(long)]
        db_schema_filter_pattern: Option<String>,
        /// Table name filter pattern to apply
        #[clap(long)]
        table_name_filter_pattern: Option<String>,
        /// Specific table types to return
        #[clap(long)]
        table_types: Option<Vec<String>>,
    },
    /// Executes `CommandGetTableTypes` and `DoGet` to return supported table types
    GetTableTypes,
    /// Executes `CommandGetSqlInfo` and `DoGet` to return server SQL capabilities
    GetSqlInfo {
        /// Specific SQL info IDs to retrieve (if not provided, returns all)
        #[clap(long)]
        info: Option<Vec<u32>>,
    },
    /// Executes `CommandGetXdbcTypeInfo` and `DoGet` to return type information
    GetXdbcTypeInfo {
        /// Optional data type to filter by
        #[clap(long)]
        data_type: Option<i32>,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum Command {
    /// Start a HTTP server
    #[cfg(feature = "http")]
    ServeHttp {
        #[clap(short, long)]
        config: Option<String>,
        #[clap(long, help = "Set the port to be used for server")]
        addr: Option<SocketAddr>,
        #[clap(long, help = "Set the port to be used for serving metrics")]
        metrics_addr: Option<SocketAddr>,
    },
    /// Make a request to a FlightSQL server
    #[cfg(feature = "flightsql")]
    #[command(name = "flightsql")]
    FlightSql {
        #[clap(subcommand)]
        command: FlightSqlCommand,
    },
    /// Start a FlightSQL server
    #[cfg(feature = "flightsql")]
    #[command(name = "serve-flightsql")]
    ServeFlightSql {
        #[clap(short, long)]
        config: Option<String>,
        #[clap(long, help = "Set the port to be used for server")]
        addr: Option<SocketAddr>,
        #[clap(long, help = "Set the port to be used for serving metrics")]
        metrics_addr: Option<SocketAddr>,
    },
    GenerateTpch {
        #[clap(long, default_value = "1.0")]
        scale_factor: f64,
        #[clap(long, default_value = "parquet")]
        format: TpchFormat,
    },
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum TpchFormat {
    Parquet,
    #[cfg(feature = "vortex")]
    Vortex,
}

fn parse_valid_file(file: &str) -> std::result::Result<PathBuf, String> {
    let path = PathBuf::from(file);
    if !path.exists() {
        Err(format!("File does not exist: '{file}'"))
    } else if !path.is_file() {
        Err(format!("Exists but is not a file: '{file}'"))
    } else {
        Ok(path)
    }
}

fn parse_command(command: &str) -> std::result::Result<String, String> {
    if !command.is_empty() {
        Ok(command.to_string())
    } else {
        Err("-c flag expects only non empty commands".to_string())
    }
}

fn parse_header_line(line: &str) -> Result<(String, String), String> {
    let (name, value) = line
        .split_once(':')
        .ok_or_else(|| format!("Invalid header format: '{}'", line))?;

    let name =
        HeaderName::try_from(name.trim()).map_err(|e| format!("Invalid header name: {}", e))?;
    let value =
        HeaderValue::try_from(value.trim()).map_err(|e| format!("Invalid header value: {}", e))?;

    let value_str = value
        .to_str()
        .map_err(|e| format!("Header value contains invalid characters: {}", e))?;

    Ok((name.to_string(), value_str.to_string()))
}

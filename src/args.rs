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
use clap::Parser;
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

    #[clap(long, help = "Path to the configuration file")]
    pub config: Option<String>,

    #[clap(long, help = "Use the FlightSQL client defined in your config")]
    pub flightsql: bool,

    #[clap(long, help = "Run DDL prior to executing")]
    pub run_ddl: bool,

    #[clap(long, short, help = "Only show how long the query took to run")]
    pub time: bool,

    #[clap(long, help = "Start a FlightSQL server")]
    pub serve: bool,
}

impl DftArgs {
    pub fn config_path(&self) -> PathBuf {
        if let Some(config) = self.config.as_ref() {
            Path::new(config).to_path_buf()
        } else {
            let mut config = get_data_dir();
            config.push("config.toml");
            config
        }
    }
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

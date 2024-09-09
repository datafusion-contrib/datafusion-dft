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

use clap::Parser;
use color_eyre::Result;
use dft::app::{execute_files_or_commands, run_app, state};
use dft::cli;
use dft::telemetry;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = cli::DftCli::parse();

    // If executing commands from files, do so and then exit
    if !cli.files.is_empty() || !cli.commands.is_empty() {
        // use env_logger to setup logging for CLI
        env_logger::init();
        let state = state::initialize(cli.clone());
        execute_files_or_commands(cli.files.clone(), cli.commands.clone(), &state).await?;
    } else {
        // use alternate logging for TUI
        telemetry::initialize_logs()?;
        let state = state::initialize(cli.clone());
        run_app(cli.clone(), state).await?;
    }

    Ok(())
}

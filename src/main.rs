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

mod app;
mod cli;
mod telemetry;
mod ui;

use crate::app::state;
use app::{execute_files, run_app};
use clap::Parser;
use color_eyre::Result;

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::initialize_logs()?;
    let cli = cli::DftCli::parse();
    let state = state::initialize(&cli);

    // If executing commands from files, do so and then exit
    if !cli.file.is_empty() {
        execute_files(cli.file.clone(), &state).await?;
    } else {
        run_app(cli.clone(), state).await?;
    }

    Ok(())
}

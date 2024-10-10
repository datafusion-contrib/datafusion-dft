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
use dft::args::DftArgs;
use dft::cli::CliApp;
use dft::execution::ExecutionContext;
use dft::flightsql_server::{FlightSqlApp, FlightSqlServiceImpl};
use dft::telemetry;
use dft::tui::{state, App};

const DEFAULT_SERVER_ADDRESS: &str = "127.0.0.1:50051";

#[tokio::main]
async fn main() -> Result<()> {
    let cli = DftArgs::parse();

    // CLI mode: executing commands from files or CLI arguments
    if !cli.files.is_empty() || !cli.commands.is_empty() {
        // use env_logger to setup logging for CLI
        env_logger::init();
        let state = state::initialize(cli.config_path());
        let execution = ExecutionContext::try_new(&state.config.execution)?;
        #[cfg(feature = "flightsql")]
        {
            if cli.flightsql {
                execution
                    .create_flightsql_client(state.config.flightsql)
                    .await?;
            }
        }
        let app = CliApp::new(execution, cli);
        app.execute_files_or_commands().await?;
    } else if cli.serve {
        #[cfg(feature = "flightsql")]
        {
            env_logger::init();
            println!("Starting FlightSQL server on {}", DEFAULT_SERVER_ADDRESS);
            let server = FlightSqlServiceImpl::new();
            let app = FlightSqlApp::new(server.service(), DEFAULT_SERVER_ADDRESS).await;
            app.run().await;
        }

        #[cfg(not(feature = "flightsql"))]
        {
            panic!("FlightSQL feature is not enabled");
        }
    }
    // UI mode: running the TUI
    else {
        // use alternate logging for TUI
        telemetry::initialize_logs()?;
        let state = state::initialize(cli.config_path());
        let execution = ExecutionContext::try_new(&state.config.execution)?;
        let app = App::new(state, execution);
        app.run_app().await?;
    }

    Ok(())
}

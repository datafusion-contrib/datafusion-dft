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
use datafusion_dft::args::DftArgs;
use datafusion_dft::cli::CliApp;
#[cfg(feature = "flightsql")]
use datafusion_dft::execution::flightsql::FlightSQLContext;
use datafusion_dft::execution::{local::ExecutionContext, AppExecution, AppType};
use datafusion_dft::extensions::DftSessionStateBuilder;
#[cfg(feature = "experimental-flightsql-server")]
use datafusion_dft::server::FlightSqlApp;
use datafusion_dft::telemetry;
use datafusion_dft::tui::{state, App};
#[cfg(feature = "experimental-flightsql-server")]
use log::info;

#[allow(unused_mut)]
fn main() -> Result<()> {
    let cli = DftArgs::parse();

    // With Runtimes configured correctly the main Tokio runtime should only be used for network
    // IO, in which a single thread should be sufficient.
    //
    // Ref: https://github.com/datafusion-contrib/datafusion-dft/pull/247#discussion_r1848270250
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;

    let entry_point = app_entry_point(cli);
    runtime.block_on(entry_point)
}

fn should_init_env_logger(cli: &DftArgs) -> bool {
    #[cfg(feature = "experimental-flightsql-server")]
    if cli.serve {
        return true;
    }
    if !cli.files.is_empty() || !cli.commands.is_empty() {
        return true;
    }
    false
}

async fn app_entry_point(cli: DftArgs) -> Result<()> {
    if should_init_env_logger(&cli) {
        env_logger::init();
    }
    let state = state::initialize(cli.config_path());
    let session_state_builder = DftSessionStateBuilder::new()
        .with_execution_config(state.config.execution.clone())
        .with_extensions()
        .await?;
    #[cfg(feature = "experimental-flightsql-server")]
    if cli.serve {
        // FlightSQL Server mode: start a FlightSQL server
        const DEFAULT_SERVER_ADDRESS: &str = "127.0.0.1:50051";
        info!("Starting FlightSQL server on {}", DEFAULT_SERVER_ADDRESS);
        let session_state = session_state_builder
            .with_app_type(AppType::FlightSQLServer)
            .build()?;
        let execution_ctx = ExecutionContext::try_new(
            &state.config.execution,
            session_state,
            AppType::FlightSQLServer,
        )?;
        if cli.run_ddl {
            execution_ctx.execute_ddl().await;
        }
        let app_execution = AppExecution::new(execution_ctx);
        let app = FlightSqlApp::try_new(
            app_execution,
            &cli.flightsql_host
                .unwrap_or(DEFAULT_SERVER_ADDRESS.to_string()),
            &state.config.flightsql.server_metrics_port,
        )
        .await?;
        app.run_app().await;
        return Ok(());
    }
    if !cli.files.is_empty() || !cli.commands.is_empty() {
        // CLI mode: executing commands from files or CLI arguments
        let session_state = session_state_builder.with_app_type(AppType::Cli).build()?;
        let execution_ctx =
            ExecutionContext::try_new(&state.config.execution, session_state, AppType::Cli)?;
        #[allow(unused_mut)]
        let mut app_execution = AppExecution::new(execution_ctx);
        #[cfg(feature = "flightsql")]
        {
            if cli.flightsql {
                let flightsql_ctx = FlightSQLContext::new(state.config.flightsql.clone());
                flightsql_ctx
                    .create_client(cli.flightsql_host.clone())
                    .await?;
                app_execution.with_flightsql_ctx(flightsql_ctx);
            }
        }
        let app = CliApp::new(app_execution, cli.clone());
        app.execute_files_or_commands().await?;
    } else {
        // TUI mode: running the TUI
        telemetry::initialize_logs()?; // use alternate logging for TUI
        let state = state::initialize(cli.config_path());
        let session_state = session_state_builder.with_app_type(AppType::Tui).build()?;
        let execution_ctx =
            ExecutionContext::try_new(&state.config.execution, session_state, AppType::Tui)?;
        let app_execution = AppExecution::new(execution_ctx);
        let app = App::new(state, cli, app_execution);
        app.run_app().await?;
    }

    Ok(())
}

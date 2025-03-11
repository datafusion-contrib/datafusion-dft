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

mod router;

use std::net::SocketAddr;

use crate::{args::DftArgs, config::AppConfig, execution::AppExecution};
use axum::Router;
use color_eyre::Result;
use datafusion_app::{
    config::merge_configs, extensions::DftSessionStateBuilder, local::ExecutionContext,
};
use router::create_router;
use tokio::{net::TcpListener, signal};
use tracing::info;

use super::try_start_metrics_server;

const DEFAULT_SERVER_ADDRESS: &str = "127.0.0.1:8080";

/// From https://github.com/tokio-rs/axum/blob/main/examples/graceful-shutdown/src/main.rs
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// Creates and manages a running FlightSqlServer with a background task
pub struct HttpApp {
    /// Address the server is listening on
    listener: TcpListener,

    /// handle for the server task
    router: Router,
}

impl HttpApp {
    /// create a new app for the flightsql server
    pub async fn try_new(execution: AppExecution, addr: &str, metrics_addr: &str) -> Result<Self> {
        info!("Listening to HTTP on {addr}");
        let listener = TcpListener::bind(addr).await.unwrap();

        // prepare the shutdown channel
        let state = execution.execution_ctx().clone();

        let router = create_router(state);

        let metrics_addr: SocketAddr = metrics_addr.parse()?;
        try_start_metrics_server(metrics_addr)?;

        let app = Self { listener, router };
        Ok(app)
    }

    pub async fn run(self) {
        match axum::serve(self.listener, self.router)
            .with_graceful_shutdown(shutdown_signal())
            .await
        {
            Ok(_) => {
                info!("Shutting down app")
            }
            Err(_) => {
                panic!("Error serving HTTP app")
            }
        }
    }
}

pub async fn try_run(cli: DftArgs, config: AppConfig) -> Result<()> {
    let merged_exec_config =
        merge_configs(config.shared.clone(), config.http_server.execution.clone());
    let session_state_builder = DftSessionStateBuilder::try_new(Some(merged_exec_config.clone()))?
        .with_extensions()
        .await?;
    let session_state = session_state_builder.build()?;
    let execution_ctx = ExecutionContext::try_new(&merged_exec_config, session_state)?;
    if cli.run_ddl {
        execution_ctx.execute_ddl().await;
    }
    let app_execution = AppExecution::new(execution_ctx);
    let app = HttpApp::try_new(
        app_execution,
        &cli.host.unwrap_or(DEFAULT_SERVER_ADDRESS.to_string()),
        &config.http_server.server_metrics_port,
    )
    .await?;
    app.run().await;

    Ok(())
}

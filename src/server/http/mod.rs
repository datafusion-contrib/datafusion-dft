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

use std::{net::SocketAddr, time::Duration};

use crate::{args::DftArgs, config::AppConfig, execution::AppExecution};
use axum::{extract::State, routing::get, Router};
use color_eyre::{eyre::eyre, Result};
use datafusion::arrow::json::{writer::LineDelimited, Writer};
use datafusion_app::{
    config::merge_configs, extensions::DftSessionStateBuilder, local::ExecutionContext,
};
use log::info;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tokio_stream::StreamExt;
use tower_http::validate_request::ValidateRequestHeaderLayer;

use super::try_start_metrics_server;

const DEFAULT_TIMEOUT_SECONDS: u64 = 60;
const DEFAULT_SERVER_ADDRESS: &str = "127.0.0.1:8080";

pub fn create_router(
    config: &AppConfig,
    // flightsql: FlightSqlServiceImpl,
    listener: TcpListener,
    rx: oneshot::Receiver<()>,
    // shutdown_future: impl Future<Output = ()> + Send,
) -> Router {
    let server_timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECONDS);
    // let mut server_builder = Server::builder().timeout(server_timeout);
    let shutdown_future = async move {
        rx.await.ok();
    };

    Router::new().route("/", get(|| async { "Hello, World!" }))

    // axum::serve(listener, router).await.unwrap();

    // TODO: onlu include TrailersLayer for testing
    // if cfg!(feature = "flightsql") {
    //     match (
    //         &config.flightsql_server.auth.basic_auth,
    //         &config.flightsql_server.auth.bearer_token,
    //     ) {
    //         (Some(_), Some(_)) => Err(eyre!("Only one auth type can be used at a time")),
    //         (Some(basic), None) => {
    //             let basic_auth_layer =
    //                 ValidateRequestHeaderLayer::basic(&basic.username, &basic.password);
    //             let f = server_builder
    //                 .layer(basic_auth_layer)
    //                 .add_service(flightsql.service())
    //                 .serve_with_incoming_shutdown(
    //                     tokio_stream::wrappers::TcpListenerStream::new(listener),
    //                     shutdown_future,
    //                 );
    //             Ok(tokio::task::spawn(f))
    //         }
    //         (None, Some(token)) => {
    //             let bearer_auth_layer = ValidateRequestHeaderLayer::bearer(token);
    //             let f = server_builder
    //                 .layer(bearer_auth_layer)
    //                 .add_service(flightsql.service())
    //                 .serve_with_incoming_shutdown(
    //                     tokio_stream::wrappers::TcpListenerStream::new(listener),
    //                     shutdown_future,
    //                 );
    //             Ok(tokio::task::spawn(f))
    //         }
    //         (None, None) => {
    //             let f = server_builder
    //                 .add_service(flightsql.service())
    //                 .serve_with_incoming_shutdown(
    //                     tokio_stream::wrappers::TcpListenerStream::new(listener),
    //                     shutdown_future,
    //                 );
    //             Ok(tokio::task::spawn(f))
    //         }
    //     }
    // } else {
    //     let f = server_builder
    //         .add_service(flightsql.service())
    //         .serve_with_incoming_shutdown(
    //             tokio_stream::wrappers::TcpListenerStream::new(listener),
    //             shutdown_future,
    //         );
    //     Ok(tokio::task::spawn(f))
    // }
}

/// Creates and manages a running FlightSqlServer with a background task
pub struct HttpApp {
    execution: AppExecution,
    /// channel to send shutdown command
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,

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
        let (tx, rx) = tokio::sync::oneshot::channel();
        let state = execution.execution_ctx().clone();

        let router = Router::new()
            .route(
                "/",
                get(|State(state): State<ExecutionContext>| async { "Hello, World!" }),
            )
            .route(
                "/query",
                get(|State(state): State<ExecutionContext>| async move {
                    let r = state.execute_sql("SELECT 1").await;
                    match r {
                        Ok(mut ba) => {
                            let mut buf = Vec::new();
                            let mut writer: Writer<&mut [u8], LineDelimited> =
                                datafusion::arrow::json::LineDelimitedWriter::new(&mut buf);
                            while let Some(b) = ba.next().await {
                                writer.write(&b.unwrap()).unwrap();
                            }
                            writer.finish().unwrap();
                            let r = String::from_utf8(buf).unwrap();
                            r
                        }
                        Err(e) => "Meep".to_string(),
                    }
                }),
            )
            .with_state(state);

        let metrics_addr: SocketAddr = metrics_addr.parse()?;
        try_start_metrics_server(metrics_addr)?;

        let app = Self {
            execution,
            shutdown: Some(tx),
            listener,
            router,
        };
        Ok(app)
    }

    pub async fn run(self) {
        match axum::serve(self.listener, self.router).await {
            Ok(_) => {}
            Err(_) => {
                panic!("Error serving HTTP app")
            }
        }
    }

    async fn root_handler() -> String {
        "Hi there".to_string()
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

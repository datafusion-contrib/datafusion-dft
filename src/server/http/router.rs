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

use std::{io::Cursor, time::Duration};

use axum::{
    body::Body,
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use datafusion::{arrow::json::ArrayWriter, execution::SendableRecordBatchStream};
use datafusion_app::local::{ExecutionContext, ExecutionOptions, ExecutionResult};
use http::{HeaderValue, StatusCode};
use log::error;
use serde::Deserialize;
use tokio_stream::StreamExt;
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer};
use tracing::info;

use crate::config::HttpServerConfig;

#[derive(Clone)]
struct ExecutionState {
    execution: ExecutionContext,
    config: HttpServerConfig,
}

impl ExecutionState {
    pub fn new(execution: ExecutionContext, config: HttpServerConfig) -> Self {
        Self { execution, config }
    }
}

pub fn create_router(execution: ExecutionContext, config: HttpServerConfig) -> Router {
    let state = ExecutionState::new(execution, config);
    Router::new()
        .route(
            "/",
            get(|State(_): State<ExecutionState>| async { "Hello, from DFT!" }),
        )
        .route(
            "/health-check",
            get(|State(_): State<ExecutionState>| async { "Healthy" }),
        )
        .route("/sql", post(post_sql_handler))
        .route("/catalog", get(get_catalog_handler))
        .route("/table/:catalog/:schema/:table", get(get_table_handler))
        .layer((
            TraceLayer::new_for_http(),
            // Graceful shutdown will wait for outstanding requests to complete. Add a timeout so
            // requests don't hang forever.
            TimeoutLayer::new(Duration::from_secs(state.config.timeout_seconds)),
        ))
        .with_state(state)
}

async fn post_sql_handler(state: State<ExecutionState>, query: String) -> Response {
    let opts = ExecutionOptions::new(Some(state.config.result_limit));
    execute_sql_with_opts(state, query, opts).await
}

async fn get_catalog_handler(state: State<ExecutionState>) -> Response {
    let opts = ExecutionOptions::new(None);
    execute_sql_with_opts(state, "SHOW TABLES".to_string(), opts).await
}

#[derive(Deserialize)]
struct GetTableParams {
    catalog: String,
    schema: String,
    table: String,
}

async fn get_table_handler(
    state: State<ExecutionState>,
    Path(params): Path<GetTableParams>,
) -> Response {
    let GetTableParams {
        catalog,
        schema,
        table,
    } = params;
    let sql = format!("SELECT * FROM \"{catalog}\".\"{schema}\".\"{table}\"");
    let opts = ExecutionOptions::new(Some(state.config.result_limit));
    execute_sql_with_opts(state, sql, opts).await
}

async fn execute_sql_with_opts(
    State(state): State<ExecutionState>,
    sql: String,
    opts: ExecutionOptions,
) -> Response {
    info!("Executing sql: {sql}");
    let results = state.execution.execute_sql_with_opts(&sql, opts).await;
    match results {
        Ok(ExecutionResult::RecordBatchStream(Ok(batch_stream))) => {
            batch_stream_to_response(batch_stream).await
        }
        Err(e) | Ok(ExecutionResult::RecordBatchStream(Err(e))) => {
            error!("Error executing SQL: {}", e);
            (
                StatusCode::BAD_REQUEST,
                format!("SQL execution failed: {}", e),
            )
                .into_response()
        }
    }
}

async fn batch_stream_to_response(batch_stream: SendableRecordBatchStream) -> Response {
    let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
    let mut writer = ArrayWriter::new(&mut buf);
    let mut batch_stream = batch_stream;
    while let Some(maybe_batch) = batch_stream.next().await {
        match maybe_batch {
            Ok(batch) => {
                if let Err(e) = writer.write(&batch) {
                    error!("Error serializing result batches: {}", e);
                    return (StatusCode::INTERNAL_SERVER_ERROR, "Serialization error")
                        .into_response();
                }
            }
            Err(e) => {
                error!("Error executing query: {}", e);
                return (StatusCode::INTERNAL_SERVER_ERROR, "Query execution error")
                    .into_response();
            }
        }
    }

    if let Err(e) = writer.finish() {
        error!("Error finalizing JSON writer: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, "Finalization error").into_response();
    }

    match String::from_utf8(buf.into_inner()) {
        Ok(json) => {
            let mut res = Response::new(Body::new(json));
            res.headers_mut()
                .insert("content-type", HeaderValue::from_static("application/json"));
            res
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "UTF-8 conversion error").into_response(),
    }
}

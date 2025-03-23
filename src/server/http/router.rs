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
    extract::{Json, Path, Query, State},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use datafusion::{arrow::json::ArrayWriter, execution::SendableRecordBatchStream};
use datafusion_app::{observability::ObservabilityRequestDetails, ExecOptions, ExecResult};
use http::{HeaderValue, StatusCode};
use jiff::Timestamp;
use log::error;
use serde::Deserialize;
use tokio_stream::StreamExt;
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer};
use tracing::info;

use crate::{config::HttpServerConfig, execution::AppExecution};

#[derive(Debug)]
struct ExecRequest {
    sql: String,
}

#[derive(Clone)]
struct ExecutionState {
    execution: AppExecution,
    config: HttpServerConfig,
}

impl ExecutionState {
    pub fn new(execution: AppExecution, config: HttpServerConfig) -> Self {
        Self { execution, config }
    }
}

pub fn create_router(execution: AppExecution, config: HttpServerConfig) -> Router {
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

#[derive(Deserialize)]
struct PostSqlBody {
    sql: String,
    #[serde(default)]
    flightsql: bool,
}

async fn post_sql_handler(state: State<ExecutionState>, Json(body): Json<PostSqlBody>) -> Response {
    if body.flightsql && !cfg!(feature = "flightsql") {
        return (
            StatusCode::BAD_REQUEST,
            "FlightSQL is not enabled on this server",
        )
            .into_response();
    }
    let req = ExecRequest { sql: body.sql };
    let opts = ExecOptions::new(Some(state.config.result_limit), body.flightsql);
    create_response(&state, req, opts).await
}

#[derive(Deserialize)]
struct GetCatalogQueryParams {
    #[serde(default)]
    flightsql: bool,
}

async fn get_catalog_handler(
    state: State<ExecutionState>,
    Query(query): Query<GetCatalogQueryParams>,
) -> Response {
    let opts = ExecOptions::new(None, query.flightsql);
    if opts.flightsql && !cfg!(feature = "flightsql") {
        return (
            StatusCode::BAD_REQUEST,
            "FlightSQL is not enabled on this server",
        )
            .into_response();
    }
    let sql = "SHOW TABLES".to_string();
    let req = ExecRequest { sql };
    create_response(&state, req, opts).await
}

#[derive(Deserialize)]
struct GetTablePathParams {
    catalog: String,
    schema: String,
    table: String,
}

#[derive(Deserialize)]
struct GetTableQueryParams {
    #[serde(default)]
    flightsql: bool,
}

async fn get_table_handler(
    state: State<ExecutionState>,
    Path(params): Path<GetTablePathParams>,
    Query(query): Query<GetTableQueryParams>,
) -> Response {
    let GetTablePathParams {
        catalog,
        schema,
        table,
    } = params;
    let sql = format!(
        "SELECT * FROM \"{catalog}\".\"{schema}\".\"{table}\" LIMIT {}",
        state.config.result_limit
    );
    let req = ExecRequest { sql };
    let opts = ExecOptions::new(Some(state.config.result_limit), query.flightsql);
    create_response(&state, req, opts).await
}

async fn response_for_sql(
    State(state): &State<ExecutionState>,
    sql: String,
    opts: ExecOptions,
) -> (Response, ResponseDetails) {
    info!("Executing sql: {sql}");
    match state.execution.execute_sql_with_opts(&sql, opts).await {
        Ok(ExecResult::RecordBatchStream(stream)) => batch_stream_to_response(stream).await,
        Ok(_) => {
            let res = (
                StatusCode::BAD_REQUEST,
                "Execution failed: unknown result type".to_string(),
            )
                .into_response();
            (res, error_response_details())
        }

        Err(e) => {
            let res = (StatusCode::BAD_REQUEST, format!("{}", e)).into_response();
            (res, error_response_details())
        }
    }
}

struct ResponseDetails {
    rows: u64,
}

fn error_response_details() -> ResponseDetails {
    ResponseDetails { rows: 0 }
}

async fn batch_stream_to_response(
    batch_stream: SendableRecordBatchStream,
) -> (Response, ResponseDetails) {
    let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
    let mut writer = ArrayWriter::new(&mut buf);
    let mut batch_stream = batch_stream;
    let mut rows: usize = 0;
    while let Some(maybe_batch) = batch_stream.next().await {
        match maybe_batch {
            Ok(batch) => {
                if let Err(e) = writer.write(&batch) {
                    error!("Error serializing result batches: {}", e);
                    return (
                        (StatusCode::INTERNAL_SERVER_ERROR, "Serialization error").into_response(),
                        error_response_details(),
                    );
                }
                rows += batch.num_rows()
            }
            Err(e) => {
                error!("Error executing query: {}", e);
                // TODO: Use more appropriate errors, like 404 for table that doesnt exist
                return (
                    (StatusCode::INTERNAL_SERVER_ERROR, "Query execution error").into_response(),
                    error_response_details(),
                );
            }
        }
    }

    if let Err(e) = writer.finish() {
        error!("Error finalizing JSON writer: {}", e);
        return (
            (StatusCode::INTERNAL_SERVER_ERROR, "Finalization error").into_response(),
            error_response_details(),
        );
    }

    match String::from_utf8(buf.into_inner()) {
        Ok(json) => {
            let mut res = Response::new(Body::new(json));
            res.headers_mut()
                .insert("content-type", HeaderValue::from_static("application/json"));
            let details = ResponseDetails { rows: rows as u64 };
            (res, details)
        }
        Err(_) => (
            (StatusCode::INTERNAL_SERVER_ERROR, "UTF-8 conversion error").into_response(),
            error_response_details(),
        ),
    }
}

async fn create_response(
    state: &State<ExecutionState>,
    req: ExecRequest,
    opts: ExecOptions,
) -> Response {
    let start = Timestamp::now();
    let (res, details) = response_for_sql(state, req.sql.clone(), opts).await;
    let elapsed = Timestamp::now() - start;
    let req = ObservabilityRequestDetails {
        sql: req.sql,
        start_ms: start.as_millisecond(),
        duration_ms: elapsed.get_milliseconds(),
        rows: details.rows,
        status: res.status().as_u16(),
    };
    let obs = state.execution.execution_ctx().observability();
    if let Err(e) = obs
        .try_record_request(state.execution.session_ctx(), req)
        .await
    {
        error!("Error recording request: {}", e.to_string())
    }
    res
}

#[cfg(test)]
mod test {
    use axum::body::Body;
    use datafusion_app::{
        config::ExecutionConfig, extensions::DftSessionStateBuilder, local::ExecutionContext,
    };
    use http::{Request, StatusCode};
    use http_body_util::BodyExt;

    use crate::{
        config::HttpServerConfig, execution::AppExecution, server::http::router::create_router,
    };
    use tower::ServiceExt;

    fn setup() -> (AppExecution, HttpServerConfig) {
        let config = ExecutionConfig::default();
        let state = DftSessionStateBuilder::try_new(None)
            .unwrap()
            .build()
            .unwrap();
        let local =
            ExecutionContext::try_new(&config, state, crate::APP_NAME, env!("CARGO_PKG_VERSION"))
                .unwrap();
        let execution = AppExecution::new(local);

        let http_config = HttpServerConfig::default();
        (execution, http_config)
    }

    #[tokio::test]
    async fn test_get_catalog() {
        let (execution, http_config) = setup();
        let router = create_router(execution, http_config);

        let req = Request::builder()
            .uri("/catalog")
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_table() {
        let (execution, http_config) = setup();
        let router = create_router(execution, http_config);

        let req = Request::builder()
            .uri("/table/datafusion/information_schema/df_settings")
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_nonexistent_table() {
        let (execution, http_config) = setup();
        let router = create_router(execution, http_config);

        let req = Request::builder()
            .uri("/table/datafusion/information_schema/df_setting")
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_correct_when_flightsql_not_enabled() {
        let (execution, http_config) = setup();
        let router = create_router(execution, http_config);

        let req = Request::builder()
            .uri("/catalog?flightsql=true")
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, "FlightSQL is not enabled on this server".as_bytes())
    }
}

#[cfg(all(test, feature = "flightsql"))]
mod flightsql_test {
    use axum::body::Body;
    use datafusion_app::{
        config::{ExecutionConfig, FlightSQLConfig},
        extensions::DftSessionStateBuilder,
        flightsql::FlightSQLContext,
        local::ExecutionContext,
    };
    use http::{Request, StatusCode};

    use crate::{
        config::HttpServerConfig, execution::AppExecution, server::http::router::create_router,
    };
    use tower::ServiceExt;

    async fn setup() -> (AppExecution, HttpServerConfig) {
        let config = ExecutionConfig::default();
        let state = DftSessionStateBuilder::try_new(None)
            .unwrap()
            .build()
            .unwrap();
        let local =
            ExecutionContext::try_new(&config, state, crate::APP_NAME, env!("CARGO_PKG_VERSION"))
                .unwrap();
        let mut execution = AppExecution::new(local);
        let flightsql_cfg = FlightSQLConfig {
            connection_url: "localhost:50051".to_string(),
            ..Default::default()
        };
        let flightsql_ctx = FlightSQLContext::new(flightsql_cfg);
        flightsql_ctx
            .create_client(Some("http://localhost:50051".to_string()))
            .await
            .unwrap();
        execution.with_flightsql_ctx(flightsql_ctx);

        let http_config = HttpServerConfig::default();
        (execution, http_config)
    }

    #[tokio::test]
    async fn test_get_catalog() {
        let (execution, http_config) = setup().await;
        let router = create_router(execution, http_config);

        let req = Request::builder()
            .uri("/catalog?flightsql=true")
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_table() {
        let (execution, http_config) = setup().await;
        let router = create_router(execution, http_config);

        let req = Request::builder()
            .uri("/table/datafusion/information_schema/df_settings?flightsql=true")
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_nonexistent_table() {
        let (execution, http_config) = setup().await;
        let router = create_router(execution, http_config);

        let req = Request::builder()
            .uri("/table/datafusion/information_schema/df_setting?flightsql=true")
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }
}

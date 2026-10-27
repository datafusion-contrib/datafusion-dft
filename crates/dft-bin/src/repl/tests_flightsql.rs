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

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{
        server::{FlightSqlService, PeekableFlightDataStream},
        CommandStatementQuery, CommandStatementUpdate, ProstMessageExt, SqlInfo,
        TicketStatementQuery,
    },
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
};
use datafusion::arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
};
use futures::stream;
use prost::Message;
use tonic::{Request, Response, Status};

use super::*;
use crate::{config::ConnectionMode, test_utils::fixture::TestFixture};

#[derive(Clone, Default)]
struct Service {
    legacy_cancellation: bool,
    cancellations: Arc<std::sync::atomic::AtomicUsize>,
    peers: Arc<Mutex<Vec<SocketAddr>>>,
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "answer",
        DataType::Int64,
        false,
    )]))
}

impl Service {
    fn record<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            != Some("Bearer test-token")
            || request
                .metadata()
                .get("x-test")
                .and_then(|v| v.to_str().ok())
                != Some("cli")
        {
            return Err(Status::unauthenticated("Missing merged credentials"));
        }
        self.peers
            .lock()
            .unwrap()
            .push(request.remote_addr().unwrap());
        Ok(())
    }
}

#[tonic::async_trait]
impl FlightSqlService for Service {
    type FlightService = Self;

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.record(&request)?;
        if query.query == "ERROR" {
            return Err(Status::invalid_argument("query failed"));
        }
        let mut info = FlightInfo::new().try_with_schema(&schema()).unwrap();
        if query.query != "NO_ENDPOINTS" {
            for _ in 0..if query.query == "MULTI" { 2 } else { 1 } {
                let ticket = TicketStatementQuery {
                    statement_handle: query.query.clone().into(),
                };
                info = info.with_endpoint(
                    FlightEndpoint::new().with_ticket(Ticket::new(ticket.as_any().encode_to_vec())),
                );
            }
        }
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // Check credentials on result requests as well as statement requests.
        let before = self.peers.lock().unwrap().len();
        self.record(&request)?;
        self.peers.lock().unwrap().truncate(before);
        let query = String::from_utf8(ticket.statement_handle.to_vec()).unwrap();
        let batches = match query.as_str() {
            "EMPTY" => vec![],
            "STREAM_ERROR" => vec![Err(FlightError::ExternalError("stream failed".into()))],
            _ => vec![Ok(RecordBatch::try_new(
                schema(),
                vec![Arc::new(Int64Array::from(vec![42]))],
            )
            .unwrap())],
        };
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema())
            .build(stream::iter(batches));
        Ok(Response::new(Box::pin(stream.map(|result| {
            result.map_err(|e| Status::internal(e.to_string()))
        }))))
    }

    async fn do_put_statement_update(
        &self,
        _query: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        self.record(&request)?;
        Ok(7)
    }

    async fn do_action_fallback(
        &self,
        request: Request<arrow_flight::Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        if self.legacy_cancellation || request.get_ref().r#type != "CancelFlightInfo" {
            return Err(Status::unimplemented("Use legacy cancellation"));
        }
        self.record(&request)?;
        let query =
            arrow_flight::CancelFlightInfoRequest::decode(request.get_ref().body.clone()).unwrap();
        assert!(query.info.is_some());
        self.cancellations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let result =
            arrow_flight::CancelFlightInfoResult::new(arrow_flight::CancelStatus::Cancelled);
        Ok(Response::new(Box::pin(stream::iter(vec![Ok(
            arrow_flight::Result {
                body: result.encode_to_vec().into(),
            },
        )]))))
    }

    async fn do_action_cancel_query(
        &self,
        query: arrow_flight::sql::ActionCancelQueryRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<arrow_flight::sql::ActionCancelQueryResult, Status> {
        self.record(&request)?;
        assert!(FlightInfo::decode(query.info).is_ok());
        self.cancellations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(arrow_flight::sql::ActionCancelQueryResult { result: 1 })
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[test]
fn connection_mode_configuration() {
    let defaults: AppConfig = toml::from_str("").unwrap();
    assert_eq!(
        defaults.flightsql_client.connection_mode,
        ConnectionMode::Persistent
    );
    let config: AppConfig =
        toml::from_str("[flightsql_client]\nconnection_mode = 'per_statement'").unwrap();
    assert_eq!(
        config.flightsql_client.connection_mode,
        ConnectionMode::PerStatement
    );
    assert!(
        toml::from_str::<AppConfig>("[flightsql_client]\nconnection_mode = 'invalid'").is_err()
    );
}

#[tokio::test]
async fn connection_lifetime_headers_empty_results_and_multiple_endpoints() {
    for mode in [ConnectionMode::Persistent, ConnectionMode::PerStatement] {
        let service = Service::default();
        let fixture =
            TestFixture::new(FlightServiceServer::new(service.clone()), "127.0.0.1:0").await;
        let directory = tempfile::tempdir().unwrap();
        let headers_file = directory.path().join("headers");
        std::fs::write(&headers_file, "x-test: file").unwrap();
        let args = DftArgs {
            flightsql: true,
            host: Some(format!("http://{}", fixture.addr)),
            headers_file: Some(headers_file),
            header: Some(vec![("x-test".into(), "cli".into())]),
            ..Default::default()
        };
        let mut config = AppConfig::default();
        config.db.path = url::Url::from_directory_path(directory.path()).unwrap();
        config.flightsql_client.connection_mode = mode;
        config.flightsql_client.auth.bearer_token = Some("test-token".into());
        config
            .flightsql_client
            .headers
            .insert("x-test".into(), "config".into());
        let execution = create_execution(&args, &config, false).await.unwrap();
        let (sender, mut output) = mpsc::channel(32);
        for statements in [vec!["MULTI", "EMPTY", "NO_ENDPOINTS"], vec!["MULTI"]] {
            run_statements(
                execution.clone(),
                &args,
                &config,
                statements.into_iter().map(str::to_owned).collect(),
                false,
                &sender,
            )
            .await
            .unwrap();
        }
        let mut rows = 0;
        while let Ok(event) = output.try_recv() {
            if let Output::Batch(batch) = event {
                rows += batch.num_rows();
            }
        }
        assert_eq!(rows, 4);
        let peers = service.peers.lock().unwrap().clone();
        assert_eq!(peers.len(), 4);
        for pair in peers.windows(2) {
            assert_eq!(pair[0] == pair[1], mode == ConnectionMode::Persistent);
        }
        run_statements(
            execution.clone(),
            &args,
            &config,
            vec!["INSERT INTO remote_table VALUES (1)".into()],
            true,
            &sender,
        )
        .await
        .unwrap();
        assert!(
            matches!(output.try_recv().unwrap(), Output::Text(text) if text.starts_with("7 row(s) affected"))
        );
        for sql in ["ERROR", "STREAM_ERROR"] {
            assert!(run_statements(
                execution.clone(),
                &args,
                &config,
                vec![sql.into()],
                false,
                &sender
            )
            .await
            .is_err());
        }
        // A failed statement must not prevent a later submission.
        run_statements(
            execution.clone(),
            &args,
            &config,
            vec!["EMPTY".into()],
            false,
            &sender,
        )
        .await
        .unwrap();
        drop(execution);
        fixture.shutdown_and_wait().await;
    }
}

#[tokio::test]
async fn remote_datafusion_ddl_and_dml_share_session() {
    use crate::test_utils::fixture::TestFlightSqlServiceImpl;
    let service = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(service.service(), "127.0.0.1:0").await;
    let directory = tempfile::tempdir().unwrap();
    let args = DftArgs {
        flightsql: true,
        host: Some(format!("http://{}", fixture.addr)),
        ..Default::default()
    };
    let mut config = AppConfig::default();
    config.db.path = url::Url::from_directory_path(directory.path()).unwrap();
    let execution = create_execution(&args, &config, false).await.unwrap();
    let (sender, mut output) = mpsc::channel(32);
    for sql in [
        "CREATE TABLE repl_remote AS SELECT 1 AS value",
        "INSERT INTO repl_remote VALUES (2)",
        "SELECT SUM(value) FROM repl_remote",
    ] {
        run_statements(
            execution.clone(),
            &args,
            &config,
            vec![sql.into()],
            false,
            &sender,
        )
        .await
        .unwrap();
    }
    let mut last_batch = None;
    while let Ok(event) = output.try_recv() {
        if let Output::Batch(batch) = event {
            last_batch = Some(batch);
        }
    }
    let batch = last_batch.unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    drop(execution);
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn requests_modern_and_legacy_server_cancellation() {
    for legacy_cancellation in [false, true] {
        let service = Service {
            legacy_cancellation,
            ..Default::default()
        };
        let fixture =
            TestFixture::new(FlightServiceServer::new(service.clone()), "127.0.0.1:0").await;
        let ctx = datafusion_app::flightsql::FlightSQLContext::new(
            datafusion_app::config::FlightSQLConfig {
                connection_url: format!("http://{}", fixture.addr),
                auth: datafusion_app::config::AuthConfig {
                    bearer_token: Some("test-token".into()),
                    ..Default::default()
                },
                headers: [("x-test".into(), "cli".into())].into_iter().collect(),
                ..Default::default()
            },
        );
        ctx.create_client(None, None).await.unwrap();
        let (info, stream) = ctx.execute_query("EMPTY").await.unwrap();
        drop(stream);
        ctx.cancel_query(info).await.unwrap();
        assert_eq!(
            service
                .cancellations
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        drop(ctx);
        fixture.shutdown_and_wait().await;
    }
}

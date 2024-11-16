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

use crate::execution::{local::ExecutionContext, AppExecution};
use crate::test_utils::trailers_layer::TrailersLayer;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{Any, CommandStatementQuery, SqlInfo, TicketStatementQuery};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::parser::DFParser;
use futures::{StreamExt, TryStreamExt};
use log::{debug, error, info};
use prost::Message;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status};
use uuid::Uuid;

#[derive(Clone)]
pub struct FlightSqlServiceImpl {
    requests: Arc<Mutex<HashMap<Uuid, LogicalPlan>>>,
    execution: ExecutionContext,
}

impl FlightSqlServiceImpl {
    pub fn new(execution: AppExecution) -> Self {
        let requests = HashMap::new();
        Self {
            execution: execution.execution_ctx().clone(),
            requests: Arc::new(Mutex::new(requests)),
        }
    }

    /// Return an [`FlightServiceServer`] that can be used with a
    /// [`Server`](tonic::transport::Server)
    pub fn service(&self) -> FlightServiceServer<Self> {
        // wrap up tonic goop
        FlightServiceServer::new(self.clone())
    }

    async fn get_flight_info_statement_handler(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_statement query: {:?}", query);
        debug!("get_flight_info_statement request: {:?}", request);
        let CommandStatementQuery { query, .. } = query;
        let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
        match DFParser::parse_sql_with_dialect(&query, &dialect) {
            Ok(statements) => {
                let statement = statements[0].clone();
                let start = std::time::Instant::now();

                let logical_plan = self.execution.statement_to_logical_plan(statement).await;

                match logical_plan {
                    Ok(logical_plan) => {
                        debug!("logical planning took: {:?}", start.elapsed());
                        let schema = logical_plan.schema();

                        let uuid = uuid::Uuid::new_v4();
                        let ticket = TicketStatementQuery {
                            statement_handle: uuid.to_string().into(),
                        };
                        info!("created ticket handle: {:?}", ticket.statement_handle);
                        let mut bytes: Vec<u8> = Vec::new();
                        if ticket.encode(&mut bytes).is_ok() {
                            let info = FlightInfo::new()
                                .try_with_schema(schema.as_arrow())
                                .unwrap()
                                .with_endpoint(
                                    FlightEndpoint::new().with_ticket(Ticket::new(bytes)),
                                )
                                .with_descriptor(FlightDescriptor::new_cmd(query));
                            debug!("flight info: {:?}", info);

                            let mut guard = self.requests.lock().map_err(|_| {
                                Status::internal("Failed to acquire lock on requests")
                            })?;
                            guard.insert(uuid, logical_plan);

                            Ok(Response::new(info))
                        } else {
                            error!("Error encoding ticket");
                            Err(Status::internal("Error encoding ticket"))
                        }
                    }
                    Err(e) => {
                        error!("Error planning SQL query: {:?}", e);
                        Err(Status::internal("Error planning SQL query"))
                    }
                }
            }
            Err(e) => {
                error!("Error parsing SQL query: {:?}", e);
                Err(Status::internal("Error parsing SQL query"))
            }
        }
    }

    async fn do_get_fallback_handler(
        &self,
        request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_fallback request: {:?}", request);
        debug!("do_get_fallback message: {:?}", message);
        let ticket = request.into_inner();
        let bytes = ticket.ticket.to_vec();
        match TicketStatementQuery::decode(bytes.as_slice()) {
            Ok(query) => {
                let handle = query.statement_handle.clone();
                match String::from_utf8(handle.to_vec()) {
                    Ok(s) => {
                        match Uuid::from_str(&s) {
                            Ok(id) => {
                                info!("getting plan for id: {:?}", id);
                                // Limit the scope of the lock
                                let maybe_plan = {
                                    let guard = self.requests.lock().map_err(|_| {
                                        Status::internal("Failed to acquire lock on requests")
                                    })?;
                                    guard.get(&id).cloned()
                                };
                                if let Some(plan) = maybe_plan {
                                    let stream = self
                                        .execution
                                        .execute_logical_plan(plan)
                                        .await
                                        .map_err(|e| Status::internal(e.to_string()))?;
                                    let builder = FlightDataEncoderBuilder::new();
                                    let flight_data_stream =
                                        builder
                                            .build(stream.map_err(|e| {
                                                FlightError::ExternalError(Box::new(e))
                                            }))
                                            .map_err(|e| Status::internal(e.to_string()))
                                            .boxed();
                                    Ok(Response::new(flight_data_stream))
                                } else {
                                    Err(Status::internal("Plan not found for id"))
                                }
                            }
                            Err(e) => {
                                error!("error decoding handle to uuid: {:?}", e);
                                Err(Status::internal("Error decoding handle to uuid"))
                            }
                        }
                    }
                    Err(e) => {
                        error!("error decoding handle to utf8: {:?}", e);
                        Err(Status::internal("Error decoding handle to utf8"))
                    }
                }
            }
            Err(e) => {
                error!("Error decoding ticket: {:?}", e);
                Err(Status::internal("Error decoding ticket"))
            }
        }
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.get_flight_info_statement_handler(query, request).await
    }

    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.do_get_fallback_handler(request, message).await
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

const DEFAULT_TIMEOUT_SECONDS: u64 = 60;

/// Creates and manages a running FlightSqlServer with a background task
pub struct FlightSqlApp {
    /// channel to send shutdown command
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,

    /// Address the server is listening on
    pub addr: SocketAddr,

    /// handle for the server task
    handle: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl FlightSqlApp {
    /// create a new app for the flightsql server
    #[allow(dead_code)]
    pub async fn new<T: FlightService>(
        flightsql_server: FlightServiceServer<T>,
        addr: &str,
    ) -> Self {
        // let OS choose a free port
        let listener = TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();

        // prepare the shutdown channel
        let (tx, rx) = tokio::sync::oneshot::channel();

        let server_timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECONDS);

        let shutdown_future = async move {
            rx.await.ok();
        };

        let serve_future = tonic::transport::Server::builder()
            .timeout(server_timeout)
            .layer(TrailersLayer)
            .add_service(flightsql_server)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                shutdown_future,
            );

        // Run the server in its own background task
        let handle = tokio::task::spawn(serve_future);

        Self {
            shutdown: Some(tx),
            addr,
            handle: Some(handle),
        }
    }

    /// Stops the server and waits for the server to shutdown
    pub async fn shutdown_and_wait(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).expect("server quit early");
        }
        if let Some(handle) = self.handle.take() {
            handle
                .await
                .expect("task join error (panic?)")
                .expect("Server Error found at shutdown");
        }
    }

    pub async fn run_app(self) {
        if let Some(handle) = self.handle {
            handle
                .await
                .expect("Unable to run server task")
                .expect("Server Error found at shutdown");
        } else {
            panic!("Server task not found");
        }
    }
}

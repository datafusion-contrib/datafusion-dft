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

use crate::test_utils::trailers_layer::TrailersLayer;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{Any, CommandStatementQuery, SqlInfo, TicketStatementQuery};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::parser::DFParser;
use futures::{StreamExt, TryStreamExt};
use log::info;
use prost::Message;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Response, Status};
use uuid::Uuid;

#[derive(Clone)]
pub struct FlightSqlServiceImpl {
    requests: Arc<Mutex<HashMap<Uuid, LogicalPlan>>>,
    context: SessionContext,
}

impl FlightSqlServiceImpl {
    pub fn new() -> Self {
        let context = SessionContext::new();
        let requests = HashMap::new();
        Self {
            context,
            requests: Arc::new(Mutex::new(requests)),
        }
    }

    /// Return an [`FlightServiceServer`] that can be used with a
    /// [`Server`](tonic::transport::Server)
    pub fn service(&self) -> FlightServiceServer<Self> {
        // wrap up tonic goop
        FlightServiceServer::new(self.clone())
    }
}

impl Default for FlightSqlServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_statement");
        let CommandStatementQuery { query, .. } = query;
        let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
        let statements = DFParser::parse_sql_with_dialect(&query, &dialect).unwrap();
        let statement = statements[0].clone();
        let logical_plan = self
            .context
            .state()
            .statement_to_plan(statement)
            .await
            .unwrap();
        let schema = logical_plan.schema();

        let uuid = uuid::Uuid::new_v4();
        let ticket = TicketStatementQuery {
            statement_handle: uuid.to_string().into(),
        };
        let mut bytes: Vec<u8> = Vec::new();
        ticket.encode(&mut bytes).unwrap();

        let info = FlightInfo::new()
            .try_with_schema(schema.as_arrow())
            .unwrap()
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(bytes)))
            .with_descriptor(FlightDescriptor::new_cmd(query));

        let mut guard = self.requests.lock().unwrap();
        guard.insert(uuid, logical_plan);

        Ok(Response::new(info))
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
        _message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_fallback");
        let ticket = request.into_inner();
        let bytes = ticket.ticket.to_vec();
        let query = TicketStatementQuery::decode(bytes.as_slice()).unwrap();
        let handle = query.statement_handle.clone();
        let s = String::from_utf8(handle.to_vec()).unwrap();
        let id = Uuid::from_str(&s).unwrap();

        // Limit the scope of the lock
        let maybe_plan = {
            let guard = self.requests.lock().unwrap();
            guard.get(&id).cloned()
        };
        if let Some(plan) = maybe_plan {
            let df = self.context.execute_logical_plan(plan).await.unwrap();
            let stream = df
                .execute_stream()
                .await
                .unwrap()
                .map_err(|e| FlightError::ExternalError(Box::new(e)));
            let builder = FlightDataEncoderBuilder::new();
            let flight_data_stream = builder.build(stream);
            let b = flight_data_stream
                .map_err(|_| Status::internal("Hi"))
                .boxed();
            let res = Response::new(b);
            Ok(res)
        } else {
            Err(Status::unimplemented("Not implemented"))
        }
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

    /// Return a [`Channel`] connected to the TestServer
    #[allow(dead_code)]
    pub async fn channel(&self) -> Channel {
        let url = format!("http://{}", self.addr);
        let uri: Uri = url.parse().expect("Valid URI");
        Channel::builder(uri)
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECONDS))
            .connect()
            .await
            .expect("error connecting to server")
    }

    /// Stops the test server and waits for the server to shutdown
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

    pub async fn run(self) {
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

// impl Drop for FlightSqlApp {
//     fn drop(&mut self) {
//         let now = std::time::Instant::now();
//         if let Some(shutdown) = self.shutdown.take() {
//             println!("TestFixture shutting down at {:?}", now);
//             shutdown.send(()).ok();
//         }
//         if self.handle.is_some() {
//             // tests should properly clean up TestFixture
//             println!("TestFixture::Drop called prior to `shutdown_and_wait`");
//         }
//     }
// }

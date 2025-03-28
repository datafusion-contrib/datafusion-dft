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

use crate::execution::AppExecution;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{Any, CommandStatementQuery, SqlInfo, TicketStatementQuery};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use color_eyre::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::parser::DFParser;
use datafusion_app::local::ExecutionContext;
use datafusion_app::observability::ObservabilityRequestDetails;
use futures::{StreamExt, TryStreamExt};
use jiff::Timestamp;
use log::{debug, error, info};
use metrics::{counter, histogram};
use prost::Message;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tonic::{Code, Request, Response, Status};
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
        query: String,
        request_id: Uuid,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_statement query: {:?}", query);
        debug!("get_flight_info_statement request: {:?}", request);
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

                        let ticket = TicketStatementQuery {
                            statement_handle: request_id.to_string().into(),
                        };
                        debug!("created ticket handle: {:?}", ticket.statement_handle);
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
                            guard.insert(request_id, logical_plan);

                            Ok(Response::new(info))
                        } else {
                            error!("error encoding ticket");
                            Err(Status::internal("Error encoding ticket"))
                        }
                    }
                    Err(e) => {
                        error!("error planning SQL query: {:?}", e);
                        Err(Status::internal("Error planning SQL query"))
                    }
                }
            }
            Err(e) => {
                error!("error parsing SQL query: {:?}", e);
                Err(Status::internal("Error parsing SQL query"))
            }
        }
    }

    async fn do_get_fallback_handler(
        &self,
        request_id: String,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_fallback message: {:?}", message);

        match Uuid::from_str(&request_id) {
            Ok(id) => {
                info!("getting plan for id: {:?}", id);
                // Limit the scope of the lock
                let maybe_plan = {
                    let guard = self
                        .requests
                        .lock()
                        .map_err(|_| Status::internal("Failed to acquire lock on requests"))?;
                    guard.get(&id).cloned()
                };
                if let Some(plan) = maybe_plan {
                    let stream = self
                        .execution
                        .execute_logical_plan(plan)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                    let builder = FlightDataEncoderBuilder::new();
                    let flight_data_stream = builder
                        .build(stream.map_err(|e| FlightError::ExternalError(Box::new(e))))
                        .map_err(|e| Status::internal(e.to_string()))
                        .boxed();
                    Ok(Response::new(flight_data_stream))
                } else {
                    Err(Status::internal("Plan not found for id"))
                }
            }
            Err(e) => {
                error!("error decoding handle to uuid for {request_id}: {:?}", e);
                Err(Status::internal(
                    "Error decoding handle to uuid for {request_id}",
                ))
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
        counter!("requests", "endpoint" => "get_flight_info").increment(1);
        let start = Timestamp::now();
        let CommandStatementQuery { query, .. } = query;
        let request_id = uuid::Uuid::new_v4();
        let res = self
            .get_flight_info_statement_handler(query.clone(), request_id, request)
            .await;
        let duration = Timestamp::now() - start;

        let grpc_code = match &res {
            Ok(_) => Code::Ok,
            Err(status) => status.code(),
        };

        let ctx = self.execution.session_ctx();
        let req = ObservabilityRequestDetails {
            request_id: Some(request_id.to_string()),
            path: "GetFlightInfo".to_string(),
            sql: Some(query),
            rows: None,
            start_ms: start.as_millisecond(),
            duration_ms: duration.get_milliseconds(),
            status: grpc_code as u16,
        };
        if let Err(e) = self
            .execution
            .observability()
            .try_record_request(ctx, req)
            .await
        {
            error!("Error recording request: {}", e.to_string())
        }
        histogram!("get_flight_info_latency_ms").record(duration.get_milliseconds() as f64);
        res
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
        counter!("requests", "endpoint" => "do_get_fallback").increment(1);
        let start = Timestamp::now();
        let request_id =
            try_request_id_from_request(request).map_err(|e| Status::internal(e.to_string()))?;
        debug!("do_get_fallback for request_id: {}", &request_id);
        let res = self
            .do_get_fallback_handler(request_id.clone(), message)
            .await;

        let duration = Timestamp::now() - start;
        let grpc_code = match &res {
            Ok(_) => Code::Ok,
            Err(status) => status.code(),
        };
        let ctx = self.execution.session_ctx();
        let req = ObservabilityRequestDetails {
            request_id: Some(request_id),
            path: "DoGetFallback".to_string(),
            sql: None,
            rows: None,
            start_ms: start.as_millisecond(),
            duration_ms: duration.get_milliseconds(),
            status: grpc_code as u16,
        };
        if let Err(e) = self
            .execution
            .observability()
            .try_record_request(ctx, req)
            .await
        {
            error!("Error recording request: {}", e.to_string())
        }

        histogram!("do_get_fallback_latency_ms").record(duration.get_milliseconds() as f64);
        res
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

fn try_request_id_from_request(request: Request<Ticket>) -> Result<String> {
    let ticket = request.into_inner();
    let bytes = ticket.ticket.to_vec();

    let request_id = String::from_utf8(
        TicketStatementQuery::decode(bytes.as_slice())?
            .statement_handle
            .to_vec(),
    )?;
    Ok(request_id)
}

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
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, Any,
    CommandGetCatalogs, CommandGetDbSchemas, CommandGetTables, CommandPreparedStatementQuery,
    CommandStatementQuery, DoPutPreparedStatementResult, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, IpcMessage, SchemaAsIpc, Ticket,
};
use color_eyre::Result;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{col, lit};
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

    /// Return a [`FlightServiceServer`] that can be used with a
    /// [`Server`](tonic::transport::Server)
    pub fn service(&self) -> FlightServiceServer<Self> {
        // wrap up tonic goop
        FlightServiceServer::new(self.clone())
    }

    async fn do_get_common_handler(
        &self,
        request_id: String,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
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
                    Err(Status::internal("plan not found for id"))
                }
            }
            Err(e) => {
                error!("error decoding handle to uuid for {request_id}: {:?}", e);
                Err(Status::internal(
                    "error decoding handle to uuid for {request_id}",
                ))
            }
        }
    }

    async fn record_request(
        &self,
        start: Timestamp,
        request_id: Option<String>,
        response_err: Option<&Status>,
        path: String,
        latency_metric: &'static str,
    ) {
        let duration = Timestamp::now() - start;
        let grpc_code = match &response_err {
            None => Code::Ok,
            Some(status) => status.code(),
        };
        let ctx = self.execution.session_ctx();
        let req = ObservabilityRequestDetails {
            request_id,
            path,
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
            error!("error recording request: {}", e.to_string())
        }

        histogram!(latency_metric).record(duration.get_milliseconds() as f64);
    }

    async fn create_flight_info_for_logical_plan(
        &self,
        logical_plan: LogicalPlan,
        request_id: Uuid,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
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
                .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(bytes.clone())))
                .with_descriptor(FlightDescriptor::new_cmd(bytes));
            debug!("created flight info: {:?}", info);

            let mut guard = self
                .requests
                .lock()
                .map_err(|_| Status::internal("failed to acquire lock on requests"))?;
            guard.insert(request_id, logical_plan);

            Ok(Response::new(info))
        } else {
            error!("error encoding ticket");
            Err(Status::internal("error encoding ticket"))
        }
    }

    async fn create_flight_info(
        &self,
        query: String,
        request_id: Uuid,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!(
            "creating flight info for request id {request_id} with query: {:?}",
            query
        );
        let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
        match DFParser::parse_sql_with_dialect(&query, &dialect) {
            Ok(statements) => {
                let statement = statements[0].clone();
                let start = std::time::Instant::now();

                let logical_plan = self
                    .execution
                    .statement_to_logical_plan(statement)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;

                debug!("logical planning took: {:?}", start.elapsed());
                self.create_flight_info_for_logical_plan(logical_plan, request_id, request)
                    .await
            }
            Err(e) => {
                error!("error parsing SQL query: {:?}", e);
                Err(Status::internal("error parsing SQL query"))
            }
        }
    }

    async fn query_to_create_prepared_statement_action(
        &self,
        query: String,
    ) -> Result<ActionCreatePreparedStatementResult> {
        let ctx = self.execution.session_ctx();
        let state = ctx.state();
        let logical_plan = state.create_logical_plan(&query).await?;
        let query_schema = logical_plan.schema().as_arrow().clone();
        let (parameter_data_types, parameter_fields): (Vec<DataType>, Vec<Field>) = logical_plan
            .get_parameter_types()?
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (v.clone(), Field::new(k, v, false))))
            .collect();
        let parameters_schema = Schema::new(parameter_fields);

        let id = Uuid::new_v4().to_string();
        let builder = LogicalPlanBuilder::new(logical_plan);
        let prepared = builder
            .prepare(id.clone(), parameter_data_types)
            .map_err(|e| Status::internal(e.to_string()))?
            .build()?;
        ctx.execute_logical_plan(prepared).await?.collect().await?;
        debug!("created prepared statement with id {id}");
        let opts = IpcWriteOptions::default();
        let dataset_schema_as_ipc = SchemaAsIpc::new(&query_schema, &opts);
        let IpcMessage(dataset_bytes) = IpcMessage::try_from(dataset_schema_as_ipc)
            .map_err(|e| Status::internal(e.to_string()))?;
        let parameters_schema_as_ipc = SchemaAsIpc::new(&parameters_schema, &opts);
        let IpcMessage(parameters_bytes) = IpcMessage::try_from(parameters_schema_as_ipc)?;
        debug!("serialized prepared statement");
        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: id.into_bytes().into(),
            dataset_schema: dataset_bytes,
            parameter_schema: parameters_bytes,
        };
        Ok(res)
    }

    async fn do_get_statement_handler(
        &self,
        request_id: String,
        ticket: TicketStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_statement ticket: {:?}", ticket);
        self.do_get_common_handler(request_id).await
    }

    async fn do_get_fallback_handler(
        &self,
        request_id: String,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_fallback message: {:?}", message);
        self.do_get_common_handler(request_id).await
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        counter!("requests", "endpoint" => "get_flight_info_catalogs").increment(1);
        let start = Timestamp::now();
        let request_id = uuid::Uuid::new_v4();
        let query = "SELECT DISTINCT table_catalog FROM information_schema.tables".to_string();
        let res = self.create_flight_info(query, request_id, request).await;

        // TODO: Move recording to after response is sent to not impact response latency
        self.record_request(
            start,
            Some(request_id.to_string()),
            res.as_ref().err(),
            "/get_flight_info_catalogs".to_string(),
            "get_flight_info_catalogs_latency_ms",
        )
        .await;
        res
    }

    async fn get_flight_info_schemas(
        &self,
        command: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        counter!("requests", "endpoint" => "get_flight_info_schemas").increment(1);
        let start = Timestamp::now();
        let request_id = uuid::Uuid::new_v4();
        let CommandGetDbSchemas {
            catalog,
            db_schema_filter_pattern,
        } = command;
        let query = match (catalog, db_schema_filter_pattern) {
            (Some(catalog), Some(filter)) => format!("SELECT DISTINCT table_catalog, table_schema FROM information_schema.tables WHERE table_catalog = '{catalog}' AND table_schema ILIKE '%{filter}%' ORDER BY table_catalog, table_schema"),
            (None, Some(filter)) => format!("SELECT DISTINCT table_catalog, table_schema FROM information_schema.tables WHERE table_schema ILIKE '%{filter}%' ORDER BY table_catalog, table_schema"),
            (Some(catalog), None) => format!("SELECT DISTINCT table_catalog, table_schema FROM information_schema.tables WHERE table_catalog = '{catalog}' ORDER BY table_catalog, table_schema"),
            (None, None) => "SELECT DISTINCT table_catalog, table_schema FROM information_schema.tables ORDER BY table_catalog, table_schema".to_string()
        };
        let res = self.create_flight_info(query, request_id, request).await;

        // TODO: Move recording to after response is sent to not impact response latency
        self.record_request(
            start,
            Some(request_id.to_string()),
            res.as_ref().err(),
            "/get_flight_info_schemas".to_string(),
            "get_flight_info_schemas_latency_ms",
        )
        .await;
        res
    }

    async fn get_flight_info_tables(
        &self,
        command: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        counter!("requests", "endpoint" => "get_flight_info_tables").increment(1);
        let start = Timestamp::now();
        let CommandGetTables {
            catalog,
            db_schema_filter_pattern,
            table_name_filter_pattern,
            table_types,
            include_schema: _include_schema,
        } = command;
        let base_query = "SELECT * FROM information_schema.tables";

        let mut df = self
            .execution
            .session_ctx()
            .sql(base_query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        if let Some(catalog) = catalog {
            df = df
                .filter(col("table_catalog").eq(lit(catalog)))
                .map_err(|e| Status::internal(e.to_string()))?;
        };
        if let Some(schema_filter) = db_schema_filter_pattern {
            df = df
                .filter(col("table_schema").ilike(lit(format!("%{schema_filter}%"))))
                .map_err(|e| Status::internal(e.to_string()))?;
        };
        if let Some(table_filter) = table_name_filter_pattern {
            df = df
                .filter(col("table_name").ilike(lit(format!("%{table_filter}%"))))
                .map_err(|e| Status::internal(e.to_string()))?;
        };
        if !table_types.is_empty() {
            let table_exprs = table_types.iter().map(lit).collect();
            df = df
                .filter(col("table_type").in_list(table_exprs, false))
                .map_err(|e| Status::internal(e.to_string()))?;
        };
        df = df
            .sort(vec![
                col("table_catalog").sort(true, false),
                col("table_schema").sort(true, false),
                col("table_name").sort(true, false),
            ])
            .map_err(|e| Status::internal(e.to_string()))?;
        let logical_plan = df.into_unoptimized_plan();

        let request_id = uuid::Uuid::new_v4();
        let res = self
            .create_flight_info_for_logical_plan(logical_plan, request_id, request)
            .await;

        // TODO: Move recording to after response is sent to not impact response latency
        self.record_request(
            start,
            Some(request_id.to_string()),
            res.as_ref().err(),
            "/get_flight_info_tables".to_string(),
            "get_flight_info_tables_latency_ms",
        )
        .await;
        res
    }

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
            .create_flight_info(query.clone(), request_id, request)
            .await;

        // TODO: Move recording to after response is sent to not impact response latency
        self.record_request(
            start,
            Some(request_id.to_string()),
            res.as_ref().err(),
            "/get_flight_info_statement".to_string(),
            "get_flight_info_statement_latency_ms",
        )
        .await;
        res
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        counter!("requests", "endpoint" => "do_action_create_prepared_statement").increment(1);
        let ActionCreatePreparedStatementRequest { query, .. } = query;
        let res = self
            .query_to_create_prepared_statement_action(query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(res)
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult> {
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        counter!("requests", "endpoint" => "do_get_statement").increment(1);
        let start = Timestamp::now();
        let request_id =
            try_request_id_from_request(request).map_err(|e| Status::internal(e.to_string()))?;
        debug!("do_get_statement for request_id: {}", &request_id);
        let res = self
            .do_get_statement_handler(request_id.clone(), ticket)
            .await;

        // TODO: Move recording to after response is sent to not impact response latency
        self.record_request(
            start,
            Some(request_id),
            res.as_ref().err(),
            "/do_get_statement".to_string(),
            "do_get_statement_latency_ms",
        )
        .await;
        res
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

        // TODO: Move recording to after response is sent to not impact response latency
        self.record_request(
            start,
            Some(request_id),
            res.as_ref().err(),
            "/do_get_fallback".to_string(),
            "do_get_fallback_latency_ms",
        )
        .await;
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

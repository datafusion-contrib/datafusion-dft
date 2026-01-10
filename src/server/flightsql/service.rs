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
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, Any, CommandGetCatalogs, CommandGetDbSchemas,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
    CommandPreparedStatementQuery, CommandStatementQuery, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{Action, FlightDescriptor, FlightEndpoint, FlightInfo, IpcMessage, SchemaAsIpc, Ticket};
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::error::ArrowError;
use prost::bytes::Bytes;
use color_eyre::Result;
use datafusion::arrow::datatypes::Schema;
use datafusion::logical_expr::LogicalPlan;
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

/// Prepared statement handle containing the logical plan and metadata
#[derive(Clone)]
pub struct PreparedStatementHandle {
    pub plan: LogicalPlan,
    pub parameter_schema: Option<Arc<Schema>>,
    pub dataset_schema: Arc<Schema>,
    pub created_at: Timestamp,
}

#[derive(Clone)]
pub struct FlightSqlServiceImpl {
    requests: Arc<Mutex<HashMap<Uuid, LogicalPlan>>>,
    prepared_statements: Arc<Mutex<HashMap<Uuid, PreparedStatementHandle>>>,
    execution: ExecutionContext,
}

impl FlightSqlServiceImpl {
    pub fn new(execution: AppExecution) -> Self {
        let requests = HashMap::new();
        let prepared_statements = HashMap::new();
        Self {
            execution: execution.execution_ctx().clone(),
            requests: Arc::new(Mutex::new(requests)),
            prepared_statements: Arc::new(Mutex::new(prepared_statements)),
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
            error!("error recording request: {}", e)
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

    async fn get_flight_info_table_types(
        &self,
            _query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        counter!("requests", "endpoint" => "get_flight_info_table_types").increment(1);
        let start = Timestamp::now();
        let request_id = uuid::Uuid::new_v4();
        let query =
            "SELECT DISTINCT table_type FROM information_schema.tables ORDER BY table_type"
                .to_string();
        let res = self.create_flight_info(query, request_id, request).await;

        // TODO: Move recording to after response is sent to not impact response latency
        self.record_request(
            start,
            Some(request_id.to_string()),
            res.as_ref().err(),
            "/get_flight_info_table_types".to_string(),
            "get_flight_info_table_types_latency_ms",
        )
        .await;
        res
    }

    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        counter!("requests", "endpoint" => "get_flight_info_sql_info").increment(1);
        let start = Timestamp::now();
        let request_id = uuid::Uuid::new_v4();

        // For now, return basic server info via a simple SQL query
        // TODO: Implement full SqlInfo support with DenseUnion schema
        let query = format!(
            "SELECT '{}' as server_name, '{}' as server_version, '57.0' as arrow_version, false as read_only",
            "datafusion-dft",
            env!("CARGO_PKG_VERSION")
        );

        let res = self.create_flight_info(query, request_id, request).await;

        // TODO: Move recording to after response is sent to not impact response latency
        self.record_request(
            start,
            Some(request_id.to_string()),
            res.as_ref().err(),
            "/get_flight_info_sql_info".to_string(),
            "get_flight_info_sql_info_latency_ms",
        )
        .await;
        res
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        counter!("requests", "endpoint" => "get_flight_info_xdbc_type_info").increment(1);
        let start = Timestamp::now();
        let request_id = uuid::Uuid::new_v4();

        // Build query to return XDBC type info for DataFusion-supported types
        // If data_type filter is provided, we would filter to that type
        // For now, return all supported types
        let type_filter = if let Some(data_type) = query.data_type {
            format!(" WHERE data_type = {}", data_type)
        } else {
            String::new()
        };

        // Return basic type information for common Arrow/DataFusion types
        // This is a simplified version - a full implementation would include all XDBC type metadata
        let query = format!(
            "SELECT * FROM (VALUES \
                (-5, 'BIGINT', 19, NULL, NULL, NULL, 1, 0, 3, 0, 0, 0, 'BIGINT', -5, 0, 10, 0), \
                (4, 'INTEGER', 10, NULL, NULL, NULL, 1, 0, 3, 0, 0, 0, 'INTEGER', 4, 0, 10, 0), \
                (5, 'SMALLINT', 5, NULL, NULL, NULL, 1, 0, 3, 0, 0, 0, 'SMALLINT', 5, 0, 10, 0), \
                (-6, 'TINYINT', 3, NULL, NULL, NULL, 1, 0, 3, 0, 0, 0, 'TINYINT', -6, 0, 10, 0), \
                (8, 'DOUBLE', 15, NULL, NULL, NULL, 1, 0, 3, 0, 0, 0, 'DOUBLE PRECISION', 8, 0, 2, 0), \
                (7, 'REAL', 7, NULL, NULL, NULL, 1, 0, 3, 0, 0, 0, 'REAL', 7, 0, 2, 0), \
                (12, 'VARCHAR', 2147483647, '''', '''', 'length', 1, 1, 3, 0, 0, 0, 'VARCHAR', 12, 0, 0, 0), \
                (91, 'DATE', 10, '''', '''', NULL, 1, 0, 3, 0, 0, 0, 'DATE', 91, 0, 0, 0), \
                (93, 'TIMESTAMP', 23, '''', '''', NULL, 1, 0, 3, 0, 0, 0, 'TIMESTAMP', 93, 3, 0, 0), \
                (-7, 'BOOLEAN', 1, NULL, NULL, NULL, 1, 0, 3, 0, 0, 0, 'BOOLEAN', -7, 0, 0, 0), \
                (-2, 'BINARY', 2147483647, '''', '''', 'length', 1, 0, 3, 0, 0, 0, 'BINARY', -2, 0, 0, 0), \
                (2, 'DECIMAL', 38, NULL, NULL, 'precision,scale', 1, 0, 3, 0, 0, 0, 'DECIMAL', 2, 0, 10, 0) \
            ) AS types(\
                type_name_num, type_name_str, column_size, literal_prefix, literal_suffix, create_params, \
                nullable, case_sensitive, searchable, unsigned_attribute, fixed_prec_scale, auto_increment, \
                local_type_name, data_type, minimum_scale, maximum_scale, sql_datetime_sub\
            ){type_filter}"
        );

        let res = self.create_flight_info(query, request_id, request).await;

        // TODO: Move recording to after response is sent to not impact response latency
        self.record_request(
            start,
            Some(request_id.to_string()),
            res.as_ref().err(),
            "/get_flight_info_xdbc_type_info".to_string(),
            "get_flight_info_xdbc_type_info_latency_ms",
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

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        counter!("requests", "endpoint" => "do_action_create_prepared_statement").increment(1);
        let start = Timestamp::now();
        let request_id = Uuid::new_v4();

        debug!("Creating prepared statement for SQL: {}", query.query);

        // Parse and create logical plan
        let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
        let statements = DFParser::parse_sql_with_dialect(&query.query, &dialect)
            .map_err(|e| Status::internal(format!("Failed to parse SQL: {}", e)))?;

        if statements.is_empty() {
            return Err(Status::invalid_argument("No SQL statement provided"));
        }

        let statement = statements[0].clone();
        let logical_plan = self
            .execution
            .statement_to_logical_plan(statement)
            .await
            .map_err(|e| Status::internal(format!("Failed to create logical plan: {}", e)))?;

        // Extract schemas
        let dataset_schema = logical_plan.schema().as_arrow().clone();
        // For now, we don't support parameterized queries, so parameter_schema is None
        let parameter_schema = None;

        // Store the prepared statement
        let handle = PreparedStatementHandle {
            plan: logical_plan,
            parameter_schema,
            dataset_schema: Arc::new(dataset_schema.clone()),
            created_at: Timestamp::now(),
        };

        {
            let mut guard = self.prepared_statements.lock()
                .map_err(|_| Status::internal("Failed to acquire lock on prepared statements"))?;
            guard.insert(request_id, handle);

            // Update active prepared statements gauge
            metrics::gauge!("prepared_statements_active").set(guard.len() as f64);
        }

        // Serialize dataset schema to IPC format
        let options = IpcWriteOptions::default();
        let IpcMessage(dataset_schema_bytes) = SchemaAsIpc::new(&dataset_schema, &options)
            .try_into()
            .map_err(|e: ArrowError| Status::internal(format!("Failed to serialize schema: {}", e)))?;

        // Build response
        let result = ActionCreatePreparedStatementResult {
            prepared_statement_handle: Bytes::from(request_id.as_bytes().to_vec()),
            dataset_schema: dataset_schema_bytes,
            parameter_schema: Bytes::new(), // No parameters supported yet
        };

        // Record metrics
        let duration = Timestamp::now() - start;
        histogram!("do_action_create_prepared_statement_latency_ms").record(duration.get_milliseconds() as f64);

        #[cfg(feature = "observability")]
        {
            let ctx = self.execution.session_ctx();
            let req = ObservabilityRequestDetails {
                request_id: Some(request_id.to_string()),
                path: "/do_action/create_prepared_statement".to_string(),
                sql: Some(query.query),
                start_ms: start.as_millisecond(),
                duration_ms: duration.get_milliseconds(),
                rows: None,
                status: 0,
            };
            if let Err(e) = self.execution.observability().try_record_request(ctx, req).await {
                error!("Error recording request: {}", e);
            }
        }

        Ok(result)
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        counter!("requests", "endpoint" => "do_action_close_prepared_statement").increment(1);
        let start = Timestamp::now();

        let handle_bytes = query.prepared_statement_handle.to_vec();
        let request_id = Uuid::from_slice(&handle_bytes)
            .map_err(|e| Status::invalid_argument(format!("Invalid prepared statement handle: {}", e)))?;

        debug!("Closing prepared statement: {}", request_id);

        // Remove from storage
        {
            let mut guard = self.prepared_statements.lock()
                .map_err(|_| Status::internal("Failed to acquire lock on prepared statements"))?;

            if guard.remove(&request_id).is_none() {
                return Err(Status::not_found(format!("Prepared statement not found: {}", request_id)));
            }

            // Update active prepared statements gauge
            metrics::gauge!("prepared_statements_active").set(guard.len() as f64);
        }

        // Record metrics
        let duration = Timestamp::now() - start;
        histogram!("do_action_close_prepared_statement_latency_ms").record(duration.get_milliseconds() as f64);

        #[cfg(feature = "observability")]
        {
            let ctx = self.execution.session_ctx();
            let req = ObservabilityRequestDetails {
                request_id: Some(request_id.to_string()),
                path: "/do_action/close_prepared_statement".to_string(),
                sql: None,
                start_ms: start.as_millisecond(),
                duration_ms: duration.get_milliseconds(),
                rows: None,
                status: 0,
            };
            if let Err(e) = self.execution.observability().try_record_request(ctx, req).await {
                error!("Error recording request: {}", e);
            }
        }

        Ok(())
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        counter!("requests", "endpoint" => "get_flight_info_prepared_statement").increment(1);
        let start = Timestamp::now();

        // Extract the prepared statement handle
        let handle_bytes = query.prepared_statement_handle.to_vec();
        let handle_uuid = Uuid::from_slice(&handle_bytes).map_err(|e| {
            Status::invalid_argument(format!("Invalid prepared statement handle: {}", e))
        })?;

        debug!("Getting flight info for prepared statement: {}", handle_uuid);

        // Look up the prepared statement
        let prepared_stmt = {
            let guard = self.prepared_statements.lock()
                .map_err(|_| Status::internal("Failed to acquire lock on prepared statements"))?;

            guard.get(&handle_uuid)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("Prepared statement not found: {}", handle_uuid)))?
        };

        // Create a new request ID for this execution
        let request_id = Uuid::new_v4();

        // Create FlightInfo from the stored logical plan
        let res = self
            .create_flight_info_for_logical_plan(prepared_stmt.plan, request_id, request)
            .await;

        // Record observability
        let duration = Timestamp::now() - start;
        histogram!("get_flight_info_prepared_statement_latency_ms")
            .record(duration.get_milliseconds() as f64);

        #[cfg(feature = "observability")]
        {
            let ctx = self.execution.session_ctx();
            let req = ObservabilityRequestDetails {
                request_id: Some(request_id.to_string()),
                path: "/get_flight_info_prepared_statement".to_string(),
                sql: None, // Don't log SQL for prepared statements
                start_ms: start.as_millisecond(),
                duration_ms: duration.get_milliseconds(),
                rows: None,
                status: if res.is_ok() { 0 } else { tonic::Code::Internal as u16 },
            };
            if let Err(e) = self.execution.observability().try_record_request(ctx, req).await {
                error!("Error recording request: {}", e);
            }
        }

        res
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        counter!("requests", "endpoint" => "do_get_prepared_statement").increment(1);
        let start = Timestamp::now();

        // Extract the request ID from the ticket
        let request_id =
            try_request_id_from_request(request).map_err(|e| Status::internal(e.to_string()))?;

        debug!("do_get_prepared_statement for request_id: {}", &request_id);

        // The request_id in the ticket should correspond to a logical plan in the requests HashMap
        // that was created by get_flight_info_prepared_statement
        let res = self
            .do_get_statement_handler(request_id.clone(), TicketStatementQuery {
                statement_handle: query.prepared_statement_handle,
            })
            .await;

        // Record observability
        let duration = Timestamp::now() - start;
        histogram!("do_get_prepared_statement_latency_ms")
            .record(duration.get_milliseconds() as f64);

        #[cfg(feature = "observability")]
        {
            let ctx = self.execution.session_ctx();
            let req = ObservabilityRequestDetails {
                request_id: Some(request_id),
                path: "/do_get_prepared_statement".to_string(),
                sql: None,
                start_ms: start.as_millisecond(),
                duration_ms: duration.get_milliseconds(),
                rows: None,
                status: if res.is_ok() { 0 } else { tonic::Code::Internal as u16 },
            };
            if let Err(e) = self.execution.observability().try_record_request(ctx, req).await {
                error!("Error recording request: {}", e);
            }
        }

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

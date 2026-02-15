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

use std::sync::Arc;

use arrow_flight::{
    decode::FlightRecordBatchStream,
    sql::{
        client::FlightSqlServiceClient, CommandGetDbSchemas, CommandGetTables,
        CommandGetXdbcTypeInfo,
    },
    FlightInfo,
};
#[cfg(feature = "flightsql")]
use base64::engine::{general_purpose::STANDARD, Engine as _};
use datafusion::{
    error::{DataFusionError, Result as DFResult},
    physical_plan::stream::RecordBatchStreamAdapter,
    sql::parser::DFParser,
};
use log::{debug, error, info, warn};

#[cfg(feature = "flightsql")]
use crate::config::BasicAuth;
use color_eyre::eyre::{self, Result};
use std::collections::HashMap;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tonic::{transport::Channel, IntoRequest};

use crate::{
    config::FlightSQLConfig,
    flightsql_benchmarks::FlightSQLBenchmarkStats,
    local_benchmarks::{BenchmarkMode, BenchmarkProgressReporter},
    ExecOptions, ExecResult,
};

pub type FlightSQLClient = Arc<Mutex<Option<FlightSqlServiceClient<Channel>>>>;

#[derive(Clone, Debug, Default)]
pub struct FlightSQLContext {
    config: FlightSQLConfig,
    client: FlightSQLClient,
}

impl FlightSQLContext {
    pub fn new(config: FlightSQLConfig) -> Self {
        Self {
            config,
            client: Arc::new(Mutex::new(None)),
        }
    }

    pub fn client(&self) -> &FlightSQLClient {
        &self.client
    }

    // TODO - Make this part of `new` method
    /// Create FlightSQL client from users FlightSQL config
    pub async fn create_client(
        &self,
        cli_host: Option<String>,
        cli_headers: Option<HashMap<String, String>>,
    ) -> Result<()> {
        let final_url = cli_host.unwrap_or(self.config.connection_url.clone());
        let url = Box::leak(final_url.into_boxed_str());
        info!("Connecting to FlightSQL host: {}", url);
        let channel = Channel::from_static(url).connect().await;
        match channel {
            Ok(c) => {
                let mut client = FlightSqlServiceClient::new(c);
                // TODO: Look into setting both bearer and basic, which requires comma separating
                // them in the same `Authorization` header key (https://www.rfc-editor.org/rfc/rfc7230#section-3.2.2)
                //
                // Although that is for HTTP/1.1 and GRPC uses HTTP/2 - so maybe it has changed.
                // To be tested later with the Tower auth layers to see what they support.
                // TODO - Do we need this feature block?
                #[cfg(feature = "flightsql")]
                {
                    if let Some(token) = &self.config.auth.bearer_token {
                        client.set_token(token.to_string());
                    } else if let Some(BasicAuth { username, password }) =
                        &self.config.auth.basic_auth
                    {
                        let encoded_basic = STANDARD.encode(format!("{username}:{password}"));
                        client.set_header("Authorization", format!("Basic {encoded_basic}"))
                    }

                    let mut headers = self.config.headers.clone();
                    if let Some(cli) = cli_headers {
                        headers.extend(cli);
                    }
                    for (name, value) in headers {
                        client.set_header(name, value);
                    }
                }
                let mut guard = self.client.lock().await;
                *guard = Some(client);
                Ok(())
            }
            Err(e) => Err(eyre::eyre!(
                "Error creating channel for FlightSQL client: {:?}",
                e
            )),
        }
    }

    pub async fn benchmark_query(
        &self,
        query: &str,
        cli_iterations: Option<usize>,
        concurrent: bool,
        progress_reporter: Option<Arc<dyn BenchmarkProgressReporter>>,
    ) -> Result<FlightSQLBenchmarkStats> {
        let iterations = cli_iterations.unwrap_or(self.config.benchmark_iterations);
        let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
        let statements = DFParser::parse_sql_with_dialect(query, &dialect)?;

        if statements.len() != 1 {
            return Err(eyre::eyre!("Only a single statement can be benchmarked"));
        }

        // Check that client exists
        {
            let guard = self.client.lock().await;
            if guard.is_none() {
                return Err(eyre::eyre!("No FlightSQL client configured"));
            }
        }

        let concurrency = if concurrent {
            std::cmp::min(iterations, num_cpus::get())
        } else {
            1
        };
        let mode = if concurrent {
            BenchmarkMode::Concurrent(concurrency)
        } else {
            BenchmarkMode::Serial
        };

        info!(
            "Benchmarking FlightSQL query with {} iterations (concurrency: {})",
            iterations, concurrency
        );

        let mut rows_returned = Vec::with_capacity(iterations);
        let mut get_flight_info_durations = Vec::with_capacity(iterations);
        let mut ttfb_durations = Vec::with_capacity(iterations);
        let mut do_get_durations = Vec::with_capacity(iterations);
        let mut total_durations = Vec::with_capacity(iterations);

        if !concurrent {
            // Serial execution
            let mut guard = self.client.lock().await;
            if let Some(ref mut client) = *guard {
                for i in 0..iterations {
                    let (rows, gfi_dur, ttfb_dur, dg_dur, total_dur) =
                        Self::benchmark_single_iteration(client, query).await?;
                    rows_returned.push(rows);
                    get_flight_info_durations.push(gfi_dur);
                    ttfb_durations.push(ttfb_dur);
                    do_get_durations.push(dg_dur);
                    total_durations.push(total_dur);

                    if let Some(ref reporter) = progress_reporter {
                        reporter.on_iteration_complete(i + 1, iterations, total_dur);
                    }
                }
            }
        } else {
            // Concurrent execution - spawn tasks that share the client
            let mut completed = 0;

            while completed < iterations {
                let batch_size = std::cmp::min(concurrency, iterations - completed);
                let mut join_set = tokio::task::JoinSet::new();

                for _ in 0..batch_size {
                    let client = Arc::clone(&self.client);
                    let query_str = query.to_string();

                    join_set.spawn(async move {
                        let mut guard = client.lock().await;
                        if let Some(ref mut c) = *guard {
                            Self::benchmark_single_iteration(c, &query_str).await
                        } else {
                            Err(eyre::eyre!("No FlightSQL client configured"))
                        }
                    });
                }

                while let Some(result) = join_set.join_next().await {
                    let (rows, gfi_dur, ttfb_dur, dg_dur, total_dur) = result??;
                    rows_returned.push(rows);
                    get_flight_info_durations.push(gfi_dur);
                    ttfb_durations.push(ttfb_dur);
                    do_get_durations.push(dg_dur);
                    total_durations.push(total_dur);

                    completed += 1;
                    if let Some(ref reporter) = progress_reporter {
                        reporter.on_iteration_complete(completed, iterations, total_dur);
                    }
                }
            }
        }

        if let Some(ref reporter) = progress_reporter {
            reporter.finish();
        }

        Ok(FlightSQLBenchmarkStats::new(
            query.to_string(),
            rows_returned,
            mode,
            get_flight_info_durations,
            ttfb_durations,
            do_get_durations,
            total_durations,
        ))
    }

    async fn benchmark_single_iteration(
        client: &mut FlightSqlServiceClient<Channel>,
        query: &str,
    ) -> Result<(
        usize,
        std::time::Duration,
        std::time::Duration,
        std::time::Duration,
        std::time::Duration,
    )> {
        let mut rows = 0;
        let start = std::time::Instant::now();
        let flight_info = client.execute(query.to_string(), None).await?;

        if flight_info.endpoint.len() > 1 {
            warn!("More than one endpoint: Benchmark results will not be reliable");
        }

        let get_flight_info_duration = start.elapsed();
        let mut ttfb_duration = std::time::Duration::from_secs(0);
        let mut do_get_duration = std::time::Duration::from_secs(0);

        for endpoint in flight_info.endpoint {
            if let Some(ticket) = &endpoint.ticket {
                match client.do_get(ticket.clone().into_request()).await {
                    Ok(ref mut s) => {
                        let mut batch_count = 0;
                        while let Some(b) = s.next().await {
                            rows += b?.num_rows();
                            if batch_count == 0 {
                                ttfb_duration = start.elapsed() - get_flight_info_duration;
                            }
                            batch_count += 1;
                        }
                        do_get_duration = start.elapsed() - get_flight_info_duration;
                    }
                    Err(e) => {
                        error!("Error getting Flight stream: {:?}", e);
                        return Err(e.into());
                    }
                }
            }
        }

        let total_duration = start.elapsed();
        Ok((
            rows,
            get_flight_info_duration,
            ttfb_duration,
            do_get_duration,
            total_duration,
        ))
    }

    pub async fn execute_sql_with_opts(
        &self,
        sql: &str,
        _opts: ExecOptions,
    ) -> DFResult<ExecResult> {
        if let Some(ref mut client) = *self.client.lock().await {
            let flight_info = client.execute(sql.to_string(), None).await?;
            if flight_info.endpoint.len() != 1 {
                return Err(DataFusionError::External("More than one endpoint".into()));
            }
            let endpoint = &flight_info.endpoint[0];
            if let Some(ticket) = &endpoint.ticket {
                match client.do_get(ticket.clone().into_request()).await {
                    Ok(stream) => {
                        let mut peekable = stream.peekable();
                        if let Some(Ok(first)) = peekable.peek().await {
                            let schema = first.schema();
                            let mapped = peekable
                                .map(|r| r.map_err(|e| DataFusionError::External(e.into())));
                            let adapter = RecordBatchStreamAdapter::new(schema, mapped);
                            Ok(ExecResult::RecordBatchStream(Box::pin(adapter)))
                        } else {
                            Err(DataFusionError::External("No first result".into()))
                        }
                    }
                    Err(e) => Err(DataFusionError::External(
                        format!("Call to do_get failed: {}", e).into(),
                    )),
                }
            } else {
                Err(DataFusionError::External("Missing ticket".into()))
            }
        } else {
            Err(DataFusionError::External("Missing client".into()))
        }
    }

    pub async fn get_catalogs_flight_info(&self) -> DFResult<FlightInfo> {
        let client = Arc::clone(&self.client);
        let mut guard = client.lock().await;
        if let Some(client) = guard.as_mut() {
            client
                .get_catalogs()
                .await
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        } else {
            Err(DataFusionError::External(
                "No FlightSQL client configured.  Add one in `~/.config/dft/config.toml`".into(),
            ))
        }
    }

    pub async fn get_db_schemas_flight_info(
        &self,
        catalog: Option<String>,
        schema_filter_pattern: Option<String>,
    ) -> DFResult<FlightInfo> {
        let client = Arc::clone(&self.client);
        let mut guard = client.lock().await;
        if let Some(client) = guard.as_mut() {
            let cmd = CommandGetDbSchemas {
                catalog,
                db_schema_filter_pattern: schema_filter_pattern,
            };
            client
                .get_db_schemas(cmd)
                .await
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        } else {
            Err(DataFusionError::External(
                "No FlightSQL client configured.  Add one in `~/.config/dft/config.toml`".into(),
            ))
        }
    }

    pub async fn get_tables_flight_info(
        &self,
        catalog: Option<String>,
        schema_filter_pattern: Option<String>,
        table_name_filter_pattern: Option<String>,
        table_types: Vec<String>,
        include_schema: bool,
    ) -> DFResult<FlightInfo> {
        let client = Arc::clone(&self.client);
        let mut guard = client.lock().await;
        if let Some(client) = guard.as_mut() {
            let cmd = CommandGetTables {
                catalog,
                db_schema_filter_pattern: schema_filter_pattern,
                table_name_filter_pattern,
                table_types,
                include_schema,
            };
            client
                .get_tables(cmd)
                .await
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        } else {
            Err(DataFusionError::External(
                "No FlightSQL client configured.  Add one in `~/.config/dft/config.toml`".into(),
            ))
        }
    }

    pub async fn get_table_types_flight_info(&self) -> DFResult<FlightInfo> {
        let client = Arc::clone(&self.client);
        let mut guard = client.lock().await;
        if let Some(client) = guard.as_mut() {
            client
                .get_table_types()
                .await
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        } else {
            Err(DataFusionError::External(
                "No FlightSQL client configured.  Add one in `~/.config/dft/config.toml`".into(),
            ))
        }
    }

    pub async fn get_sql_info_flight_info(&self, info: Option<Vec<u32>>) -> DFResult<FlightInfo> {
        let client = Arc::clone(&self.client);
        let mut guard = client.lock().await;
        if let Some(client) = guard.as_mut() {
            use arrow_flight::sql::SqlInfo;
            // Convert u32 IDs to SqlInfo enum variants if needed
            let sql_info_list: Vec<SqlInfo> = info
                .unwrap_or_default()
                .into_iter()
                .filter_map(|id| SqlInfo::try_from(id as i32).ok())
                .collect();
            client
                .get_sql_info(sql_info_list)
                .await
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        } else {
            Err(DataFusionError::External(
                "No FlightSQL client configured.  Add one in `~/.config/dft/config.toml`".into(),
            ))
        }
    }

    pub async fn get_xdbc_type_info_flight_info(
        &self,
        data_type: Option<i32>,
    ) -> DFResult<FlightInfo> {
        let client = Arc::clone(&self.client);
        let mut guard = client.lock().await;
        if let Some(client) = guard.as_mut() {
            let cmd = CommandGetXdbcTypeInfo { data_type };
            client
                .get_xdbc_type_info(cmd)
                .await
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        } else {
            Err(DataFusionError::External(
                "No FlightSQL client configured.  Add one in `~/.config/dft/config.toml`".into(),
            ))
        }
    }

    pub async fn do_get(&self, flight_info: FlightInfo) -> DFResult<Vec<FlightRecordBatchStream>> {
        let client = Arc::clone(&self.client);
        let mut guard = client.lock().await;
        if let Some(client) = guard.as_mut() {
            let mut streams = Vec::new();
            for endpoint in flight_info.endpoint {
                if let Some(ticket) = endpoint.ticket {
                    let stream = client
                        .do_get(ticket.into_request())
                        .await
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    streams.push(stream);
                } else {
                    debug!("No ticket for endpoint: {endpoint}");
                }
            }
            Ok(streams)
        } else {
            Err(DataFusionError::External(
                "No FlightSQL client configured. Add one in `~/.config/dft/config.toml`".into(),
            ))
        }
    }

    /// Get raw metrics batch without reconstruction (for --analyze-raw)
    pub async fn analyze_query_raw(
        &self,
        query: &str,
    ) -> Result<(String, datafusion::arrow::array::RecordBatch)> {
        self.fetch_analyze_batches(query).await
    }

    /// Reconstruct ExecutionStats from metrics (for --analyze)
    pub async fn analyze_query(&self, query: &str) -> Result<crate::stats::ExecutionStats> {
        let (query_str, metrics_batch) = self.fetch_analyze_batches(query).await?;

        // Reconstruct ExecutionStats from metrics table
        let stats = crate::stats::ExecutionStats::from_metrics_table(metrics_batch, query_str)?;

        Ok(stats)
    }

    /// Shared logic to fetch analyze batch and query from server
    async fn fetch_analyze_batches(
        &self,
        query: &str,
    ) -> Result<(String, datafusion::arrow::array::RecordBatch)> {
        use arrow_flight::utils::flight_data_to_batches;
        use arrow_flight::{Action, FlightData};

        // Validate that query contains only a single statement
        let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
        let statements = DFParser::parse_sql_with_dialect(query, &dialect)?;
        if statements.len() != 1 {
            return Err(eyre::eyre!("Only a single SQL statement can be analyzed"));
        }

        // 1. Create Action with type "analyze_query" and SQL in body
        let action = Action {
            r#type: "analyze_query".to_string(),
            body: query.as_bytes().to_vec().into(),
        };

        // 2. Call do_action on the FlightSQL service
        let mut client = self.client.lock().await;
        let client = client
            .as_mut()
            .ok_or_else(|| eyre::eyre!("No FlightSQL client configured"))?;

        let mut stream = client
            .do_action(action.into_request())
            .await
            .map_err(|e| eyre::eyre!("do_action failed: {}", e))?;

        // 3. Collect all Result messages from stream
        let mut result_messages = Vec::new();
        while let Some(result) = stream.next().await {
            let result = result.map_err(|e| eyre::eyre!("Stream error: {}", e))?;
            result_messages.push(result);
        }

        // 4. Decode each Result message to FlightData and extract query from metadata
        let mut all_flight_data = Vec::new();
        let mut sql_query = None;

        for result in result_messages {
            // Deserialize the FlightData from the Result.body bytes using prost
            let flight_data = <FlightData as prost::Message>::decode(result.body.as_ref())
                .map_err(|e| eyre::eyre!("Failed to decode FlightData: {}", e))?;

            // Extract SQL from schema message (first message) metadata
            if sql_query.is_none() && !flight_data.app_metadata.is_empty() {
                sql_query = Some(
                    String::from_utf8(flight_data.app_metadata.to_vec())
                        .map_err(|e| eyre::eyre!("Invalid UTF-8 in metadata: {}", e))?,
                );
            }

            all_flight_data.push(flight_data);
        }

        // 5. Validate we got the SQL query in metadata
        let query_str =
            sql_query.ok_or_else(|| eyre::eyre!("SQL query not found in response metadata"))?;

        // 6. Decode metrics batch
        // batches_to_flight_data creates [schema, data] for the batch
        if all_flight_data.len() < 2 {
            return Err(eyre::eyre!(
                "Invalid analyze response: expected at least 2 FlightData messages (schema + data), got {}",
                all_flight_data.len()
            ));
        }

        let metrics_batches = flight_data_to_batches(&all_flight_data)
            .map_err(|e| eyre::eyre!("Failed to decode metrics batch: {}", e))?;

        if metrics_batches.is_empty() {
            return Err(eyre::eyre!("No metrics batch found in response"));
        }

        Ok((query_str, metrics_batches[0].clone()))
    }
}

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

//! [`AppExecution`]: Handles executing queries for the TUI application.

use crate::tui::{AppEvent, ExecutionError, ExecutionResultsBatch};
use crate::execution::ExecutionContext;
use color_eyre::eyre::Result;
#[allow(unused_imports)] // No idea why this is being picked up as unused when I use it twice.
use datafusion::arrow::error::ArrowError;
use datafusion::execution::context::SessionContext;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::execute_stream;
use futures::StreamExt;
use log::{error, info};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
#[cfg(feature = "flightsql")]
use tokio_stream::StreamMap;

#[cfg(feature = "flightsql")]
use {
    crate::config::FlightSQLConfig, arrow_flight::decode::FlightRecordBatchStream,
    arrow_flight::sql::client::FlightSqlServiceClient, arrow_flight::Ticket,
    tonic::transport::Channel, tonic::IntoRequest,
};

/// Handles executing queries for the TUI application, formatting results
/// and sending them to the UI.
///
/// TODO: I think we want to store the SQL associated with a stream
pub struct AppExecution {
    inner: Arc<ExecutionContext>,
    result_stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
    #[cfg(feature = "flightsql")]
    flightsql_result_stream: Arc<Mutex<Option<StreamMap<String, FlightRecordBatchStream>>>>,
}

impl AppExecution {
    /// Create a new instance of [`AppExecution`].
    pub fn new(inner: Arc<ExecutionContext>) -> Self {
        Self {
            inner,
            result_stream: Arc::new(Mutex::new(None)),
            #[cfg(feature = "flightsql")]
            flightsql_result_stream: Arc::new(Mutex::new(None)),
        }
    }

    pub fn session_ctx(&self) -> &SessionContext {
        self.inner.session_ctx()
    }

    pub async fn set_result_stream(&self, stream: SendableRecordBatchStream) {
        let mut s = self.result_stream.lock().await;
        *s = Some(stream)
    }

    #[cfg(feature = "flightsql")]
    pub async fn set_flightsql_result_stream(
        &self,
        ticket: Ticket,
        stream: FlightRecordBatchStream,
    ) {
        let mut s = self.flightsql_result_stream.lock().await;
        if let Some(ref mut streams) = *s {
            streams.insert(ticket.to_string(), stream);
        } else {
            let mut map: StreamMap<String, FlightRecordBatchStream> = StreamMap::new();
            let t = ticket.to_string();
            info!("Adding {t} to FlightSQL streams");
            map.insert(ticket.to_string(), stream);
            *s = Some(map);
        }
    }

    #[cfg(feature = "flightsql")]
    pub async fn reset_flightsql_result_stream(&self) {
        let mut s = self.flightsql_result_stream.lock().await;
        *s = None;
    }

    /// Run the sequence of SQL queries, sending the results as
    /// [`AppEvent::ExecutionResultsBatch`].
    /// All queries except the last one will have their results discarded.
    ///
    /// Error handling: If an error occurs while executing a query, the error is
    /// logged and execution continues
    pub async fn run_sqls(
        self: Arc<Self>,
        sqls: Vec<String>,
        sender: UnboundedSender<AppEvent>,
    ) -> Result<()> {
        // We need to filter out empty strings to correctly determine the last query for displaying
        // results.
        info!("Running sqls: {:?}", sqls);
        let non_empty_sqls: Vec<String> = sqls.into_iter().filter(|s| !s.is_empty()).collect();
        info!("Non empty SQLs: {:?}", non_empty_sqls);
        let statement_count = non_empty_sqls.len();
        for (i, sql) in non_empty_sqls.into_iter().enumerate() {
            info!("Running query {}", i);
            let _sender = sender.clone();
            let start = std::time::Instant::now();
            if i == statement_count - 1 {
                info!("Executing last query and display results");
                sender.send(AppEvent::NewExecution)?;
                match self.inner.create_physical_plan(&sql).await {
                    Ok(plan) => match execute_stream(plan, self.inner.session_ctx().task_ctx()) {
                        Ok(stream) => {
                            self.set_result_stream(stream).await;
                            let mut stream = self.result_stream.lock().await;
                            if let Some(s) = stream.as_mut() {
                                if let Some(b) = s.next().await {
                                    match b {
                                        Ok(b) => {
                                            let duration = start.elapsed();
                                            let results = ExecutionResultsBatch {
                                                query: sql.to_string(),
                                                batch: b,
                                                duration,
                                            };
                                            sender.send(AppEvent::ExecutionResultsNextBatch(
                                                results,
                                            ))?;
                                        }
                                        Err(e) => {
                                            error!("Error getting RecordBatch: {:?}", e);
                                        }
                                    }
                                }
                            }
                        }
                        Err(stream_err) => {
                            error!("Error executing stream: {:?}", stream_err);
                            let elapsed = start.elapsed();
                            let e = ExecutionError {
                                query: sql.to_string(),
                                error: stream_err.to_string(),
                                duration: elapsed,
                            };
                            sender.send(AppEvent::ExecutionResultsError(e))?;
                        }
                    },
                    Err(plan_err) => {
                        error!("Error creating physical plan: {:?}", plan_err);
                        let elapsed = start.elapsed();
                        let e = ExecutionError {
                            query: sql.to_string(),
                            error: plan_err.to_string(),
                            duration: elapsed,
                        };
                        sender.send(AppEvent::ExecutionResultsError(e))?;
                    }
                }
            } else {
                match self.inner.execute_sql_and_discard_results(&sql).await {
                    Ok(_) => {}
                    Err(e) => {
                        // We only log failed queries, we don't want to stop the execution of the
                        // remaining queries. Perhaps there should be a configuration option for
                        // this though in case the user wants to stop execution on the first error.
                        error!("Error executing {sql}: {:?}", e);
                    }
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "flightsql")]
    pub async fn run_flightsqls(
        self: Arc<Self>,
        sqls: Vec<String>,
        sender: UnboundedSender<AppEvent>,
    ) -> Result<()> {
        info!("Running sqls: {:?}", sqls);
        self.reset_flightsql_result_stream().await;
        let non_empty_sqls: Vec<String> = sqls.into_iter().filter(|s| !s.is_empty()).collect();
        let statement_count = non_empty_sqls.len();
        for (i, sql) in non_empty_sqls.into_iter().enumerate() {
            let _sender = sender.clone();
            if i == statement_count - 1 {
                info!("Executing last query and display results");
                sender.send(AppEvent::FlightSQLNewExecution)?;
                if let Some(ref mut client) = *self.flightsql_client().lock().await {
                    let start = std::time::Instant::now();
                    match client.execute(sql.clone(), None).await {
                        Ok(flight_info) => {
                            for endpoint in flight_info.endpoint {
                                if let Some(ticket) = endpoint.ticket {
                                    match client.do_get(ticket.clone().into_request()).await {
                                        Ok(stream) => {
                                            self.set_flightsql_result_stream(ticket, stream).await;
                                            if let Some(streams) =
                                                self.flightsql_result_stream.lock().await.as_mut()
                                            {
                                                match streams.next().await {
                                                    Some((ticket, Ok(batch))) => {
                                                        info!("Received batch for {ticket}");
                                                        let duration = start.elapsed();
                                                        let results = ExecutionResultsBatch {
                                                            batch,
                                                            duration,
                                                            query: sql.to_string(),
                                                        };
                                                        sender.send(
                                                            AppEvent::FlightSQLExecutionResultsNextBatch(
                                                                results,
                                                            ),
                                                        )?;
                                                    }
                                                    Some((ticket, Err(e))) => {
                                                        error!(
                                                            "Error executing stream for ticket {ticket}: {:?}",
                                                            e
                                                        );
                                                        let elapsed = start.elapsed();
                                                        let e = ExecutionError {
                                                            query: sql.to_string(),
                                                            error: e.to_string(),
                                                            duration: elapsed,
                                                        };
                                                        sender.send(
                                                            AppEvent::FlightSQLExecutionResultsError(e),
                                                        )?;
                                                    }
                                                    None => {}
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!("Error creating result stream: {:?}", e);
                                            if let ArrowError::IpcError(ipc_err) = &e {
                                                if ipc_err.contains("error trying to connect") {
                                                    let e = ExecutionError {
                                                        query: sql.to_string(),
                                                        error: "Error connecting to Flight server"
                                                            .to_string(),
                                                        duration: std::time::Duration::from_secs(0),
                                                    };
                                                    sender.send(
                                                        AppEvent::FlightSQLExecutionResultsError(e),
                                                    )?;
                                                    return Ok(());
                                                }
                                            }

                                            let elapsed = start.elapsed();
                                            let e = ExecutionError {
                                                query: sql.to_string(),
                                                error: e.to_string(),
                                                duration: elapsed,
                                            };
                                            sender.send(
                                                AppEvent::FlightSQLExecutionResultsError(e),
                                            )?;
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error getting flight info: {:?}", e);
                            if let ArrowError::IpcError(ipc_err) = &e {
                                if ipc_err.contains("error trying to connect") {
                                    let e = ExecutionError {
                                        query: sql.to_string(),
                                        error: "Error connecting to Flight server".to_string(),
                                        duration: std::time::Duration::from_secs(0),
                                    };
                                    sender.send(AppEvent::FlightSQLExecutionResultsError(e))?;
                                    return Ok(());
                                }
                            }
                            let elapsed = start.elapsed();
                            let e = ExecutionError {
                                query: sql.to_string(),
                                error: e.to_string(),
                                duration: elapsed,
                            };
                            sender.send(AppEvent::FlightSQLExecutionResultsError(e))?;
                        }
                    }
                } else {
                    let e = ExecutionError {
                        query: sql.to_string(),
                        error: "No FlightSQL client".to_string(),
                        duration: std::time::Duration::from_secs(0),
                    };
                    sender.send(AppEvent::FlightSQLExecutionResultsError(e))?;
                }
            }
        }

        Ok(())
    }

    pub async fn next_batch(&self, sql: String, sender: UnboundedSender<AppEvent>) {
        let mut stream = self.result_stream.lock().await;
        if let Some(s) = stream.as_mut() {
            let start = std::time::Instant::now();
            if let Some(b) = s.next().await {
                match b {
                    Ok(b) => {
                        let duration = start.elapsed();
                        let results = ExecutionResultsBatch {
                            query: sql,
                            batch: b,
                            duration,
                        };
                        let _ = sender.send(AppEvent::ExecutionResultsNextBatch(results));
                    }
                    Err(e) => {
                        error!("Error getting RecordBatch: {:?}", e);
                    }
                }
            }
        }
    }

    #[cfg(feature = "flightsql")]
    pub async fn create_flightsql_client(&self, config: FlightSQLConfig) -> Result<()> {
        self.inner.create_flightsql_client(config).await
    }

    #[cfg(feature = "flightsql")]
    pub fn flightsql_client(&self) -> &Mutex<Option<FlightSqlServiceClient<Channel>>> {
        self.inner.flightsql_client()
    }

    pub fn load_ddl(&self) -> Option<String> {
        self.inner.load_ddl()
    }

    pub fn save_ddl(&self, ddl: String) {
        self.inner.save_ddl(ddl)
    }
}

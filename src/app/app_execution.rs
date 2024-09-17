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

use crate::app::state::tabs::sql::Query;
use crate::app::{AppEvent, ExecutionError};
use crate::execution::ExecutionContext;
use color_eyre::eyre::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use futures::StreamExt;
use log::{error, info};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;

/// Handles executing queries for the TUI application, formatting results
/// and sending them to the UI.
pub(crate) struct AppExecution {
    inner: Arc<ExecutionContext>,
    result_stream: Arc<Mutex<Option<SendableRecordBatchStream>>>,
}

impl AppExecution {
    /// Create a new instance of [`AppExecution`].
    pub fn new(inner: Arc<ExecutionContext>) -> Self {
        Self {
            inner,
            result_stream: Arc::new(Mutex::new(None)),
        }
    }

    pub fn session_ctx(&self) -> &SessionContext {
        self.inner.session_ctx()
    }

    pub async fn set_result_stream(&self, stream: SendableRecordBatchStream) {
        let mut s = self.result_stream.lock().await;
        *s = Some(stream)
    }

    /// Run the sequence of SQL queries, sending the results as [`AppEvent::QueryResult`] via the sender.
    ///
    /// All queries except the last one will have their results discarded.
    ///
    /// Error handling: If an error occurs while executing a query, the error is
    /// logged and execution continues
    pub async fn run_sqls(&self, sqls: Vec<&str>, sender: UnboundedSender<AppEvent>) -> Result<()> {
        // We need to filter out empty strings to correctly determine the last query for displaying
        // results.
        info!("Running sqls: {:?}", sqls);
        let non_empty_sqls: Vec<&str> = sqls.into_iter().filter(|s| !s.is_empty()).collect();
        info!("Non empty SQLs: {:?}", non_empty_sqls);
        let statement_count = non_empty_sqls.len();
        for (i, sql) in non_empty_sqls.into_iter().enumerate() {
            info!("Running query {}", i);
            let _sender = sender.clone();
            let mut query =
                Query::new(sql.to_string(), None, None, None, Duration::default(), None);
            let start = std::time::Instant::now();
            if i == statement_count - 1 {
                info!("Executing last query and display results");
                sender.send(AppEvent::NewExecution)?;
                match self.inner.create_physical_plan(sql).await {
                    Ok(plan) => match execute_stream(plan, self.inner.session_ctx().task_ctx()) {
                        Ok(stream) => {
                            self.set_result_stream(stream).await;
                            let mut stream = self.result_stream.lock().await;
                            if let Some(s) = stream.as_mut() {
                                if let Some(b) = s.next().await {
                                    match b {
                                        Ok(b) => {
                                            sender.send(AppEvent::ExecutionResultsNextPage(b))?;
                                        }
                                        Err(e) => {
                                            error!("Error getting RecordBatch: {:?}", e);
                                        }
                                    }
                                }
                            }
                        }
                        Err(stream_err) => {
                            error!("Error creating physical plan: {:?}", stream_err);
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
                // match self.inner.execute_sql(sql).await {
                //     Ok(stream) => {
                //         // self.set_result_stream(stream).await;
                //         // let mut stream = self.result_stream.lock().await;
                //         // if let Some(s) = stream.as_mut() {
                //         //     if let Some(b) = s.next().await {
                //         //         match b {
                //         //             Ok(b) => {
                //         //                 sender.send(AppEvent::ExecutionResultsNextPage(b))?;
                //         //             }
                //         //             Err(e) => {
                //         //                 error!("Error getting RecordBatch: {:?}", e);
                //         //             }
                //         //         }
                //         //     }
                //         // }
                //         // let mut batches = Vec::new();
                //         // while let Some(maybe_batch) = stream.next().await {
                //         //     match maybe_batch {
                //         //         Ok(batch) => {
                //         //             batches.push(batch);
                //         //         }
                //         //         Err(e) => {
                //         //             let elapsed = start.elapsed();
                //         //             query.set_error(Some(e.to_string()));
                //         //             query.set_execution_time(elapsed);
                //         //             break;
                //         //         }
                //         //     }
                //         // }
                //         // let elapsed = start.elapsed();
                //         // let rows: usize = batches.iter().map(|r| r.num_rows()).sum();
                //         // query.set_results(Some(batches));
                //         // query.set_num_rows(Some(rows));
                //         // query.set_execution_time(elapsed);
                //     }
                //     Err(e) => {
                //         error!("Error creating dataframe: {:?}", e);
                //         let elapsed = start.elapsed();
                //         let e = ExecutionError {
                //             query: sql.to_string(),
                //             error: e.to_string(),
                //             duration: elapsed,
                //         };
                //         sender.send(AppEvent::ExecutionResultsError(e))?;
                //     }
                // }
            } else {
                match self.inner.execute_sql_and_discard_results(sql).await {
                    Ok(_) => {
                        let elapsed = start.elapsed();
                        query.set_execution_time(elapsed);
                    }
                    Err(e) => {
                        // We only log failed queries, we don't want to stop the execution of the
                        // remaining queries. Perhaps there should be a configuration option for
                        // this though in case the user wants to stop execution on the first error.
                        error!("Error executing {sql}: {:?}", e);
                    }
                }
            }
            _sender.send(AppEvent::QueryResult(query))?; // Send the query result to the UI
        }
        Ok(())
    }
}

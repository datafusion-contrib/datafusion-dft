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
use crate::app::AppEvent;
use crate::execution::ExecutionContext;
use color_eyre::eyre::Result;
use futures::StreamExt;
use log::{error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

/// Handles executing queries for the TUI application, formatting results
/// and sending them to the UI.
pub(crate) struct AppExecution {
    inner: Arc<ExecutionContext>,
}

impl AppExecution {
    /// Create a new instance of [`AppExecution`].
    pub fn new(inner: Arc<ExecutionContext>) -> Self {
        Self { inner }
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
                match self.inner.execute_sql(sql).await {
                    Ok(mut stream) => {
                        let mut batches = Vec::new();
                        while let Some(maybe_batch) = stream.next().await {
                            match maybe_batch {
                                Ok(batch) => {
                                    batches.push(batch);
                                }
                                Err(e) => {
                                    let elapsed = start.elapsed();
                                    query.set_error(Some(e.to_string()));
                                    query.set_execution_time(elapsed);
                                    break;
                                }
                            }
                        }
                        let elapsed = start.elapsed();
                        let rows: usize = batches.iter().map(|r| r.num_rows()).sum();
                        query.set_results(Some(batches));
                        query.set_num_rows(Some(rows));
                        query.set_execution_time(elapsed);
                    }
                    Err(e) => {
                        error!("Error creating dataframe: {:?}", e);
                        let elapsed = start.elapsed();
                        query.set_error(Some(e.to_string()));
                        query.set_execution_time(elapsed);
                    }
                }
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

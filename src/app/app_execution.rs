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
use datafusion::arrow::array::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
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
#[derive(Debug)]
pub(crate) struct AppExecution {
    inner: Arc<ExecutionContext>,
    results: Arc<Mutex<Option<PaginatingRecordBatchStream>>>,
}

impl AppExecution {
    /// Create a new instance of [`AppExecution`].
    pub fn new(inner: Arc<ExecutionContext>) -> Self {
        Self {
            inner,
            results: Arc::new(Mutex::new(None)),
        }
    }

    pub fn session_ctx(&self) -> &SessionContext {
        self.inner.session_ctx()
    }

    pub fn results(&self) -> Arc<Mutex<Option<PaginatingRecordBatchStream>>> {
        Arc::clone(&self.results)
    }

    async fn set_results(&self, results: PaginatingRecordBatchStream) {
        let mut r = self.results.lock().await;
        *r = Some(results);
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
                    Ok(stream) => {
                        let mut paginating_stream = PaginatingRecordBatchStream::new(stream);
                        paginating_stream.next_batch().await?;
                        self.set_results(paginating_stream).await;

                        // let mut batches = Vec::new();
                        // while let Some(maybe_batch) = stream.next().await {
                        //     match maybe_batch {
                        //         Ok(batch) => {
                        //             batches.push(batch);
                        //         }
                        //         Err(e) => {
                        //             let elapsed = start.elapsed();
                        //             query.set_error(Some(e.to_string()));
                        //             query.set_execution_time(elapsed);
                        //             break;
                        //         }
                        //     }
                        // }
                        // let elapsed = start.elapsed();
                        // let rows: usize = batches.iter().map(|r| r.num_rows()).sum();
                        // query.set_results(Some(batches));
                        // query.set_num_rows(Some(rows));
                        // query.set_execution_time(elapsed);
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

/// A stream of [`RecordBatch`]es that can be paginated for display in the TUI.
pub struct PaginatingRecordBatchStream {
    // currently executing stream
    inner: SendableRecordBatchStream,
    // any batches that have been buffered so far
    batches: Vec<RecordBatch>,
    // current batch being shown
    current_batch: Option<usize>,
}

impl PaginatingRecordBatchStream {
    pub fn new(inner: Pin<Box<dyn RecordBatchStream + Send>>) -> Self {
        Self {
            inner,
            batches: Vec::new(),
            current_batch: None,
        }
    }

    /// Return the batch at the current index
    pub fn current_batch(&self) -> Option<&RecordBatch> {
        if let Some(idx) = self.current_batch {
            self.batches.get(idx)
        } else {
            None
        }
    }

    /// Return the next batch
    /// TBD on logic for handling the end
    pub async fn next_batch(&mut self) -> Result<Option<&RecordBatch>> {
        if let Some(b) = self.inner.next().await {
            match b {
                Ok(batch) => {
                    self.batches.push(batch);
                    self.current_batch = Some(self.batches.len() - 1);
                    Ok(self.current_batch())
                }
                Err(e) => Err(e.into()),
            }
        } else {
            Ok(None)
        }
    }

    /// Return the previous batch
    /// TBD on logic for handling the beginning
    pub fn previous_batch(&mut self) -> Option<&RecordBatch> {
        if let Some(idx) = self.current_batch {
            if idx > 0 {
                self.current_batch = Some(idx - 1);
            }
        }
        self.current_batch()
    }
}

impl Debug for PaginatingRecordBatchStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaginatingRecordBatchStream")
            .field("batches", &self.batches)
            .field("current_batch", &self.current_batch)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::PaginatingRecordBatchStream;
    use datafusion::{
        arrow::array::{ArrayRef, Int32Array, RecordBatch},
        common::Result,
        physical_plan::stream::RecordBatchStreamAdapter,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_paginating_record_batch_stream() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![1, 1]));

        let record_batch1 = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let record_batch2 = RecordBatch::try_from_iter(vec![("b", b)]).unwrap();

        let schema = record_batch1.schema();
        let batches: Vec<Result<RecordBatch>> =
            vec![Ok(record_batch1.clone()), Ok(record_batch2.clone())];
        let stream = futures::stream::iter(batches);
        let sendable_stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));

        let mut paginating_stream = PaginatingRecordBatchStream::new(sendable_stream);

        assert_eq!(paginating_stream.current_batch(), None);
        assert_eq!(
            paginating_stream.next_batch().await.unwrap(),
            Some(&record_batch1)
        );
        assert_eq!(
            paginating_stream.next_batch().await.unwrap(),
            Some(&record_batch2)
        );
        assert_eq!(paginating_stream.next_batch().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_paginating_record_batch_stream_previous() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![1, 1]));

        let record_batch1 = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let record_batch2 = RecordBatch::try_from_iter(vec![("b", b)]).unwrap();

        let schema = record_batch1.schema();
        let batches: Vec<Result<RecordBatch>> =
            vec![Ok(record_batch1.clone()), Ok(record_batch2.clone())];
        let stream = futures::stream::iter(batches);
        let sendable_stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));

        let mut paginating_stream = PaginatingRecordBatchStream::new(sendable_stream);

        assert_eq!(paginating_stream.current_batch(), None);
        assert_eq!(
            paginating_stream.next_batch().await.unwrap(),
            Some(&record_batch1)
        );
        assert_eq!(
            paginating_stream.next_batch().await.unwrap(),
            Some(&record_batch2)
        );
        assert_eq!(paginating_stream.next_batch().await.unwrap(), None);
        assert_eq!(paginating_stream.current_batch(), Some(&record_batch2));
        assert_eq!(paginating_stream.previous_batch(), Some(&record_batch1));
        assert_eq!(paginating_stream.previous_batch(), Some(&record_batch1));
    }

    #[tokio::test]
    async fn test_paginating_record_batch_stream_one_error() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let record_batch1 = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();

        let schema = record_batch1.schema();
        let batches: Vec<Result<RecordBatch>> = vec![Err(
            datafusion::error::DataFusionError::Execution("Error creating dataframe".to_string()),
        )];
        let stream = futures::stream::iter(batches);
        let sendable_stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));

        let mut paginating_stream = PaginatingRecordBatchStream::new(sendable_stream);

        assert_eq!(paginating_stream.current_batch(), None);

        let res = paginating_stream.next_batch().await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_paginating_record_batch_stream_successful_then_error() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));

        let record_batch1 = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();

        let schema = record_batch1.schema();
        let batches: Vec<Result<RecordBatch>> = vec![
            Ok(record_batch1.clone()),
            Err(datafusion::error::DataFusionError::Execution(
                "Error creating dataframe".to_string(),
            )),
        ];
        let stream = futures::stream::iter(batches);
        let sendable_stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));

        let mut paginating_stream = PaginatingRecordBatchStream::new(sendable_stream);

        assert_eq!(paginating_stream.current_batch(), None);
        assert_eq!(
            paginating_stream.next_batch().await.unwrap(),
            Some(&record_batch1)
        );
        let res = paginating_stream.next_batch().await;
        assert!(res.is_err());
        assert_eq!(paginating_stream.next_batch().await.unwrap(), None);
        assert_eq!(paginating_stream.current_batch(), Some(&record_batch1));
    }
}

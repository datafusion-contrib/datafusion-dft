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

//! `websocket` table function: streams messages received over a WebSocket
//! connection as rows.
//!
//! ```sql
//! SELECT * FROM websocket('wss://abc.com', 'msg1', 'msg2') LIMIT 10
//! ```
//!
//! The first argument is the connection URL (`ws://` or `wss://`) and any
//! remaining arguments are messages sent after the connection is established
//! (for example, subscription messages). The source is unbounded: without a
//! `LIMIT` the query streams until the server closes the connection.

use std::{
    sync::{Arc, Once},
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{RecordBatch, StringArray, TimestampMillisecondArray},
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    },
    catalog::{Session, TableFunctionImpl, TableProvider},
    common::{internal_err, plan_err, project_schema, Column, DataFusionError, Result},
    datasource::TableType,
    execution::SendableRecordBatchStream,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchReceiverStream,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
    prelude::Expr,
    scalar::ScalarValue,
};
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Message;

use crate::executor::io::io_runtime_handle;

/// rustls requires a process-level default `CryptoProvider` for TLS
/// (`wss://`) connections. Other dependencies in the workspace enable both
/// the `ring` and `aws-lc-rs` provider features, so rustls cannot select one
/// automatically and panics unless a default is installed explicitly.
fn ensure_crypto_provider() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // Ignore the error, which means a default was already installed
        // elsewhere in the process
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

fn websocket_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "received_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("message", DataType::Utf8, false),
    ]))
}

/// Table function that connects to a WebSocket endpoint and streams received
/// messages as rows
#[derive(Debug, Default)]
pub struct WebSocketFunc {}

impl TableFunctionImpl for WebSocketFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if exprs.is_empty() {
            return plan_err!("websocket requires at least one argument, the connection URL");
        }
        let url = expr_to_string(&exprs[0], "URL (first argument)")?;
        if !(url.starts_with("ws://") || url.starts_with("wss://")) {
            return plan_err!("websocket URL must start with 'ws://' or 'wss://', got '{url}'");
        }
        let messages = exprs[1..]
            .iter()
            .map(|e| expr_to_string(e, "message argument"))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(WebSocketTable::new(url, messages)))
    }
}

fn expr_to_string(expr: &Expr, what: &str) -> Result<String> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(s))
            | ScalarValue::Utf8View(Some(s))
            | ScalarValue::LargeUtf8(Some(s)),
            _,
        ) => Ok(s.clone()),
        // Double quoted strings are parsed as columns
        Expr::Column(Column { name, .. }) => Ok(name.clone()),
        _ => plan_err!("websocket {what} must be a string literal"),
    }
}

/// [`TableProvider`] backed by a WebSocket connection
#[derive(Debug)]
pub struct WebSocketTable {
    url: String,
    messages: Vec<String>,
    schema: SchemaRef,
}

impl WebSocketTable {
    pub fn new(url: String, messages: Vec<String>) -> Self {
        Self {
            url,
            messages,
            schema: websocket_schema(),
        }
    }
}

#[async_trait]
impl TableProvider for WebSocketTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = WebSocketExec::try_new(
            self.url.clone(),
            self.messages.clone(),
            Arc::clone(&self.schema),
            projection.cloned(),
            limit,
        )?;
        Ok(Arc::new(exec))
    }
}

/// Execution plan that connects to a WebSocket endpoint on `execute` and
/// yields one record batch per received message. No connection is made during
/// planning (e.g. `EXPLAIN`).
#[derive(Debug)]
struct WebSocketExec {
    url: String,
    messages: Vec<String>,
    /// Schema representing the data before projection
    schema: SchemaRef,
    /// Optional projection
    projection: Option<Vec<usize>>,
    /// Schema representing the data after the optional projection is applied
    projected_schema: SchemaRef,
    limit: Option<usize>,
    cache: Arc<PlanProperties>,
}

impl WebSocketExec {
    fn try_new(
        url: String,
        messages: Vec<String>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        let cache = Self::compute_properties(Arc::clone(&projected_schema));
        Ok(Self {
            url,
            messages,
            schema,
            projection,
            projected_schema,
            limit,
            cache,
        })
    }

    fn compute_properties(projected_schema: SchemaRef) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        ))
    }
}

impl DisplayAs for WebSocketExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "WebSocketExec: url={}, messages={}, limit={:?}",
            self.url,
            self.messages.len(),
            self.limit
        )
    }
}

impl ExecutionPlan for WebSocketExec {
    fn name(&self) -> &str {
        "WebSocketExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // This is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // WebSocketExec has no children
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in {self:?}")
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("WebSocketExec has a single partition, got {partition}");
        }
        let mut builder =
            RecordBatchReceiverStream::builder(Arc::clone(&self.projected_schema), 16);
        let tx = builder.tx();
        let task = read_websocket(
            self.url.clone(),
            self.messages.clone(),
            Arc::clone(&self.schema),
            self.projection.clone(),
            self.limit,
            tx,
        );
        // The socket task needs a tokio I/O driver. When running on the
        // dedicated (CPU) executor, which has no I/O driver, spawn on the IO
        // runtime registered for this thread instead.
        match io_runtime_handle() {
            Some(handle) => builder.spawn_on(task, &handle),
            None => builder.spawn(task),
        }
        Ok(builder.build())
    }
}

/// Connects to `url`, sends `messages`, then forwards each received message
/// as a single row record batch until the connection closes, an error occurs,
/// `limit` rows have been produced, or the consumer drops the stream.
async fn read_websocket(
    url: String,
    messages: Vec<String>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    tx: Sender<Result<RecordBatch>>,
) -> Result<()> {
    ensure_crypto_provider();
    let (mut ws, _resp) = match tokio_tungstenite::connect_async(&url).await {
        Ok(conn) => conn,
        Err(e) => {
            let _ = tx
                .send(Err(DataFusionError::External(
                    format!("websocket failed to connect to {url}: {e}").into(),
                )))
                .await;
            return Ok(());
        }
    };
    for msg in &messages {
        if let Err(e) = ws.send(Message::text(msg.clone())).await {
            let _ = tx
                .send(Err(DataFusionError::External(
                    format!("websocket failed to send message: {e}").into(),
                )))
                .await;
            return Ok(());
        }
    }
    let mut produced = 0usize;
    while let Some(msg) = ws.next().await {
        let text = match msg {
            Ok(Message::Text(t)) => t.to_string(),
            Ok(Message::Binary(b)) => String::from_utf8_lossy(&b).into_owned(),
            // Graceful end of stream
            Ok(Message::Close(_)) => break,
            // Tungstenite automatically queues Pong replies on read
            Ok(_) => continue,
            Err(e) => {
                let _ = tx
                    .send(Err(DataFusionError::External(
                        format!("websocket read error: {e}").into(),
                    )))
                    .await;
                break;
            }
        };
        let batch = message_to_batch(&schema, &projection, text)?;
        if tx.send(Ok(batch)).await.is_err() {
            // Consumer dropped the stream (e.g. LIMIT satisfied)
            break;
        }
        produced += 1;
        if limit.is_some_and(|l| produced >= l) {
            let _ = ws.close(None).await;
            break;
        }
    }
    Ok(())
}

fn message_to_batch(
    schema: &SchemaRef,
    projection: &Option<Vec<usize>>,
    message: String,
) -> Result<RecordBatch> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| DataFusionError::External(e.into()))?
        .as_millis() as i64;
    let received_at = TimestampMillisecondArray::from(vec![millis]);
    let message = StringArray::from(vec![message]);
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(received_at), Arc::new(message)],
    )?;
    match projection {
        Some(p) => Ok(batch.project(p)?),
        None => Ok(batch),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_call_requires_url() {
        let func = WebSocketFunc::default();
        let err = func.call(&[]).unwrap_err();
        assert!(err.to_string().contains("at least one argument"));
    }

    #[test]
    fn test_call_rejects_invalid_scheme() {
        let func = WebSocketFunc::default();
        let args = vec![Expr::Literal(
            ScalarValue::Utf8(Some("http://example.com".to_string())),
            None,
        )];
        let err = func.call(&args).unwrap_err();
        assert!(err.to_string().contains("'ws://' or 'wss://'"));
    }

    #[test]
    fn test_call_rejects_non_string_message() {
        let func = WebSocketFunc::default();
        let args = vec![
            Expr::Literal(
                ScalarValue::Utf8(Some("ws://example.com".to_string())),
                None,
            ),
            Expr::Literal(ScalarValue::Int64(Some(1)), None),
        ];
        let err = func.call(&args).unwrap_err();
        assert!(err.to_string().contains("must be a string literal"));
    }

    #[test]
    fn test_call_parses_url_and_messages() {
        let func = WebSocketFunc::default();
        let args = vec![
            Expr::Literal(
                ScalarValue::Utf8(Some("wss://example.com".to_string())),
                None,
            ),
            Expr::Literal(ScalarValue::Utf8(Some("subscribe".to_string())), None),
        ];
        let provider = func.call(&args).unwrap();
        assert_eq!(provider.schema(), websocket_schema());
    }

    #[test]
    fn test_exec_is_unbounded_single_partition() {
        let exec = WebSocketExec::try_new(
            "ws://example.com".to_string(),
            vec![],
            websocket_schema(),
            None,
            None,
        )
        .unwrap();
        assert!(matches!(
            exec.properties().boundedness,
            Boundedness::Unbounded {
                requires_infinite_memory: false
            }
        ));
        assert_eq!(exec.properties().partitioning.partition_count(), 1);
    }

    #[test]
    fn test_exec_projection() {
        let exec = WebSocketExec::try_new(
            "ws://example.com".to_string(),
            vec![],
            websocket_schema(),
            Some(vec![1]),
            None,
        )
        .unwrap();
        assert_eq!(exec.schema().field(0).name(), "message");
    }

    #[tokio::test]
    async fn test_explain_makes_no_connection() {
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_udtf("websocket", Arc::new(WebSocketFunc::default()));
        // Planning must not connect - localhost:1 is not listening so a
        // connection attempt would error
        let df = ctx
            .sql("EXPLAIN SELECT * FROM websocket('ws://localhost:1/x', 'subscribe')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert!(!batches.is_empty());
    }
}

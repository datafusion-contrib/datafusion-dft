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

//! `stdin` table function: streams data piped to the process on stdin.
//!
//! ```sql
//! SELECT * FROM stdin('csv')
//! ```
//!
//! The single argument is the format of the piped data: `csv`, `json`
//! (newline-delimited), `parquet`, or `arrow` (IPC file or stream).
//!
//! At plan time only the bytes needed for schema inference are read: a
//! bounded prefix for `csv` and `json`, and the schema message for `arrow`
//! (both IPC variants put it first; for the file format the magic header is
//! skipped and the trailing footer ignored). The remaining data is decoded
//! incrementally at execution time on a blocking task, with the
//! already-consumed prefix replayed ahead of the live stdin stream. Only
//! `parquet` requires its metadata footer before decoding, so that format is
//! fully buffered at plan time (but still decoded lazily at execution time).
//!
//! Stdin is consumable once: when execution has started reading past the
//! inference prefix, a second scan returns an error.

use std::io::{BufReader, Cursor, IsTerminal, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    arrow::{array::RecordBatch, csv, datatypes::SchemaRef, ipc, json},
    catalog::{Session, TableFunctionImpl, TableProvider},
    common::{
        exec_err, internal_err, plan_err, project_schema, tree_node::TreeNodeRecursion, Result,
    },
    datasource::TableType,
    execution::SendableRecordBatchStream,
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    physical_expr::{EquivalenceProperties, PhysicalExpr},
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchReceiverStream,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
    prelude::Expr,
    scalar::ScalarValue,
};
use parking_lot::Mutex;
use tokio::sync::mpsc::Sender;

/// Maximum number of bytes read at plan time to infer the schema for the
/// `csv` and `json` formats
const SCHEMA_INFERENCE_PREFIX_LIMIT: usize = 1024 * 1024;

/// Magic bytes at the start of an arrow IPC file (as opposed to an IPC
/// stream, which has no magic)
const ARROW_FILE_MAGIC: &[u8] = b"ARROW1";

#[derive(Clone, Copy, Debug, PartialEq)]
enum StdinFormat {
    Csv,
    Json,
    Parquet,
    Arrow,
}

impl StdinFormat {
    /// Canonical name used to detect conflicting `stdin(...)` calls
    fn canonical_name(&self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Parquet => "parquet",
            Self::Arrow => "arrow",
        }
    }
}

/// The result of the plan-time read of stdin: the inferred schema, the bytes
/// consumed to infer it (replayed ahead of the live stream at execution
/// time), and whether those bytes are the entire input.
#[derive(Debug)]
struct StdinCapture {
    format: StdinFormat,
    schema: SchemaRef,
    prefix: Bytes,
    /// True if `prefix` holds the entire input, in which case scans never
    /// touch stdin again and the table can be scanned repeatedly
    eof: bool,
}

/// Stdin can only be consumed once per process, so the plan-time capture is
/// done a single time and cached. Holds `Err` if stdin was a terminal or
/// reading/inference failed, so every call site reports the same error.
static CAPTURE: OnceLock<std::result::Result<Arc<StdinCapture>, String>> = OnceLock::new();

/// Set when execution starts streaming stdin past the captured prefix
static STDIN_CONSUMED: AtomicBool = AtomicBool::new(false);

/// Wraps a reader and records every byte read from it, so bytes consumed by
/// (buffered) schema inference can be replayed at execution time
struct TrackingReader<R> {
    inner: R,
    captured: Arc<Mutex<Vec<u8>>>,
}

impl<R: Read> Read for TrackingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.captured.lock().extend_from_slice(&buf[..n]);
        Ok(n)
    }
}

/// Reads up to `limit` bytes from `reader`. Returns the bytes and whether the
/// end of input was reached.
fn read_prefix(reader: &mut impl Read, limit: usize) -> std::io::Result<(Vec<u8>, bool)> {
    let mut buf = Vec::new();
    reader.take(limit as u64).read_to_end(&mut buf)?;
    let eof = buf.len() < limit;
    Ok((buf, eof))
}

/// The portion of `prefix` that is safe to use for line-based (csv / json)
/// schema inference: everything when the whole input was read, otherwise up
/// to and including the last complete line
fn inference_sample(prefix: &[u8], eof: bool) -> Result<&[u8]> {
    if eof {
        return Ok(prefix);
    }
    match prefix.iter().rposition(|b| *b == b'\n') {
        Some(pos) => Ok(&prefix[..=pos]),
        None => exec_err!(
            "could not infer schema from stdin: no complete line in the first {SCHEMA_INFERENCE_PREFIX_LIMIT} bytes"
        ),
    }
}

fn capture_stdin(format_name: &str) -> std::result::Result<Arc<StdinCapture>, String> {
    let stdin = std::io::stdin();
    if stdin.is_terminal() {
        return Err("stdin is a terminal; pipe data into the process to use stdin()".to_string());
    }
    let mut lock = stdin.lock();
    let capture = match format_name {
        "csv" | "json" | "ndjson" => {
            let (prefix, eof) =
                read_prefix(&mut lock, SCHEMA_INFERENCE_PREFIX_LIMIT).map_err(|e| e.to_string())?;
            if prefix.is_empty() {
                return Err("no data received on stdin".to_string());
            }
            let sample = inference_sample(&prefix, eof).map_err(|e| e.to_string())?;
            let (format, schema) = if format_name == "csv" {
                let (schema, _) = csv::reader::Format::default()
                    .with_header(true)
                    .infer_schema(Cursor::new(sample), None)
                    .map_err(|e| e.to_string())?;
                (StdinFormat::Csv, schema)
            } else {
                let (schema, _) = json::reader::infer_json_schema(Cursor::new(sample), None)
                    .map_err(|e| e.to_string())?;
                (StdinFormat::Json, schema)
            };
            StdinCapture {
                format,
                schema: Arc::new(schema),
                prefix: Bytes::from(prefix),
                eof,
            }
        }
        "parquet" => {
            // Parquet metadata is in the footer so the full input must be
            // buffered. Only the schema is decoded here; record batches are
            // decoded at execution time.
            let mut buf = Vec::new();
            lock.read_to_end(&mut buf).map_err(|e| e.to_string())?;
            if buf.is_empty() {
                return Err("no data received on stdin".to_string());
            }
            let bytes = Bytes::from(buf);
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
                .map_err(|e| e.to_string())?;
            let schema = Arc::clone(builder.schema());
            StdinCapture {
                format: StdinFormat::Parquet,
                schema,
                prefix: bytes,
                eof: true,
            }
        }
        "arrow" | "ipc" => {
            // The IPC file format is the stream format wrapped with a magic
            // header (plus padding) and a trailing footer. Both variants are
            // streamed with `StreamReader`: for a file the magic and padding
            // are skipped so the schema message is next, and the trailing
            // footer is never read because the reader stops at the
            // end-of-stream marker.
            let (magic, _) =
                read_prefix(&mut lock, ARROW_FILE_MAGIC.len()).map_err(|e| e.to_string())?;
            if magic.is_empty() {
                return Err("no data received on stdin".to_string());
            }
            let head = if magic == ARROW_FILE_MAGIC {
                // Discard the 2 bytes padding the magic to 8 bytes, then any
                // zero words aligning the first message (e.g. arrow-rs pads
                // the body start to 64 bytes). A message never starts with a
                // zero word: that is the end-of-stream marker.
                read_prefix(&mut lock, 2).map_err(|e| e.to_string())?;
                loop {
                    let (word, _) = read_prefix(&mut lock, 4).map_err(|e| e.to_string())?;
                    if word.len() < 4 {
                        return Err(
                            "arrow IPC file from stdin ended before the schema message".to_string()
                        );
                    }
                    if word != [0u8; 4] {
                        break word;
                    }
                }
            } else {
                // Not a file: the bytes are the start of the stream format
                magic
            };
            let captured = Arc::new(Mutex::new(Vec::new()));
            let tracking = TrackingReader {
                inner: Cursor::new(head).chain(lock),
                captured: Arc::clone(&captured),
            };
            let reader =
                ipc::reader::StreamReader::try_new(tracking, None).map_err(|e| e.to_string())?;
            let schema = reader.schema();
            drop(reader);
            let prefix = std::mem::take(&mut *captured.lock());
            StdinCapture {
                format: StdinFormat::Arrow,
                schema,
                prefix: Bytes::from(prefix),
                eof: false,
            }
        }
        other => {
            return Err(format!(
            "unsupported stdin() format '{other}'; expected 'csv', 'json', 'parquet', or 'arrow'"
        ))
        }
    };
    Ok(Arc::new(capture))
}

/// Table function that exposes data piped to the process on stdin as a table.
/// The required argument is the data format: `csv`, `json`, `parquet`, or
/// `arrow`.
#[derive(Debug, Default)]
pub struct StdinFunc {}

impl TableFunctionImpl for StdinFunc {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Utf8(Some(format)), _)) = args.first() else {
            return plan_err!(
                "stdin() requires a single string argument with the data format: 'csv', 'json', 'parquet', or 'arrow'"
            );
        };
        let format_name = format.to_ascii_lowercase();
        let capture = match CAPTURE.get_or_init(|| capture_stdin(&format_name)) {
            Ok(capture) => Arc::clone(capture),
            Err(e) => return exec_err!("{e}"),
        };
        // The capture is created once per process, so later calls must use
        // the same format
        let canonical = match format_name.as_str() {
            "ndjson" => "json",
            "ipc" => "arrow",
            other => other,
        };
        if canonical != capture.format.canonical_name() {
            return plan_err!(
                "stdin() was already opened with format '{}' and cannot be reopened with format '{canonical}'",
                capture.format.canonical_name()
            );
        }
        Ok(Arc::new(StdinTable { capture }))
    }
}

/// [`TableProvider`] backed by data piped to the process on stdin
#[derive(Debug)]
struct StdinTable {
    capture: Arc<StdinCapture>,
}

#[async_trait]
impl TableProvider for StdinTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.capture.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = StdinExec::try_new(Arc::clone(&self.capture), projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

/// Execution plan that decodes the captured stdin prefix followed by the live
/// stdin stream. Stdin is only read on `execute`, not during planning (e.g.
/// `EXPLAIN`).
#[derive(Debug)]
struct StdinExec {
    capture: Arc<StdinCapture>,
    /// Optional projection
    projection: Option<Vec<usize>>,
    /// Schema after the optional projection is applied
    projected_schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl StdinExec {
    fn try_new(capture: Arc<StdinCapture>, projection: Option<Vec<usize>>) -> Result<Self> {
        let projected_schema = project_schema(&capture.schema, projection.as_ref())?;
        let cache = Self::compute_properties(Arc::clone(&projected_schema));
        Ok(Self {
            capture,
            projection,
            projected_schema,
            cache,
        })
    }

    fn compute_properties(projected_schema: SchemaRef) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl DisplayAs for StdinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "StdinExec: format={}",
            self.capture.format.canonical_name()
        )
    }
}

impl ExecutionPlan for StdinExec {
    fn name(&self) -> &str {
        "StdinExec"
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
        // StdinExec has no children
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
            return internal_err!("StdinExec has a single partition, got {partition}");
        }
        // When the capture holds the entire input the stream is replayable;
        // otherwise executing reads (and consumes) the rest of stdin
        if !self.capture.eof && STDIN_CONSUMED.swap(true, Ordering::SeqCst) {
            return exec_err!(
                "stdin has already been consumed by a previous scan and cannot be scanned again"
            );
        }
        let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&self.projected_schema), 2);
        let tx = builder.tx();
        let capture = Arc::clone(&self.capture);
        let projection = self.projection.clone();
        builder.spawn_blocking(move || read_stdin_batches(capture, projection, tx));
        Ok(builder.build())
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // Leaf node with no expressions.
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Runs on a blocking task: decodes record batches from the captured prefix
/// chained with the live stdin stream and forwards them to the stream, until
/// the input ends, an error occurs, or the consumer drops the stream
fn read_stdin_batches(
    capture: Arc<StdinCapture>,
    projection: Option<Vec<usize>>,
    tx: Sender<Result<RecordBatch>>,
) -> Result<()> {
    // The lock is acquired on this blocking thread because `StdinLock` is not
    // `Send`. When the prefix holds the entire input stdin is already at EOF,
    // so chaining it is a no-op.
    let chained = Cursor::new(capture.prefix.clone()).chain(std::io::stdin().lock());
    let batches: Box<dyn Iterator<Item = Result<RecordBatch>>> = match capture.format {
        StdinFormat::Csv => {
            let reader = csv::ReaderBuilder::new(Arc::clone(&capture.schema))
                .with_header(true)
                .build(chained)?;
            Box::new(reader.map(|r| r.map_err(Into::into)))
        }
        StdinFormat::Json => {
            let reader = json::ReaderBuilder::new(Arc::clone(&capture.schema))
                .build(BufReader::new(chained))?;
            Box::new(reader.map(|r| r.map_err(Into::into)))
        }
        StdinFormat::Parquet => {
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(capture.prefix.clone())?.build()?;
            Box::new(reader.map(|r| r.map_err(Into::into)))
        }
        StdinFormat::Arrow => {
            // For the file format the magic and padding were stripped at
            // capture time, so the prefix starts at the schema message for
            // both IPC variants
            let reader = ipc::reader::StreamReader::try_new(chained, None)?;
            Box::new(reader.map(|r| r.map_err(Into::into)))
        }
    };
    for batch in batches {
        let projected = match (batch, &projection) {
            (Ok(batch), Some(indices)) => batch.project(indices).map_err(Into::into),
            (batch, _) => batch,
        };
        let is_err = projected.is_err();
        if tx.blocking_send(projected).is_err() {
            // Receiver dropped (e.g. query canceled or limit reached)
            break;
        }
        if is_err {
            break;
        }
    }
    Ok(())
}

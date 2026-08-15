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

//! `capture` table function: streams live-captured packets from a network
//! interface as rows.
//!
//! ```sql
//! -- Stream until 100 packets have been captured
//! SELECT * FROM capture('en0') LIMIT 100;
//!
//! -- Apply a BPF filter
//! SELECT * FROM capture('en0', 'tcp port 443') LIMIT 10;
//!
//! -- Capture for 10 seconds; the stream terminates, so aggregations work
//! SELECT src_ip, count(*) FROM capture('en0', '', 10) GROUP BY src_ip;
//! ```
//!
//! Arguments: interface name, optional BPF filter expression, optional
//! capture duration in seconds. Without a duration the source is unbounded:
//! use a `LIMIT` or the query streams until cancelled.
//!
//! Live capture requires elevated privileges (see the error message emitted
//! when opening the device fails). No device is opened during planning
//! (e.g. `EXPLAIN`), only on execution.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    catalog::{Session, TableFunctionImpl, TableProvider},
    common::{internal_err, plan_err, project_schema, DataFusionError, Result},
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
use tokio::sync::mpsc::Sender;

use crate::{
    decode::decode_frame,
    expr_to_string,
    schema::{packet_schema, payload_projected, send_batch, send_error, PacketBatchBuilder},
};

/// Number of rows accumulated before a batch is emitted
const BATCH_SIZE: usize = 1024;
/// Partial batches are flushed at this interval so results appear promptly
/// on quiet interfaces. Also used as the libpcap read timeout so the read
/// loop can flush, and check for cancellation, without traffic.
const FLUSH_INTERVAL: Duration = Duration::from_millis(250);

/// Table function that captures packets from a network interface
#[derive(Debug, Default)]
pub struct CaptureFunc {}

impl TableFunctionImpl for CaptureFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let (interface, filter, duration) = parse_capture_args("capture", exprs)?;
        Ok(Arc::new(CaptureTable::new(interface, filter, duration)))
    }
}

/// Parses the arguments shared by `capture` and `capture_wide` (hence the
/// function name parameter): interface name, optional BPF filter, optional
/// duration in seconds
pub(crate) fn parse_capture_args(
    func: &str,
    exprs: &[Expr],
) -> Result<(String, String, Option<Duration>)> {
    if exprs.is_empty() || exprs.len() > 3 {
        return plan_err!(
            "{func} requires 1 to 3 arguments: interface name, optional BPF filter, optional duration in seconds"
        );
    }
    let interface = expr_to_string(&exprs[0], func, "interface (first argument)")?;
    let filter = exprs
        .get(1)
        .map(|e| expr_to_string(e, func, "BPF filter (second argument)"))
        .transpose()?
        .unwrap_or_default();
    let duration = exprs
        .get(2)
        .map(|e| expr_to_duration(func, e))
        .transpose()?;
    Ok((interface, filter, duration))
}

fn expr_to_duration(func: &str, expr: &Expr) -> Result<Duration> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(secs)), _) if *secs > 0 => {
            Ok(Duration::from_secs(*secs as u64))
        }
        _ => plan_err!("{func} duration (third argument) must be a positive integer of seconds"),
    }
}

/// [`TableProvider`] backed by a live capture on a network interface
#[derive(Debug)]
pub struct CaptureTable {
    interface: String,
    filter: String,
    duration: Option<Duration>,
    schema: SchemaRef,
}

impl CaptureTable {
    pub fn new(interface: String, filter: String, duration: Option<Duration>) -> Self {
        Self {
            interface,
            filter,
            duration,
            schema: packet_schema(),
        }
    }
}

#[async_trait]
impl TableProvider for CaptureTable {
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
        let exec = CaptureExec::try_new(
            self.interface.clone(),
            self.filter.clone(),
            self.duration,
            Arc::clone(&self.schema),
            projection.cloned(),
            limit,
        )?;
        Ok(Arc::new(exec))
    }
}

/// Execution plan that opens the capture device on `execute` and yields
/// record batches of decoded packets
#[derive(Debug)]
struct CaptureExec {
    interface: String,
    filter: String,
    duration: Option<Duration>,
    projection: Option<Vec<usize>>,
    /// Schema representing the data after the optional projection is applied
    projected_schema: SchemaRef,
    limit: Option<usize>,
    cache: Arc<PlanProperties>,
}

impl CaptureExec {
    fn try_new(
        interface: String,
        filter: String,
        duration: Option<Duration>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        // A capture with a duration terminates, which allows plans that
        // require bounded input (e.g. aggregations) to run against it
        let boundedness = if duration.is_some() {
            Boundedness::Bounded
        } else {
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            }
        };
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            boundedness,
        ));
        Ok(Self {
            interface,
            filter,
            duration,
            projection,
            projected_schema,
            limit,
            cache,
        })
    }
}

impl DisplayAs for CaptureExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "CaptureExec: interface={}, filter={}, duration={:?}, limit={:?}",
            self.interface, self.filter, self.duration, self.limit
        )
    }
}

impl ExecutionPlan for CaptureExec {
    fn name(&self) -> &str {
        "CaptureExec"
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
        // CaptureExec has no children
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
            return internal_err!("CaptureExec has a single partition, got {partition}");
        }
        let mut builder =
            RecordBatchReceiverStream::builder(Arc::clone(&self.projected_schema), 16);
        let tx = builder.tx();
        let interface = self.interface.clone();
        let filter = self.filter.clone();
        let duration = self.duration;
        let projection = self.projection.clone();
        let limit = self.limit;
        // libpcap reads are blocking C calls, so run the capture loop on a
        // blocking thread rather than an async task
        builder
            .spawn_blocking(move || read_live(interface, filter, duration, projection, limit, tx));
        Ok(builder.build())
    }
}

/// Captures packets from `interface` and sends decoded record batches until
/// the duration elapses, `limit` rows have been produced, or the consumer
/// drops the stream
fn read_live(
    interface: String,
    filter: String,
    duration: Option<Duration>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    tx: Sender<Result<RecordBatch>>,
) -> Result<()> {
    let inactive = match pcap::Capture::from_device(interface.as_str()) {
        Ok(inactive) => inactive,
        Err(e) => {
            send_error(&tx, open_error(&interface, &e));
            return Ok(());
        }
    };
    let capture = inactive
        .promisc(true)
        .snaplen(65535)
        .timeout(FLUSH_INTERVAL.as_millis() as i32)
        .immediate_mode(true)
        .open();
    let mut capture = match capture {
        Ok(capture) => capture,
        Err(e) => {
            send_error(&tx, open_error(&interface, &e));
            return Ok(());
        }
    };
    if !filter.is_empty() {
        if let Err(e) = capture.filter(&filter, true) {
            send_error(
                &tx,
                DataFusionError::External(
                    format!("capture failed to set BPF filter '{filter}': {e}").into(),
                ),
            );
            return Ok(());
        }
    }
    let link_type = capture.get_datalink().0 as u32;

    let mut builder = PacketBatchBuilder::new(payload_projected(&projection));
    let mut frame_number = 0u64;
    let mut produced = 0usize;
    let started = Instant::now();
    let mut last_flush = Instant::now();

    loop {
        if duration.is_some_and(|d| started.elapsed() >= d) {
            break;
        }
        match capture.next_packet() {
            Ok(packet) => {
                frame_number += 1;
                // tv_sec/tv_usec widths vary by platform (i64 on macOS,
                // where these casts are no-ops)
                #[allow(clippy::unnecessary_cast)]
                let ts_micros =
                    packet.header.ts.tv_sec as i64 * 1_000_000 + packet.header.ts.tv_usec as i64;
                let decoded = decode_frame(link_type, packet.data);
                builder.append(
                    ts_micros,
                    frame_number,
                    packet.header.len,
                    packet.header.caplen,
                    &decoded,
                );
                produced += 1;
                if limit.is_some_and(|l| produced >= l) {
                    break;
                }
            }
            // No packets within the read timeout; fall through to flush any
            // partial batch and re-check the duration
            Err(pcap::Error::TimeoutExpired) => {}
            Err(e) => {
                send_error(
                    &tx,
                    DataFusionError::External(
                        format!("capture read error on '{interface}': {e}").into(),
                    ),
                );
                break;
            }
        }
        if builder.len() >= BATCH_SIZE
            || (!builder.is_empty() && last_flush.elapsed() >= FLUSH_INTERVAL)
        {
            if !send_batch(&mut builder, &projection, &tx)? {
                // Consumer dropped the stream
                return Ok(());
            }
            last_flush = Instant::now();
        }
        // Even with nothing to flush, a closed channel means the consumer is
        // gone and capturing should stop
        if tx.is_closed() {
            return Ok(());
        }
    }

    send_batch(&mut builder, &projection, &tx)?;
    Ok(())
}

fn open_error(interface: &str, err: &pcap::Error) -> DataFusionError {
    let permission_denied = match err {
        pcap::Error::IoError(kind) => *kind == std::io::ErrorKind::PermissionDenied,
        pcap::Error::PcapError(msg) => {
            let msg = msg.to_lowercase();
            msg.contains("permission denied") || msg.contains("not permitted")
        }
        _ => false,
    };
    let hint = if permission_denied {
        " (live capture requires elevated privileges: run with sudo, grant the binary \
         cap_net_raw+cap_net_admin on Linux, or install ChmodBPF on macOS)"
    } else {
        ""
    };
    DataFusionError::External(
        format!("capture failed to open device '{interface}': {err}{hint}").into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_call_requires_interface() {
        let func = CaptureFunc::default();
        let err = func.call(&[]).unwrap_err();
        assert!(err.to_string().contains("1 to 3 arguments"));
    }

    #[test]
    fn test_call_rejects_too_many_arguments() {
        let func = CaptureFunc::default();
        let args = vec![Expr::Literal(ScalarValue::Utf8(Some("en0".to_string())), None); 4];
        let err = func.call(&args).unwrap_err();
        assert!(err.to_string().contains("1 to 3 arguments"));
    }

    #[test]
    fn test_call_rejects_non_integer_duration() {
        let func = CaptureFunc::default();
        let args = vec![
            Expr::Literal(ScalarValue::Utf8(Some("en0".to_string())), None),
            Expr::Literal(ScalarValue::Utf8(Some("tcp".to_string())), None),
            Expr::Literal(ScalarValue::Utf8(Some("ten".to_string())), None),
        ];
        let err = func.call(&args).unwrap_err();
        assert!(err.to_string().contains("positive integer"));
    }

    #[test]
    fn test_call_parses_all_arguments() {
        let func = CaptureFunc::default();
        let args = vec![
            Expr::Literal(ScalarValue::Utf8(Some("en0".to_string())), None),
            Expr::Literal(ScalarValue::Utf8(Some("tcp port 443".to_string())), None),
            Expr::Literal(ScalarValue::Int64(Some(10)), None),
        ];
        let provider = func.call(&args).unwrap();
        assert_eq!(provider.schema(), packet_schema());
    }

    #[tokio::test]
    async fn test_explain_does_not_open_device() {
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_udtf("capture", Arc::new(CaptureFunc::default()));
        // Planning must not open a capture device (which would fail without
        // elevated privileges, and this device does not exist anyway)
        let df = ctx
            .sql("EXPLAIN SELECT * FROM capture('definitely-not-a-device')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert!(!batches.is_empty());
    }
}

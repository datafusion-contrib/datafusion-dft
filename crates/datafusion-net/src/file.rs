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

//! `pcap` table function: reads a pcap or pcapng capture file as a table.
//!
//! ```sql
//! SELECT src_ip, dst_ip, protocol, length FROM pcap('capture.pcap') WHERE dst_port = 443
//! ```
//!
//! The single argument is a local filesystem path. The file is not opened
//! during planning (e.g. `EXPLAIN`), only on execution.

use std::{fs::File, sync::Arc};

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
};
use pcap_parser::{create_reader, Block, PcapBlockOwned, PcapError};
use tokio::sync::mpsc::Sender;

use crate::{
    decode::{decode_frame, link_type},
    expr_to_string,
    schema::{packet_schema, payload_projected, send_batch, send_error, PacketBatchBuilder},
};

/// Number of rows accumulated before a batch is emitted
const BATCH_SIZE: usize = 1024;
/// Read buffer capacity, must exceed the largest block in the file
const READER_CAPACITY: usize = 1 << 20;

/// Table function that reads a pcap/pcapng capture file
#[derive(Debug, Default)]
pub struct PcapFunc {}

impl TableFunctionImpl for PcapFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let path = parse_pcap_args("pcap", exprs)?;
        Ok(Arc::new(PcapTable::new(path)))
    }
}

/// Parses the arguments shared by `pcap` and `pcap_wide` (hence the function
/// name parameter): a single path to a pcap/pcapng file
pub(crate) fn parse_pcap_args(func: &str, exprs: &[Expr]) -> Result<String> {
    if exprs.len() != 1 {
        return plan_err!("{func} requires a single argument, the path to a pcap/pcapng file");
    }
    expr_to_string(&exprs[0], func, "path (first argument)")
}

/// [`TableProvider`] backed by a pcap/pcapng file
#[derive(Debug)]
pub struct PcapTable {
    path: String,
    schema: SchemaRef,
}

impl PcapTable {
    pub fn new(path: String) -> Self {
        Self {
            path,
            schema: packet_schema(),
        }
    }
}

#[async_trait]
impl TableProvider for PcapTable {
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
        let exec = PcapExec::try_new(
            self.path.clone(),
            Arc::clone(&self.schema),
            projection.cloned(),
            limit,
        )?;
        Ok(Arc::new(exec))
    }
}

/// Execution plan that reads and decodes a capture file on `execute`
#[derive(Debug)]
struct PcapExec {
    path: String,
    projection: Option<Vec<usize>>,
    /// Schema representing the data after the optional projection is applied
    projected_schema: SchemaRef,
    limit: Option<usize>,
    cache: Arc<PlanProperties>,
}

impl PcapExec {
    fn try_new(
        path: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            path,
            projection,
            projected_schema,
            limit,
            cache,
        })
    }
}

impl DisplayAs for PcapExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PcapExec: path={}, limit={:?}", self.path, self.limit)
    }
}

impl ExecutionPlan for PcapExec {
    fn name(&self) -> &str {
        "PcapExec"
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
        // PcapExec has no children
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
            return internal_err!("PcapExec has a single partition, got {partition}");
        }
        let mut builder =
            RecordBatchReceiverStream::builder(Arc::clone(&self.projected_schema), 16);
        let tx = builder.tx();
        let path = self.path.clone();
        let projection = self.projection.clone();
        let limit = self.limit;
        // Reading and decoding is blocking (file I/O + CPU), so run it on a
        // blocking thread rather than an async task
        builder.spawn_blocking(move || read_pcap_file(path, projection, limit, tx));
        Ok(builder.build())
    }
}

/// Tracks per-interface timestamp metadata from pcapng interface description
/// blocks
struct NgInterface {
    link_type: u32,
    /// Timestamp units per second
    ts_resolution: u64,
    /// Offset added to timestamps, in seconds
    ts_offset: i64,
}

/// A raw captured frame handed to [`for_each_frame`] callbacks
pub(crate) struct RawFrame<'a> {
    pub link_type: u32,
    pub ts_micros: i64,
    pub origlen: u32,
    pub caplen: u32,
    pub data: &'a [u8],
}

/// Whether [`for_each_frame`] should keep reading
pub(crate) enum FrameLoop {
    Continue,
    Stop,
}

/// Iterates the frames of the pcap/pcapng file at `path`, driving the format
/// state machine (section/interface blocks, per-interface link types and
/// timestamp resolutions) and calling `on_frame` with the 1-based frame
/// number and each captured frame. `func` names the calling table function
/// in error messages.
pub(crate) fn for_each_frame(
    func: &str,
    path: &str,
    mut on_frame: impl FnMut(u64, RawFrame<'_>) -> Result<FrameLoop>,
) -> Result<()> {
    let file = File::open(path).map_err(|e| {
        DataFusionError::External(format!("{func} failed to open '{path}': {e}").into())
    })?;
    let mut reader = create_reader(READER_CAPACITY, file).map_err(|e| {
        DataFusionError::External(
            format!("{func} failed to read '{path}' as a pcap/pcapng file: {e}").into(),
        )
    })?;

    let mut frame_number = 0u64;
    // Legacy pcap state
    let mut legacy_link_type = link_type::ETHERNET;
    let mut legacy_ts_nanos = false;
    // pcapng state
    let mut ng_interfaces: Vec<NgInterface> = Vec::new();

    loop {
        match reader.next() {
            Ok((offset, block)) => {
                let frame = match block {
                    PcapBlockOwned::LegacyHeader(header) => {
                        legacy_link_type = header.network.0 as u32;
                        legacy_ts_nanos = header.is_nanosecond_precision();
                        None
                    }
                    PcapBlockOwned::Legacy(b) => {
                        let sub_micros = if legacy_ts_nanos {
                            (b.ts_usec / 1000) as i64
                        } else {
                            b.ts_usec as i64
                        };
                        let ts_micros = b.ts_sec as i64 * 1_000_000 + sub_micros;
                        Some((legacy_link_type, ts_micros, b.origlen, b.caplen, b.data))
                    }
                    PcapBlockOwned::NG(Block::SectionHeader(_)) => {
                        ng_interfaces.clear();
                        None
                    }
                    PcapBlockOwned::NG(Block::InterfaceDescription(ref idb)) => {
                        ng_interfaces.push(NgInterface {
                            link_type: idb.linktype.0 as u32,
                            ts_resolution: idb.ts_resolution().unwrap_or(1_000_000),
                            ts_offset: idb.ts_offset(),
                        });
                        None
                    }
                    PcapBlockOwned::NG(Block::EnhancedPacket(ref epb)) => {
                        match ng_interfaces.get(epb.if_id as usize) {
                            Some(interface) => {
                                let ts_units = ((epb.ts_high as u64) << 32) | epb.ts_low as u64;
                                // u128 to avoid overflow: nanosecond
                                // resolutions exceed u64 range when scaled
                                let ts_micros = (ts_units as u128 * 1_000_000
                                    / interface.ts_resolution as u128)
                                    as i64
                                    + interface.ts_offset * 1_000_000;
                                Some((
                                    interface.link_type,
                                    ts_micros,
                                    epb.origlen,
                                    epb.caplen,
                                    epb.data,
                                ))
                            }
                            None => None,
                        }
                    }
                    PcapBlockOwned::NG(Block::SimplePacket(ref spb)) => {
                        let link_type = ng_interfaces
                            .first()
                            .map(|i| i.link_type)
                            .unwrap_or(link_type::ETHERNET);
                        // Simple packet blocks carry no timestamp
                        Some((link_type, 0, spb.origlen, spb.data.len() as u32, spb.data))
                    }
                    // Name resolution, statistics, and other metadata blocks
                    PcapBlockOwned::NG(_) => None,
                };

                if let Some((link_type, ts_micros, origlen, caplen, data)) = frame {
                    frame_number += 1;
                    let raw = RawFrame {
                        link_type,
                        ts_micros,
                        origlen,
                        caplen,
                        data,
                    };
                    if let FrameLoop::Stop = on_frame(frame_number, raw)? {
                        return Ok(());
                    }
                }
                reader.consume(offset);
            }
            Err(PcapError::Eof) => return Ok(()),
            Err(PcapError::Incomplete(_)) => {
                reader.refill().map_err(|e| {
                    DataFusionError::External(format!("{func} failed to read '{path}': {e}").into())
                })?;
            }
            Err(e) => {
                return Err(DataFusionError::External(
                    format!("{func} failed to read '{path}': {e}").into(),
                ))
            }
        }
    }
}

/// Reads `path`, decoding each frame and sending record batches of
/// `BATCH_SIZE` rows until the file ends, `limit` rows have been produced, or
/// the consumer drops the stream
fn read_pcap_file(
    path: String,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    tx: Sender<Result<RecordBatch>>,
) -> Result<()> {
    let mut builder = PacketBatchBuilder::new(payload_projected(&projection));
    let mut produced = 0usize;
    let result = for_each_frame("pcap", &path, |frame_number, frame| {
        let decoded = decode_frame(frame.link_type, frame.data);
        builder.append(
            frame.ts_micros,
            frame_number,
            frame.origlen,
            frame.caplen,
            &decoded,
        );
        produced += 1;
        if limit.is_some_and(|l| produced >= l) {
            return Ok(FrameLoop::Stop);
        }
        if builder.len() >= BATCH_SIZE && !send_batch(&mut builder, &projection, &tx)? {
            // Consumer dropped the stream
            return Ok(FrameLoop::Stop);
        }
        Ok(FrameLoop::Continue)
    });
    match result {
        Ok(()) => {
            // Flush the final partial batch; if the consumer already dropped
            // the stream the send is a no-op
            send_batch(&mut builder, &projection, &tx)?;
        }
        Err(e) => send_error(&tx, e),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::scalar::ScalarValue;

    #[test]
    fn test_call_requires_single_path() {
        let func = PcapFunc::default();
        let err = func.call(&[]).unwrap_err();
        assert!(err.to_string().contains("single argument"));
    }

    #[test]
    fn test_call_rejects_non_string_path() {
        let func = PcapFunc::default();
        let args = vec![Expr::Literal(ScalarValue::Int64(Some(1)), None)];
        let err = func.call(&args).unwrap_err();
        assert!(err.to_string().contains("must be a string literal"));
    }

    #[tokio::test]
    async fn test_explain_does_not_open_file() {
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_udtf("pcap", Arc::new(PcapFunc::default()));
        // Planning must not touch the filesystem - this path does not exist
        let df = ctx
            .sql("EXPLAIN SELECT * FROM pcap('/does/not/exist.pcap')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert!(!batches.is_empty());
    }
}

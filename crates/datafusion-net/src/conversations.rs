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

//! `tcp_conversations` table function: flow-level TCP analytics over a
//! capture file, similar to wireshark's "Statistics → Conversations" or
//! `tshark -z conv,tcp`. One row per TCP connection:
//!
//! ```sql
//! SELECT dst_ip, dst_port, handshake_rtt_ms, retransmissions_fwd, state
//! FROM tcp_conversations('capture.pcap')
//! ORDER BY handshake_rtt_ms DESC;
//! ```
//!
//! Direction is from the connection initiator's perspective: `src_*` is the
//! endpoint that sent the first SYN (or, when the capture starts
//! mid-connection, the first endpoint seen sending), `fwd` counts
//! initiator → responder traffic and `rev` the reverse. The handshake RTT is
//! the time from the initiator's SYN to its handshake-completing ACK
//! (wireshark's initial RTT); it is NULL when the capture does not contain
//! the full handshake. Retransmissions are counted per direction with the
//! standard heuristic: a segment that consumes sequence space entirely below
//! the direction's highest seen sequence number.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            Float64Builder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder, UInt16Builder,
            UInt64Builder,
        },
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    },
    catalog::{Session, TableFunctionImpl, TableProvider},
    common::{internal_err, project_schema, Result},
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
use tokio::sync::mpsc::Sender;

use crate::{
    decode::{decode_frame, DecodedPacket},
    file::{for_each_frame, parse_pcap_args, FrameLoop},
    schema::send_error,
};

/// Schema of the `tcp_conversations` table function: one row per TCP
/// connection, from the initiator's perspective
pub fn tcp_conversations_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("src_ip", DataType::Utf8, false),
        Field::new("src_port", DataType::UInt16, false),
        Field::new("dst_ip", DataType::Utf8, false),
        Field::new("dst_port", DataType::UInt16, false),
        Field::new(
            "first_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "last_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("duration_ms", DataType::Float64, false),
        Field::new("packets_fwd", DataType::UInt64, false),
        Field::new("packets_rev", DataType::UInt64, false),
        Field::new("bytes_fwd", DataType::UInt64, false),
        Field::new("bytes_rev", DataType::UInt64, false),
        Field::new("retransmissions_fwd", DataType::UInt64, false),
        Field::new("retransmissions_rev", DataType::UInt64, false),
        Field::new("handshake_rtt_ms", DataType::Float64, true),
        Field::new("state", DataType::Utf8, false),
    ]))
}

/// Table function that aggregates a capture file into TCP conversations
#[derive(Debug, Default)]
pub struct TcpConversationsFunc {}

impl TableFunctionImpl for TcpConversationsFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let path = parse_pcap_args("tcp_conversations", exprs)?;
        Ok(Arc::new(TcpConversationsTable::new(path)))
    }
}

/// [`TableProvider`] backed by flow aggregation of a pcap/pcapng file
#[derive(Debug)]
pub struct TcpConversationsTable {
    path: String,
    schema: SchemaRef,
}

impl TcpConversationsTable {
    pub fn new(path: String) -> Self {
        Self {
            path,
            schema: tcp_conversations_schema(),
        }
    }
}

#[async_trait]
impl TableProvider for TcpConversationsTable {
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
        let exec = TcpConversationsExec::try_new(
            self.path.clone(),
            Arc::clone(&self.schema),
            projection.cloned(),
            limit,
        )?;
        Ok(Arc::new(exec))
    }
}

/// Execution plan that scans the capture on `execute` and emits a single
/// batch of conversations once the whole file has been read
#[derive(Debug)]
struct TcpConversationsExec {
    path: String,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    limit: Option<usize>,
    cache: Arc<PlanProperties>,
}

impl TcpConversationsExec {
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
            // Aggregation over the whole file: nothing is emitted until the
            // scan completes
            EmissionType::Final,
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

impl DisplayAs for TcpConversationsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "TcpConversationsExec: path={}, limit={:?}",
            self.path, self.limit
        )
    }
}

impl ExecutionPlan for TcpConversationsExec {
    fn name(&self) -> &str {
        "TcpConversationsExec"
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
            return internal_err!("TcpConversationsExec has a single partition, got {partition}");
        }
        let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&self.projected_schema), 2);
        let tx = builder.tx();
        let path = self.path.clone();
        let projection = self.projection.clone();
        let limit = self.limit;
        builder.spawn_blocking(move || read_conversations(path, projection, limit, tx));
        Ok(builder.build())
    }
}

/// Scans the capture, feeding every TCP packet into the flow tracker, and
/// sends the resulting conversations as a single batch
fn read_conversations(
    path: String,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    tx: Sender<Result<RecordBatch>>,
) -> Result<()> {
    let mut tracker = FlowTracker::default();
    let result = for_each_frame("tcp_conversations", &path, |_, frame| {
        let decoded = decode_frame(frame.link_type, frame.data);
        tracker.observe(frame.ts_micros, frame.origlen, &decoded);
        Ok(FrameLoop::Continue)
    });
    match result {
        Ok(()) => {
            let batch = tracker.into_batch()?;
            let batch = match limit {
                Some(limit) if limit < batch.num_rows() => batch.slice(0, limit),
                _ => batch,
            };
            let batch = match &projection {
                Some(p) => batch.project(p)?,
                None => batch,
            };
            let _ = tx.blocking_send(Ok(batch));
        }
        Err(e) => send_error(&tx, e),
    }
    Ok(())
}

/// An (ip, port) endpoint
type Endpoint = (String, u16);

/// Direction-independent flow identity: the two endpoints in sorted order
#[derive(Debug, PartialEq, Eq, Hash)]
struct FlowKey {
    lo: Endpoint,
    hi: Endpoint,
}

impl FlowKey {
    fn new(a: &Endpoint, b: &Endpoint) -> Self {
        if a <= b {
            Self {
                lo: a.clone(),
                hi: b.clone(),
            }
        } else {
            Self {
                lo: b.clone(),
                hi: a.clone(),
            }
        }
    }
}

/// `a <= b` in TCP sequence space (wraparound aware)
fn seq_leq(a: u32, b: u32) -> bool {
    a.wrapping_sub(b) as i32 <= 0
}

/// Accumulated state for one TCP connection
#[derive(Debug)]
struct Flow {
    initiator: Endpoint,
    responder: Endpoint,
    first_ts: i64,
    last_ts: i64,
    packets_fwd: u64,
    packets_rev: u64,
    bytes_fwd: u64,
    bytes_rev: u64,
    retransmissions_fwd: u64,
    retransmissions_rev: u64,
    /// Timestamp of the initiator's first SYN
    syn_ts: Option<i64>,
    /// Whether the responder's SYN|ACK has been seen
    synack_seen: bool,
    /// Timestamp of the initiator's first ACK after the SYN|ACK, completing
    /// the handshake
    handshake_ack_ts: Option<i64>,
    fin_fwd: bool,
    fin_rev: bool,
    rst: bool,
    /// Highest end of consumed sequence space per direction, for
    /// retransmission detection
    max_seq_end_fwd: Option<u32>,
    max_seq_end_rev: Option<u32>,
}

impl Flow {
    fn new(initiator: Endpoint, responder: Endpoint, first_ts: i64) -> Self {
        Self {
            initiator,
            responder,
            first_ts,
            last_ts: first_ts,
            packets_fwd: 0,
            packets_rev: 0,
            bytes_fwd: 0,
            bytes_rev: 0,
            retransmissions_fwd: 0,
            retransmissions_rev: 0,
            syn_ts: None,
            synack_seen: false,
            handshake_ack_ts: None,
            fin_fwd: false,
            fin_rev: false,
            rst: false,
            max_seq_end_fwd: None,
            max_seq_end_rev: None,
        }
    }

    fn handshake_rtt_ms(&self) -> Option<f64> {
        let syn = self.syn_ts?;
        let ack = self.handshake_ack_ts?;
        Some((ack - syn) as f64 / 1000.0)
    }

    fn state(&self) -> &'static str {
        if self.rst {
            "reset"
        } else if self.fin_fwd && self.fin_rev {
            "closed"
        } else if self.fin_fwd || self.fin_rev {
            "half_closed"
        } else {
            "active"
        }
    }
}

/// Groups observed TCP packets into flows
#[derive(Debug, Default)]
struct FlowTracker {
    flows: HashMap<FlowKey, Flow>,
}

impl FlowTracker {
    /// Feeds one decoded packet into the tracker; non-TCP packets and
    /// packets without addresses/ports are ignored
    fn observe(&mut self, ts: i64, frame_len: u32, decoded: &DecodedPacket<'_>) {
        if decoded.protocol.as_deref() != Some("tcp") {
            return;
        }
        let (Some(src_ip), Some(dst_ip), Some(src_port), Some(dst_port), Some(seq)) = (
            &decoded.src_ip,
            &decoded.dst_ip,
            decoded.src_port,
            decoded.dst_port,
            decoded.tcp_seq,
        ) else {
            return;
        };
        let flags: Vec<&str> = decoded
            .tcp_flags
            .as_deref()
            .unwrap_or_default()
            .split('|')
            .collect();
        let syn = flags.contains(&"SYN");
        let ack = flags.contains(&"ACK");
        let fin = flags.contains(&"FIN");
        let rst = flags.contains(&"RST");

        let src: Endpoint = (src_ip.clone(), src_port);
        let dst: Endpoint = (dst_ip.clone(), dst_port);
        let key = FlowKey::new(&src, &dst);
        let flow = self.flows.entry(key).or_insert_with(|| {
            // The initiator is normally the first endpoint seen sending; a
            // first-seen SYN|ACK means the capture missed the SYN and the
            // sender is actually the responder
            if syn && ack {
                Flow::new(dst.clone(), src.clone(), ts)
            } else {
                Flow::new(src.clone(), dst.clone(), ts)
            }
        });
        let fwd = src == flow.initiator;

        flow.last_ts = flow.last_ts.max(ts);
        if fwd {
            flow.packets_fwd += 1;
            flow.bytes_fwd += frame_len as u64;
        } else {
            flow.packets_rev += 1;
            flow.bytes_rev += frame_len as u64;
        }
        if rst {
            flow.rst = true;
        }
        if fin {
            if fwd {
                flow.fin_fwd = true;
            } else {
                flow.fin_rev = true;
            }
        }

        // Handshake: initiator SYN → responder SYN|ACK → initiator ACK
        if syn && !ack && fwd && flow.syn_ts.is_none() {
            flow.syn_ts = Some(ts);
        }
        if syn && ack && !fwd {
            flow.synack_seen = true;
        }
        if !syn && ack && fwd && flow.synack_seen && flow.handshake_ack_ts.is_none() {
            flow.handshake_ack_ts = Some(ts);
        }

        // Retransmission detection: a segment consuming sequence space
        // (payload, SYN, or FIN) whose end does not advance beyond the
        // highest end seen in its direction is a retransmission
        let consumed =
            decoded.payload.map(|p| p.len() as u32).unwrap_or(0) + syn as u32 + fin as u32;
        if consumed > 0 {
            let end = seq.wrapping_add(consumed);
            let (max_end, retransmissions) = if fwd {
                (&mut flow.max_seq_end_fwd, &mut flow.retransmissions_fwd)
            } else {
                (&mut flow.max_seq_end_rev, &mut flow.retransmissions_rev)
            };
            match max_end {
                Some(max) if seq_leq(end, *max) => *retransmissions += 1,
                Some(max) => *max = end,
                None => *max_end = Some(end),
            }
        }
    }

    /// Builds the conversations batch, ordered by first activity (then by
    /// endpoints, for deterministic output)
    fn into_batch(self) -> Result<RecordBatch> {
        let mut flows: Vec<Flow> = self.flows.into_values().collect();
        flows.sort_by(|a, b| {
            (a.first_ts, &a.initiator, &a.responder).cmp(&(b.first_ts, &b.initiator, &b.responder))
        });

        let mut src_ip = StringBuilder::new();
        let mut src_port = UInt16Builder::new();
        let mut dst_ip = StringBuilder::new();
        let mut dst_port = UInt16Builder::new();
        let mut first_timestamp = TimestampMicrosecondBuilder::new();
        let mut last_timestamp = TimestampMicrosecondBuilder::new();
        let mut duration_ms = Float64Builder::new();
        let mut packets_fwd = UInt64Builder::new();
        let mut packets_rev = UInt64Builder::new();
        let mut bytes_fwd = UInt64Builder::new();
        let mut bytes_rev = UInt64Builder::new();
        let mut retransmissions_fwd = UInt64Builder::new();
        let mut retransmissions_rev = UInt64Builder::new();
        let mut handshake_rtt_ms = Float64Builder::new();
        let mut state = StringBuilder::new();

        for flow in &flows {
            src_ip.append_value(&flow.initiator.0);
            src_port.append_value(flow.initiator.1);
            dst_ip.append_value(&flow.responder.0);
            dst_port.append_value(flow.responder.1);
            first_timestamp.append_value(flow.first_ts);
            last_timestamp.append_value(flow.last_ts);
            duration_ms.append_value((flow.last_ts - flow.first_ts) as f64 / 1000.0);
            packets_fwd.append_value(flow.packets_fwd);
            packets_rev.append_value(flow.packets_rev);
            bytes_fwd.append_value(flow.bytes_fwd);
            bytes_rev.append_value(flow.bytes_rev);
            retransmissions_fwd.append_value(flow.retransmissions_fwd);
            retransmissions_rev.append_value(flow.retransmissions_rev);
            handshake_rtt_ms.append_option(flow.handshake_rtt_ms());
            state.append_value(flow.state());
        }

        Ok(RecordBatch::try_new(
            tcp_conversations_schema(),
            vec![
                Arc::new(src_ip.finish()),
                Arc::new(src_port.finish()),
                Arc::new(dst_ip.finish()),
                Arc::new(dst_port.finish()),
                Arc::new(first_timestamp.finish()),
                Arc::new(last_timestamp.finish()),
                Arc::new(duration_ms.finish()),
                Arc::new(packets_fwd.finish()),
                Arc::new(packets_rev.finish()),
                Arc::new(bytes_fwd.finish()),
                Arc::new(bytes_rev.finish()),
                Arc::new(retransmissions_fwd.finish()),
                Arc::new(retransmissions_rev.finish()),
                Arc::new(handshake_rtt_ms.finish()),
                Arc::new(state.finish()),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tcp_packet(
        src: (&str, u16),
        dst: (&str, u16),
        seq: u32,
        flags: &str,
        payload_len: usize,
    ) -> DecodedPacket<'static> {
        DecodedPacket {
            protocol: Some("tcp".to_string()),
            src_ip: Some(src.0.to_string()),
            dst_ip: Some(dst.0.to_string()),
            src_port: Some(src.1),
            dst_port: Some(dst.1),
            tcp_seq: Some(seq),
            tcp_flags: Some(flags.to_string()),
            payload: Some(&[0u8; 1500][..payload_len]),
            ..Default::default()
        }
    }

    const CLIENT: (&str, u16) = ("10.0.0.1", 51000);
    const SERVER: (&str, u16) = ("10.0.0.2", 443);

    #[test]
    fn test_handshake_rtt_and_state() {
        let mut tracker = FlowTracker::default();
        tracker.observe(0, 60, &tcp_packet(CLIENT, SERVER, 1000, "SYN", 0));
        tracker.observe(50_000, 60, &tcp_packet(SERVER, CLIENT, 2000, "SYN|ACK", 0));
        tracker.observe(100_000, 54, &tcp_packet(CLIENT, SERVER, 1001, "ACK", 0));
        let flow = tracker.flows.values().next().unwrap();
        assert_eq!(flow.initiator, ("10.0.0.1".to_string(), 51000));
        assert_eq!(flow.handshake_rtt_ms(), Some(100.0));
        assert_eq!(flow.state(), "active");
        assert_eq!(flow.packets_fwd, 2);
        assert_eq!(flow.packets_rev, 1);
    }

    #[test]
    fn test_retransmission_detected_per_direction() {
        let mut tracker = FlowTracker::default();
        tracker.observe(0, 100, &tcp_packet(CLIENT, SERVER, 1000, "PSH|ACK", 5));
        // Same segment again: a retransmission
        tracker.observe(1000, 100, &tcp_packet(CLIENT, SERVER, 1000, "PSH|ACK", 5));
        // New data advancing the window: not a retransmission
        tracker.observe(2000, 100, &tcp_packet(CLIENT, SERVER, 1005, "PSH|ACK", 5));
        // Reverse direction is tracked independently
        tracker.observe(3000, 100, &tcp_packet(SERVER, CLIENT, 9000, "PSH|ACK", 5));
        let flow = tracker.flows.values().next().unwrap();
        assert_eq!(flow.retransmissions_fwd, 1);
        assert_eq!(flow.retransmissions_rev, 0);
    }

    #[test]
    fn test_states() {
        // RST wins
        let mut tracker = FlowTracker::default();
        tracker.observe(0, 60, &tcp_packet(CLIENT, SERVER, 1, "SYN", 0));
        tracker.observe(1, 60, &tcp_packet(SERVER, CLIENT, 1, "RST|ACK", 0));
        assert_eq!(tracker.flows.values().next().unwrap().state(), "reset");

        // FIN from one side only
        let mut tracker = FlowTracker::default();
        tracker.observe(0, 60, &tcp_packet(CLIENT, SERVER, 1, "FIN|ACK", 0));
        assert_eq!(
            tracker.flows.values().next().unwrap().state(),
            "half_closed"
        );

        // FIN from both sides
        let mut tracker = FlowTracker::default();
        tracker.observe(0, 60, &tcp_packet(CLIENT, SERVER, 1, "FIN|ACK", 0));
        tracker.observe(1, 60, &tcp_packet(SERVER, CLIENT, 1, "FIN|ACK", 0));
        assert_eq!(tracker.flows.values().next().unwrap().state(), "closed");
    }

    #[test]
    fn test_first_seen_synack_flips_initiator() {
        // Capture started after the SYN: the SYN|ACK sender is the responder
        let mut tracker = FlowTracker::default();
        tracker.observe(0, 60, &tcp_packet(SERVER, CLIENT, 2000, "SYN|ACK", 0));
        let flow = tracker.flows.values().next().unwrap();
        assert_eq!(flow.initiator, ("10.0.0.1".to_string(), 51000));
        assert_eq!(flow.packets_rev, 1);
    }

    #[test]
    fn test_distinct_flows_are_separate() {
        let mut tracker = FlowTracker::default();
        tracker.observe(0, 60, &tcp_packet(CLIENT, SERVER, 1, "SYN", 0));
        tracker.observe(1, 60, &tcp_packet(("10.0.0.3", 6000), SERVER, 1, "SYN", 0));
        assert_eq!(tracker.flows.len(), 2);
    }

    #[test]
    fn test_non_tcp_ignored() {
        let mut tracker = FlowTracker::default();
        let mut udp = tcp_packet(CLIENT, SERVER, 1, "", 0);
        udp.protocol = Some("udp".to_string());
        tracker.observe(0, 60, &udp);
        assert!(tracker.flows.is_empty());
    }

    #[test]
    fn test_call_rejects_bad_arguments() {
        let func = TcpConversationsFunc::default();
        let err = func.call(&[]).unwrap_err();
        assert!(err.to_string().contains("tcp_conversations"), "got: {err}");
    }
}

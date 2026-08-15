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

//! Arrow schema for decoded packets and a columnar builder for turning
//! decoded packets into [`RecordBatch`]es.

use std::sync::Arc;

use datafusion::arrow::{
    array::{
        BinaryBuilder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder, UInt16Builder,
        UInt32Builder, UInt64Builder, UInt8Builder,
    },
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
};
use datafusion::common::Result;
use tokio::sync::mpsc::Sender;

use crate::decode::DecodedPacket;

/// Index of the `payload` column, used to skip materializing packet payloads
/// when the column is not projected
pub const PAYLOAD_COLUMN_INDEX: usize = 20;

/// Schema shared by the `pcap` and `capture` table functions. One row per
/// captured frame; layers that could not be decoded yield null columns.
pub fn packet_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("frame_number", DataType::UInt64, false),
        Field::new("length", DataType::UInt32, false),
        Field::new("capture_length", DataType::UInt32, false),
        Field::new("eth_src", DataType::Utf8, true),
        Field::new("eth_dst", DataType::Utf8, true),
        Field::new("ethertype", DataType::Utf8, true),
        Field::new("vlan", DataType::UInt16, true),
        Field::new("ip_version", DataType::UInt8, true),
        Field::new("src_ip", DataType::Utf8, true),
        Field::new("dst_ip", DataType::Utf8, true),
        Field::new("ttl", DataType::UInt8, true),
        Field::new("protocol", DataType::Utf8, true),
        Field::new("src_port", DataType::UInt16, true),
        Field::new("dst_port", DataType::UInt16, true),
        Field::new("tcp_flags", DataType::Utf8, true),
        Field::new("tcp_seq", DataType::UInt32, true),
        Field::new("tcp_ack", DataType::UInt32, true),
        Field::new("tcp_window", DataType::UInt16, true),
        Field::new("payload_length", DataType::UInt32, true),
        Field::new("payload", DataType::Binary, true),
    ]))
}

/// Accumulates decoded packets into columnar builders and produces
/// [`RecordBatch`]es with the full [`packet_schema`]
pub struct PacketBatchBuilder {
    schema: SchemaRef,
    /// Whether to materialize payload bytes (false when the `payload` column
    /// is projected away, avoiding the copy)
    include_payload: bool,
    rows: usize,
    timestamp: TimestampMicrosecondBuilder,
    frame_number: UInt64Builder,
    length: UInt32Builder,
    capture_length: UInt32Builder,
    eth_src: StringBuilder,
    eth_dst: StringBuilder,
    ethertype: StringBuilder,
    vlan: UInt16Builder,
    ip_version: UInt8Builder,
    src_ip: StringBuilder,
    dst_ip: StringBuilder,
    ttl: UInt8Builder,
    protocol: StringBuilder,
    src_port: UInt16Builder,
    dst_port: UInt16Builder,
    tcp_flags: StringBuilder,
    tcp_seq: UInt32Builder,
    tcp_ack: UInt32Builder,
    tcp_window: UInt16Builder,
    payload_length: UInt32Builder,
    payload: BinaryBuilder,
}

impl PacketBatchBuilder {
    pub fn new(include_payload: bool) -> Self {
        Self {
            schema: packet_schema(),
            include_payload,
            rows: 0,
            timestamp: TimestampMicrosecondBuilder::new(),
            frame_number: UInt64Builder::new(),
            length: UInt32Builder::new(),
            capture_length: UInt32Builder::new(),
            eth_src: StringBuilder::new(),
            eth_dst: StringBuilder::new(),
            ethertype: StringBuilder::new(),
            vlan: UInt16Builder::new(),
            ip_version: UInt8Builder::new(),
            src_ip: StringBuilder::new(),
            dst_ip: StringBuilder::new(),
            ttl: UInt8Builder::new(),
            protocol: StringBuilder::new(),
            src_port: UInt16Builder::new(),
            dst_port: UInt16Builder::new(),
            tcp_flags: StringBuilder::new(),
            tcp_seq: UInt32Builder::new(),
            tcp_ack: UInt32Builder::new(),
            tcp_window: UInt16Builder::new(),
            payload_length: UInt32Builder::new(),
            payload: BinaryBuilder::new(),
        }
    }

    pub fn append(
        &mut self,
        ts_micros: i64,
        frame_number: u64,
        length: u32,
        capture_length: u32,
        decoded: &DecodedPacket<'_>,
    ) {
        self.timestamp.append_value(ts_micros);
        self.frame_number.append_value(frame_number);
        self.length.append_value(length);
        self.capture_length.append_value(capture_length);
        self.eth_src.append_option(decoded.eth_src.as_deref());
        self.eth_dst.append_option(decoded.eth_dst.as_deref());
        self.ethertype.append_option(decoded.ethertype.as_deref());
        self.vlan.append_option(decoded.vlan);
        self.ip_version.append_option(decoded.ip_version);
        self.src_ip.append_option(decoded.src_ip.as_deref());
        self.dst_ip.append_option(decoded.dst_ip.as_deref());
        self.ttl.append_option(decoded.ttl);
        self.protocol.append_option(decoded.protocol.as_deref());
        self.src_port.append_option(decoded.src_port);
        self.dst_port.append_option(decoded.dst_port);
        self.tcp_flags.append_option(decoded.tcp_flags.as_deref());
        self.tcp_seq.append_option(decoded.tcp_seq);
        self.tcp_ack.append_option(decoded.tcp_ack);
        self.tcp_window.append_option(decoded.tcp_window);
        self.payload_length
            .append_option(decoded.payload.map(|p| p.len() as u32));
        if self.include_payload {
            self.payload.append_option(decoded.payload);
        } else {
            self.payload.append_null();
        }
        self.rows += 1;
    }

    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Builds a [`RecordBatch`] with the full schema from the accumulated
    /// rows, resetting the builder
    pub fn finish(&mut self) -> Result<RecordBatch> {
        self.rows = 0;
        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.timestamp.finish()),
                Arc::new(self.frame_number.finish()),
                Arc::new(self.length.finish()),
                Arc::new(self.capture_length.finish()),
                Arc::new(self.eth_src.finish()),
                Arc::new(self.eth_dst.finish()),
                Arc::new(self.ethertype.finish()),
                Arc::new(self.vlan.finish()),
                Arc::new(self.ip_version.finish()),
                Arc::new(self.src_ip.finish()),
                Arc::new(self.dst_ip.finish()),
                Arc::new(self.ttl.finish()),
                Arc::new(self.protocol.finish()),
                Arc::new(self.src_port.finish()),
                Arc::new(self.dst_port.finish()),
                Arc::new(self.tcp_flags.finish()),
                Arc::new(self.tcp_seq.finish()),
                Arc::new(self.tcp_ack.finish()),
                Arc::new(self.tcp_window.finish()),
                Arc::new(self.payload_length.finish()),
                Arc::new(self.payload.finish()),
            ],
        )?;
        Ok(batch)
    }
}

/// Finishes the builder, applies the optional projection, and sends the
/// resulting batch on `tx` from a blocking context. Returns `false` when the
/// consumer has dropped the stream (e.g. a `LIMIT` was satisfied) and reading
/// should stop.
pub(crate) fn send_batch(
    builder: &mut PacketBatchBuilder,
    projection: &Option<Vec<usize>>,
    tx: &Sender<Result<RecordBatch>>,
) -> Result<bool> {
    if builder.is_empty() {
        return Ok(true);
    }
    let batch = builder.finish()?;
    let batch = match projection {
        Some(p) => batch.project(p)?,
        None => batch,
    };
    Ok(tx.blocking_send(Ok(batch)).is_ok())
}

/// Sends an error on `tx` from a blocking context, ignoring send failures
/// (the consumer may already be gone)
pub(crate) fn send_error(
    tx: &Sender<Result<RecordBatch>>,
    err: datafusion::error::DataFusionError,
) {
    let _ = tx.blocking_send(Err(err));
}

/// Whether the `payload` column is included in the projection (and therefore
/// needs to be materialized)
pub(crate) fn payload_projected(projection: &Option<Vec<usize>>) -> bool {
    match projection {
        Some(p) => p.contains(&PAYLOAD_COLUMN_INDEX),
        None => true,
    }
}

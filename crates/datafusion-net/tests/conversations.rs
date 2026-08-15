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

//! End-to-end tests for the `tcp_conversations` table function: a full TCP
//! connection (handshake, data with one retransmission, orderly close) is
//! written with [`datafusion_net::writer`] and aggregated back into a
//! conversation row

use std::{path::Path, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion_net::{writer::PcapWriter, TcpConversationsFunc};
use etherparse::PacketBuilder;

const CLIENT_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x01];
const SERVER_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x02];
const CLIENT_IP: [u8; 4] = [10, 0, 0, 1];
const SERVER_IP: [u8; 4] = [10, 0, 0, 2];
const CLIENT_PORT: u16 = 51000;
const SERVER_PORT: u16 = 443;
/// 2025-01-01T00:00:00Z
const BASE_TS_MICROS: i64 = 1_735_689_600_000_000;
const MS: i64 = 1_000;

struct Writer {
    inner: PcapWriter<std::fs::File>,
}

impl Writer {
    fn new(path: &Path) -> Self {
        let file = std::fs::File::create(path).unwrap();
        Self {
            inner: PcapWriter::new(file, 1).unwrap(),
        }
    }

    fn client_packet(
        &mut self,
        at_ms: i64,
        seq: u32,
        ack: Option<u32>,
        syn: bool,
        fin: bool,
        payload: &[u8],
    ) {
        self.packet(true, at_ms, seq, ack, syn, fin, payload);
    }

    fn server_packet(
        &mut self,
        at_ms: i64,
        seq: u32,
        ack: Option<u32>,
        syn: bool,
        fin: bool,
        payload: &[u8],
    ) {
        self.packet(false, at_ms, seq, ack, syn, fin, payload);
    }

    #[allow(clippy::too_many_arguments)]
    fn packet(
        &mut self,
        from_client: bool,
        at_ms: i64,
        seq: u32,
        ack: Option<u32>,
        syn: bool,
        fin: bool,
        payload: &[u8],
    ) {
        let (src_mac, dst_mac, src_ip, dst_ip, src_port, dst_port) = if from_client {
            (
                CLIENT_MAC,
                SERVER_MAC,
                CLIENT_IP,
                SERVER_IP,
                CLIENT_PORT,
                SERVER_PORT,
            )
        } else {
            (
                SERVER_MAC,
                CLIENT_MAC,
                SERVER_IP,
                CLIENT_IP,
                SERVER_PORT,
                CLIENT_PORT,
            )
        };
        let mut builder = PacketBuilder::ethernet2(src_mac, dst_mac)
            .ipv4(src_ip, dst_ip, 64)
            .tcp(src_port, dst_port, seq, 1024);
        if syn {
            builder = builder.syn();
        }
        if fin {
            builder = builder.fin();
        }
        if let Some(ack) = ack {
            builder = builder.ack(ack);
        }
        let mut frame = Vec::with_capacity(builder.size(payload.len()));
        builder.write(&mut frame, payload).unwrap();
        self.inner
            .write_packet(BASE_TS_MICROS + at_ms * MS, &frame)
            .unwrap();
    }
}

/// Writes a complete connection: handshake (100ms RTT), client request with
/// one retransmission, server response, orderly close, plus an unrelated UDP
/// packet that must be ignored
fn write_test_pcap(path: &Path) {
    let mut w = Writer::new(path);
    w.client_packet(0, 1000, None, true, false, &[]); // SYN
    w.server_packet(50, 2000, Some(1001), true, false, &[]); // SYN|ACK
    w.client_packet(100, 1001, Some(2001), false, false, &[]); // ACK
    w.client_packet(200, 1001, Some(2001), false, false, b"hello"); // data
    w.client_packet(300, 1001, Some(2001), false, false, b"hello"); // retransmission
    w.server_packet(400, 2001, Some(1006), false, false, b"hi!"); // response
    w.client_packet(500, 1006, Some(2004), false, true, &[]); // FIN
    w.server_packet(600, 2004, Some(1007), false, true, &[]); // FIN

    let udp = PacketBuilder::ethernet2(CLIENT_MAC, SERVER_MAC)
        .ipv4(CLIENT_IP, [8, 8, 8, 8], 64)
        .udp(53001, 53);
    let mut frame = Vec::with_capacity(udp.size(3));
    udp.write(&mut frame, b"dns").unwrap();
    w.inner
        .write_packet(BASE_TS_MICROS + 700 * MS, &frame)
        .unwrap();
    w.inner.flush().unwrap();
}

async fn run(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

fn setup() -> (SessionContext, tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.pcap");
    write_test_pcap(&path);
    let ctx = SessionContext::new();
    ctx.register_udtf(
        "tcp_conversations",
        Arc::new(TcpConversationsFunc::default()),
    );
    let path = path.display().to_string();
    (ctx, dir, path)
}

#[tokio::test]
async fn test_conversation_aggregation() {
    let (ctx, _dir, path) = setup();
    let sql = format!(
        "SELECT src_ip, src_port, dst_ip, dst_port, packets_fwd, packets_rev, \
                retransmissions_fwd, retransmissions_rev, handshake_rtt_ms, \
                duration_ms, state \
         FROM tcp_conversations('{path}')"
    );
    let out = run(&ctx, &sql).await;
    let expected = "\
+----------+----------+----------+----------+-------------+-------------+---------------------+---------------------+------------------+-------------+--------+
| src_ip   | src_port | dst_ip   | dst_port | packets_fwd | packets_rev | retransmissions_fwd | retransmissions_rev | handshake_rtt_ms | duration_ms | state  |
+----------+----------+----------+----------+-------------+-------------+---------------------+---------------------+------------------+-------------+--------+
| 10.0.0.1 | 51000    | 10.0.0.2 | 443      | 5           | 3           | 1                   | 0                   | 100.0            | 600.0       | closed |
+----------+----------+----------+----------+-------------+-------------+---------------------+---------------------+------------------+-------------+--------+";
    assert_eq!(out.trim(), expected.trim(), "got:\n{out}");
}

#[tokio::test]
async fn test_conversation_bytes_and_timestamps() {
    let (ctx, _dir, path) = setup();
    let sql = format!(
        "SELECT count(*) AS conversations, \
                sum(packets_fwd + packets_rev) AS packets, \
                min(first_timestamp) = arrow_cast('2025-01-01T00:00:00Z', 'Timestamp(Microsecond, None)') AS starts_at_base, \
                sum(bytes_fwd) > sum(bytes_rev) AS fwd_heavier \
         FROM tcp_conversations('{path}')"
    );
    let out = run(&ctx, &sql).await;
    assert!(out.contains("| 1 "), "expected one conversation in:\n{out}");
    assert!(out.contains("| 8 "), "expected eight packets in:\n{out}");
    assert_eq!(
        out.matches("true").count(),
        2,
        "expected timestamp and byte checks true in:\n{out}"
    );
}

#[tokio::test]
async fn test_conversation_missing_file_errors() {
    let (ctx, _dir, _path) = setup();
    let result = ctx
        .sql("SELECT * FROM tcp_conversations('/definitely/missing.pcap')")
        .await
        .unwrap()
        .collect()
        .await;
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("tcp_conversations failed to open"),
        "unexpected error: {err}"
    );
}

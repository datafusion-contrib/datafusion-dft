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

//! Round-trip tests: write a capture file with [`datafusion_net::writer`]
//! and query it back through the `pcap` table function

use std::{path::Path, sync::Arc};

use datafusion::{arrow::util::pretty::pretty_format_batches, prelude::SessionContext};
use datafusion_net::{writer::PcapWriter, PcapFunc};
use etherparse::PacketBuilder;

const SRC_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x01];
const DST_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x02];
/// 2025-01-01T00:00:00Z
const BASE_TS_MICROS: i64 = 1_735_689_600_000_000;

/// Writes a small capture with two TCP packets and one UDP packet
fn write_test_pcap(path: &Path) {
    let file = std::fs::File::create(path).unwrap();
    let mut writer = PcapWriter::new(file, 1).unwrap();

    let builder = PacketBuilder::ethernet2(SRC_MAC, DST_MAC)
        .ipv4([10, 0, 0, 1], [10, 0, 0, 2], 64)
        .tcp(51000, 443, 1000, 1024)
        .syn();
    let mut frame = Vec::with_capacity(builder.size(0));
    builder.write(&mut frame, &[]).unwrap();
    writer.write_packet(BASE_TS_MICROS, &frame).unwrap();

    let builder = PacketBuilder::ethernet2(DST_MAC, SRC_MAC)
        .ipv4([10, 0, 0, 2], [10, 0, 0, 1], 64)
        .tcp(443, 51000, 2000, 1024)
        .syn()
        .ack(1001);
    let mut frame = Vec::with_capacity(builder.size(0));
    builder.write(&mut frame, &[]).unwrap();
    writer.write_packet(BASE_TS_MICROS + 500, &frame).unwrap();

    let builder = PacketBuilder::ethernet2(SRC_MAC, DST_MAC)
        .ipv4([10, 0, 0, 1], [8, 8, 8, 8], 64)
        .udp(53001, 53);
    let payload = b"dns query";
    let mut frame = Vec::with_capacity(builder.size(payload.len()));
    builder.write(&mut frame, payload).unwrap();
    writer.write_packet(BASE_TS_MICROS + 1000, &frame).unwrap();

    writer.flush().unwrap();
}

async fn ctx_with_pcap() -> (SessionContext, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    write_test_pcap(&dir.path().join("test.pcap"));
    let ctx = SessionContext::new();
    ctx.register_udtf("pcap", Arc::new(PcapFunc::default()));
    (ctx, dir)
}

async fn run(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    pretty_format_batches(&batches).unwrap().to_string()
}

#[tokio::test]
async fn test_select_columns() {
    let (ctx, dir) = ctx_with_pcap().await;
    let sql = format!(
        "SELECT frame_number, src_ip, dst_ip, protocol, src_port, dst_port, tcp_flags \
         FROM pcap('{}/test.pcap')",
        dir.path().display()
    );
    let output = run(&ctx, &sql).await;
    let expected = "\
+--------------+----------+----------+----------+----------+----------+-----------+
| frame_number | src_ip   | dst_ip   | protocol | src_port | dst_port | tcp_flags |
+--------------+----------+----------+----------+----------+----------+-----------+
| 1            | 10.0.0.1 | 10.0.0.2 | tcp      | 51000    | 443      | SYN       |
| 2            | 10.0.0.2 | 10.0.0.1 | tcp      | 443      | 51000    | SYN|ACK   |
| 3            | 10.0.0.1 | 8.8.8.8  | udp      | 53001    | 53       |           |
+--------------+----------+----------+----------+----------+----------+-----------+";
    assert_eq!(output, expected);
}

#[tokio::test]
async fn test_filter_and_aggregate() {
    let (ctx, dir) = ctx_with_pcap().await;
    let sql = format!(
        "SELECT protocol, count(*) AS packets FROM pcap('{}/test.pcap') \
         GROUP BY protocol ORDER BY protocol",
        dir.path().display()
    );
    let output = run(&ctx, &sql).await;
    let expected = "\
+----------+---------+
| protocol | packets |
+----------+---------+
| tcp      | 2       |
| udp      | 1       |
+----------+---------+";
    assert_eq!(output, expected);
}

#[tokio::test]
async fn test_limit() {
    let (ctx, dir) = ctx_with_pcap().await;
    let sql = format!(
        "SELECT frame_number FROM pcap('{}/test.pcap') LIMIT 1",
        dir.path().display()
    );
    let output = run(&ctx, &sql).await;
    assert!(output.contains("| 1            |"));
    assert!(!output.contains("| 2            |"));
}

#[tokio::test]
async fn test_payload_and_timestamp() {
    let (ctx, dir) = ctx_with_pcap().await;
    let sql = format!(
        "SELECT timestamp, payload_length, payload FROM pcap('{}/test.pcap') \
         WHERE protocol = 'udp'",
        dir.path().display()
    );
    let output = run(&ctx, &sql).await;
    // 'dns query' hex encoded
    assert!(output.contains("646e73207175657279"));
    assert!(output.contains("2025-01-01T00:00:00.001"));
    assert!(output.contains("| 9              |"));
}

#[tokio::test]
async fn test_missing_file_errors_at_execution() {
    let ctx = SessionContext::new();
    ctx.register_udtf("pcap", Arc::new(PcapFunc::default()));
    let err = ctx
        .sql("SELECT * FROM pcap('/does/not/exist.pcap')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("failed to open"));
}

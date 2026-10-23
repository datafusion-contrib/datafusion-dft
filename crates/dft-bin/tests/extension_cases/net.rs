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

//! Tests for the `pcap` and `capture` table functions

use std::path::Path;

use datafusion_net::writer::PcapWriter;
use etherparse::PacketBuilder;

use crate::extension_cases::TestExecution;

const SRC_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x01];
const DST_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x02];
/// 2025-01-01T00:00:00Z
const BASE_TS_MICROS: i64 = 1_735_689_600_000_000;

/// Writes a small capture: a TCP handshake packet each way and a DNS-style
/// UDP query
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

#[tokio::test]
async fn test_pcap_select() {
    let dir = tempfile::tempdir().unwrap();
    write_test_pcap(&dir.path().join("test.pcap"));
    let execution = TestExecution::new().await;
    let sql = format!(
        "SELECT frame_number, src_ip, dst_ip, protocol, src_port, dst_port, tcp_flags \
         FROM pcap('{}/test.pcap')",
        dir.path().display()
    );
    let output = execution.run_and_format(&sql).await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +--------------+----------+----------+----------+----------+----------+-----------+
    - "| frame_number | src_ip   | dst_ip   | protocol | src_port | dst_port | tcp_flags |"
    - +--------------+----------+----------+----------+----------+----------+-----------+
    - "| 1            | 10.0.0.1 | 10.0.0.2 | tcp      | 51000    | 443      | SYN       |"
    - "| 2            | 10.0.0.2 | 10.0.0.1 | tcp      | 443      | 51000    | SYN|ACK   |"
    - "| 3            | 10.0.0.1 | 8.8.8.8  | udp      | 53001    | 53       |           |"
    - +--------------+----------+----------+----------+----------+----------+-----------+
    "#);
}

#[tokio::test]
async fn test_pcap_filter_and_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    write_test_pcap(&dir.path().join("test.pcap"));
    let execution = TestExecution::new().await;
    let sql = format!(
        "SELECT protocol, count(*) AS packets, sum(length) AS bytes \
         FROM pcap('{}/test.pcap') WHERE dst_port = 443 GROUP BY protocol",
        dir.path().display()
    );
    let output = execution.run_and_format(&sql).await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +----------+---------+-------+
    - "| protocol | packets | bytes |"
    - +----------+---------+-------+
    - "| tcp      | 1       | 54    |"
    - +----------+---------+-------+
    "#);
}

#[tokio::test]
async fn test_pcap_limit() {
    let dir = tempfile::tempdir().unwrap();
    write_test_pcap(&dir.path().join("test.pcap"));
    let execution = TestExecution::new().await;
    let sql = format!(
        "SELECT frame_number, protocol FROM pcap('{}/test.pcap') LIMIT 2",
        dir.path().display()
    );
    let output = execution.run_and_format(&sql).await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +--------------+----------+
    - "| frame_number | protocol |"
    - +--------------+----------+
    - "| 1            | tcp      |"
    - "| 2            | tcp      |"
    - +--------------+----------+
    "#);
}

#[tokio::test]
async fn test_capture_explain_does_not_open_device() {
    let execution = TestExecution::new().await;
    // Planning must not open a capture device, which would fail without
    // elevated privileges (and this device does not exist anyway)
    let result = execution
        .run("EXPLAIN SELECT * FROM capture('definitely-not-a-device', 'tcp', 5)")
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_interfaces_lists_devices() {
    let execution = TestExecution::new().await;
    // The device list is environment dependent, but any host running the
    // tests has at least one interface
    let output = execution
        .run_and_format("SELECT count(*) >= 1 AS has_interfaces FROM interfaces()")
        .await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +----------------+
    - "| has_interfaces |"
    - +----------------+
    - "| true           |"
    - +----------------+
    "#);
}

#[tokio::test]
async fn test_show_functions_documents_udfs() {
    let execution = TestExecution::new().await;
    // Each scalar UDF's description and syntax come from its documentation()
    // implementation, which is what `SHOW FUNCTIONS` displays. Query the
    // routines table (which `SHOW FUNCTIONS` is rewritten to join against)
    // for the two documented columns.
    let output = execution
        .run_and_format(
            "SELECT routine_name, syntax_example FROM information_schema.routines \
             WHERE routine_name IN ('reverse_dns', 'geoip', 'dns_query', 'tls_sni') \
             ORDER BY routine_name",
        )
        .await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +--------------+-----------------------+
    - "| routine_name | syntax_example        |"
    - +--------------+-----------------------+
    - "| dns_query    | dns_query(payload)    |"
    - "| geoip        | geoip(ip [, db_path]) |"
    - "| reverse_dns  | reverse_dns(ip)       |"
    - "| tls_sni      | tls_sni(payload)      |"
    - +--------------+-----------------------+
    "#);

    // Every one of them also carries a non-empty description
    let with_descriptions = execution
        .run(
            "SELECT routine_name FROM information_schema.routines \
              WHERE routine_name IN ('reverse_dns', 'geoip', 'dns_query', 'tls_sni') \
              AND description IS NOT NULL AND length(description) > 0",
        )
        .await
        .unwrap();
    let rows: usize = with_descriptions.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 4, "expected all four UDFs to have descriptions");

    // And `SHOW FUNCTIONS` itself surfaces the documentation end to end
    let show = execution
        .run_and_format("SHOW FUNCTIONS LIKE 'tls_sni'")
        .await;
    let joined = show.join("\n");
    assert!(
        joined.contains("Server Name Indication"),
        "SHOW FUNCTIONS should include the description:\n{joined}"
    );
    assert!(
        joined.contains("tls_sni(payload)"),
        "SHOW FUNCTIONS should include the syntax example:\n{joined}"
    );
}

/// A minimal DNS query for `example.com` A record
fn dns_query_bytes() -> Vec<u8> {
    let mut m = vec![
        0x12, 0x34, // id
        0x01, 0x00, // flags: RD
        0x00, 0x01, // qdcount
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // an/ns/ar
    ];
    m.extend_from_slice(b"\x07example\x03com\x00");
    m.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]); // A, IN
    m
}

/// A minimal TLS ClientHello with an SNI extension for `host`
fn client_hello_bytes(host: &str) -> Vec<u8> {
    let host = host.as_bytes();
    let mut sni_ext = Vec::new();
    let entry_len = 1 + 2 + host.len();
    sni_ext.extend_from_slice(&(entry_len as u16).to_be_bytes());
    sni_ext.push(0); // host_name
    sni_ext.extend_from_slice(&(host.len() as u16).to_be_bytes());
    sni_ext.extend_from_slice(host);

    let mut extensions = Vec::new();
    extensions.extend_from_slice(&0u16.to_be_bytes()); // server_name
    extensions.extend_from_slice(&(sni_ext.len() as u16).to_be_bytes());
    extensions.extend_from_slice(&sni_ext);

    let mut body = Vec::new();
    body.extend_from_slice(&[0x03, 0x03]);
    body.extend_from_slice(&[0u8; 32]);
    body.push(0);
    body.extend_from_slice(&[0x00, 0x02, 0x13, 0x01]);
    body.extend_from_slice(&[0x01, 0x00]);
    body.extend_from_slice(&(extensions.len() as u16).to_be_bytes());
    body.extend_from_slice(&extensions);

    let mut handshake = vec![0x01]; // ClientHello
    let body_len = body.len();
    handshake.extend_from_slice(&[
        (body_len >> 16) as u8,
        (body_len >> 8) as u8,
        body_len as u8,
    ]);
    handshake.extend_from_slice(&body);

    let mut record = vec![0x16, 0x03, 0x01]; // handshake, TLS 1.0 record
    record.extend_from_slice(&(handshake.len() as u16).to_be_bytes());
    record.extend_from_slice(&handshake);
    record
}

/// Writes a capture with a real DNS query (UDP:53) and a TLS ClientHello
/// (TCP:443) so the payload UDFs have something to decode
fn write_payload_pcap(path: &Path) {
    let file = std::fs::File::create(path).unwrap();
    let mut writer = PcapWriter::new(file, 1).unwrap();

    let dns = dns_query_bytes();
    let builder = PacketBuilder::ethernet2(SRC_MAC, DST_MAC)
        .ipv4([10, 0, 0, 1], [8, 8, 8, 8], 64)
        .udp(53001, 53);
    let mut frame = Vec::with_capacity(builder.size(dns.len()));
    builder.write(&mut frame, &dns).unwrap();
    writer.write_packet(BASE_TS_MICROS, &frame).unwrap();

    let hello = client_hello_bytes("example.com");
    let builder = PacketBuilder::ethernet2(SRC_MAC, DST_MAC)
        .ipv4([10, 0, 0, 1], [10, 0, 0, 2], 64)
        .tcp(51000, 443, 1000, 1024)
        .psh()
        .ack(1);
    let mut frame = Vec::with_capacity(builder.size(hello.len()));
    builder.write(&mut frame, &hello).unwrap();
    writer.write_packet(BASE_TS_MICROS + 500, &frame).unwrap();

    writer.flush().unwrap();
}

#[tokio::test]
async fn test_tcp_conversations() {
    let dir = tempfile::tempdir().unwrap();
    write_test_pcap(&dir.path().join("test.pcap"));
    let execution = TestExecution::new().await;
    // The fixture has a SYN and a SYN|ACK for one flow (initiator 10.0.0.1),
    // plus an unrelated UDP packet that must not appear
    let sql = format!(
        "SELECT src_ip, src_port, dst_ip, dst_port, packets_fwd, packets_rev, state \
         FROM tcp_conversations('{}/test.pcap')",
        dir.path().display()
    );
    let output = execution.run_and_format(&sql).await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +----------+----------+----------+----------+-------------+-------------+--------+
    - "| src_ip   | src_port | dst_ip   | dst_port | packets_fwd | packets_rev | state  |"
    - +----------+----------+----------+----------+-------------+-------------+--------+
    - "| 10.0.0.1 | 51000    | 10.0.0.2 | 443      | 1           | 1           | active |"
    - +----------+----------+----------+----------+-------------+-------------+--------+
    "#);
}

#[tokio::test]
async fn test_dns_query_udf() {
    let dir = tempfile::tempdir().unwrap();
    write_payload_pcap(&dir.path().join("test.pcap"));
    let execution = TestExecution::new().await;
    let sql = format!(
        "SELECT dns_query(payload)['name'] AS name, dns_query(payload)['query_type'] AS qtype \
         FROM pcap('{}/test.pcap') WHERE dst_port = 53",
        dir.path().display()
    );
    let output = execution.run_and_format(&sql).await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +-------------+-------+
    - "| name        | qtype |"
    - +-------------+-------+
    - "| example.com | A     |"
    - +-------------+-------+
    "#);
}

#[tokio::test]
async fn test_tls_sni_udf() {
    let dir = tempfile::tempdir().unwrap();
    write_payload_pcap(&dir.path().join("test.pcap"));
    let execution = TestExecution::new().await;
    let sql = format!(
        "SELECT tls_sni(payload) AS host FROM pcap('{}/test.pcap') WHERE dst_port = 443",
        dir.path().display()
    );
    let output = execution.run_and_format(&sql).await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +-------------+
    - "| host        |"
    - +-------------+
    - "| example.com |"
    - +-------------+
    "#);
}

#[tokio::test]
async fn test_pcap_wide_adds_enrichment_columns() {
    let dir = tempfile::tempdir().unwrap();
    write_test_pcap(&dir.path().join("test.pcap"));
    let execution = TestExecution::new().await;
    // The fixture's private 10.0.0.0/8 addresses are absent from any
    // geolocation database (and none is configured in the test environment),
    // so the geo columns are NULL. This exercises that the wide schema is
    // present and the query executes.
    let sql = format!(
        "SELECT src_ip, src_country, src_city, src_lat, src_lon \
         FROM pcap_wide('{}/test.pcap') WHERE dst_port = 443",
        dir.path().display()
    );
    let output = execution.run_and_format(&sql).await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +----------+-------------+----------+---------+---------+
    - "| src_ip   | src_country | src_city | src_lat | src_lon |"
    - +----------+-------------+----------+---------+---------+
    - "| 10.0.0.1 |             |          |         |         |"
    - +----------+-------------+----------+---------+---------+
    "#);

    // The reverse DNS columns are environment dependent; only exercise that
    // they project and the query executes
    let sql = format!(
        "SELECT src_host, dst_host FROM pcap_wide('{}/test.pcap')",
        dir.path().display()
    );
    let result = execution.run(&sql).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_capture_wide_explain_does_not_open_device() {
    let execution = TestExecution::new().await;
    // Like capture, planning capture_wide must not open a device
    let result = execution
        .run("EXPLAIN SELECT src_ip, src_host, src_country FROM capture_wide('definitely-not-a-device', 'tcp', 5)")
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_reverse_dns_loopback() {
    let execution = TestExecution::new().await;
    // The loopback address must resolve to some hostname on any host with a
    // working resolver; the exact PTR record is environment dependent so we
    // only assert it is non-null
    let output = execution
        .run_and_format("SELECT reverse_dns('127.0.0.1') IS NOT NULL AS resolved")
        .await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +----------+
    - "| resolved |"
    - +----------+
    - "| true     |"
    - +----------+
    "#);
}

#[tokio::test]
async fn test_reverse_dns_invalid_input_is_null() {
    let execution = TestExecution::new().await;
    // Unparseable input and a reserved address with no PTR record both yield
    // null rather than erroring the query
    let output = execution
        .run_and_format(
            "SELECT reverse_dns(ip) AS host FROM (VALUES ('not-an-ip'), ('192.0.2.1')) AS t(ip)",
        )
        .await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +------+
    - "| host |"
    - +------+
    - "|      |"
    - "|      |"
    - +------+
    "#);
}

#[tokio::test]
async fn test_geoip_invalid_ip_is_null() {
    let execution = TestExecution::new().await;
    // geoip parses the address before resolving the database, so an
    // unparseable address yields NULL even with a database path that does
    // not exist
    let output = execution
        .run_and_format("SELECT geoip('not-an-ip', '/definitely/missing.mmdb') IS NULL AS missing")
        .await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +---------+
    - "| missing |"
    - +---------+
    - "| true    |"
    - +---------+
    "#);
}

#[tokio::test]
async fn test_geoip_unopenable_db_reported_in_error_field() {
    let execution = TestExecution::new().await;
    // A database that cannot be opened does not fail the query: the
    // location fields are NULL and the struct's error field carries the
    // reason
    let output = execution
        .run_and_format(
            "SELECT geoip('1.1.1.1', '/definitely/missing.mmdb')['country_code'] IS NULL AS no_country, \
                    geoip('1.1.1.1', '/definitely/missing.mmdb')['error'] LIKE '%failed to open database%' AS reported",
        )
        .await;
    insta::assert_yaml_snapshot!(output, @r#"
    - +------------+----------+
    - "| no_country | reported |"
    - +------------+----------+
    - "| true       | true     |"
    - +------------+----------+
    "#);
}

#[tokio::test]
async fn test_geoip_db_path_from_config() {
    // The GEOIP_DB environment variable takes precedence over the config
    // value, so the config path is only observable when it is not set
    if std::env::var_os(datafusion_net::GEOIP_DB_ENV_VAR).is_some() {
        return;
    }
    let mut config = datafusion_dft::config::AppConfig::default();
    let db_path = "/config/provided/db.mmdb";
    config.cli.execution.net.geoip_db_path = Some(db_path.into());
    let execution = TestExecution::new_with_config(config).await;
    // The configured path reaches the single-argument form of geoip: the
    // error field reports a failure to open that (nonexistent) database
    // rather than complaining that no database is configured
    let batches = execution
        .run("SELECT geoip('1.1.1.1')['error'] AS error")
        .await
        .unwrap();
    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(
        formatted.contains(&format!("geoip failed to open database '{db_path}'")),
        "unexpected output: {formatted}"
    );
}

#[tokio::test]
async fn test_reverse_dns_over_pcap() {
    let dir = tempfile::tempdir().unwrap();
    write_test_pcap(&dir.path().join("test.pcap"));
    let execution = TestExecution::new().await;
    // reverse_dns composes with the pcap table function over the src_ip
    // column. The 10.0.0.0/8 addresses in the fixture will not resolve, but
    // the query must plan and execute successfully.
    let sql = format!(
        "SELECT DISTINCT src_ip, reverse_dns(src_ip) AS host FROM pcap('{}/test.pcap')",
        dir.path().display()
    );
    let result = execution.run(&sql).await;
    assert!(result.is_ok());
}

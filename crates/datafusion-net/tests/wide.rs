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

//! End-to-end tests for the `pcap_wide` table function: a capture written
//! with [`datafusion_net::writer`] is queried back with geolocation
//! enrichment resolved against the minimal MaxMind DB written by the shared
//! [`common`] test support (`1.1.1.0/24` maps to Sydney, Australia)

use std::{path::Path, sync::Arc};

use datafusion::prelude::SessionContext;
use datafusion_net::{writer::PcapWriter, PcapWideFunc};
use etherparse::PacketBuilder;

mod common;
use common::write_test_mmdb;

const SRC_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x01];
const DST_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x02];
/// 2025-01-01T00:00:00Z
const BASE_TS_MICROS: i64 = 1_735_689_600_000_000;

/// Writes a capture with one packet from 1.1.1.1 (in the test geolocation
/// database) to 9.9.9.9 (absent from it)
fn write_test_pcap(path: &Path) {
    let file = std::fs::File::create(path).unwrap();
    let mut writer = PcapWriter::new(file, 1).unwrap();
    let builder = PacketBuilder::ethernet2(SRC_MAC, DST_MAC)
        .ipv4([1, 1, 1, 1], [9, 9, 9, 9], 64)
        .tcp(51000, 443, 1000, 1024)
        .syn();
    let mut frame = Vec::with_capacity(builder.size(0));
    builder.write(&mut frame, &[]).unwrap();
    writer.write_packet(BASE_TS_MICROS, &frame).unwrap();
    writer.flush().unwrap();
}

/// Returns a context with `pcap_wide` registered (against the test
/// geolocation database when `with_geoip_db`) plus the tempdir and the
/// capture path
fn setup(with_geoip_db: bool) -> (SessionContext, tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let pcap_path = dir.path().join("test.pcap");
    write_test_pcap(&pcap_path);
    let geoip_db = with_geoip_db.then(|| {
        let db = dir.path().join("test.mmdb");
        write_test_mmdb(&db);
        db
    });
    let ctx = SessionContext::new();
    ctx.register_udtf("pcap_wide", Arc::new(PcapWideFunc::new(geoip_db)));
    let pcap_path = pcap_path.display().to_string();
    (ctx, dir, pcap_path)
}

async fn run(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn test_pcap_wide_geolocates_addresses() {
    let (ctx, _dir, pcap) = setup(true);
    let sql = format!(
        "SELECT src_ip, src_country, src_city, src_lat, src_lon, \
                dst_ip, dst_country \
         FROM pcap_wide('{pcap}')"
    );
    let out = run(&ctx, &sql).await;
    assert!(out.contains("1.1.1.1"), "missing src_ip in:\n{out}");
    assert!(out.contains("AU"), "missing src_country in:\n{out}");
    assert!(out.contains("Sydney"), "missing src_city in:\n{out}");
    assert!(out.contains("-33.8688"), "missing src_lat in:\n{out}");
    assert!(out.contains("151.2093"), "missing src_lon in:\n{out}");
    // 9.9.9.9 is not in the database: dst_country is null
    assert!(
        !out.contains("| AU          | AU"),
        "dst_country should be null in:\n{out}"
    );
}

#[tokio::test]
async fn test_pcap_wide_without_geoip_db_yields_nulls() {
    let (ctx, _dir, pcap) = setup(false);
    let sql = format!(
        "SELECT src_country IS NULL AND src_city IS NULL \
                AND src_lat IS NULL AND src_lon IS NULL \
                AND dst_country IS NULL AS geo_null \
         FROM pcap_wide('{pcap}')"
    );
    let out = run(&ctx, &sql).await;
    assert!(out.contains("true"), "expected null geo columns in:\n{out}");
}

#[tokio::test]
async fn test_pcap_wide_unopenable_db_yields_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let pcap_path = dir.path().join("test.pcap");
    write_test_pcap(&pcap_path);
    let ctx = SessionContext::new();
    // A database path that cannot be opened must not fail the query: the
    // geolocation columns are NULL (the reason is available through
    // geoip(ip, path)['error'])
    ctx.register_udtf(
        "pcap_wide",
        Arc::new(PcapWideFunc::new(Some("/definitely/missing.mmdb".into()))),
    );
    let sql = format!(
        "SELECT src_country IS NULL AND src_city IS NULL \
                AND src_lat IS NULL AND src_lon IS NULL AS geo_null \
         FROM pcap_wide('{}')",
        pcap_path.display()
    );
    let out = run(&ctx, &sql).await;
    assert!(out.contains("true"), "expected null geo columns in:\n{out}");
}

#[tokio::test]
async fn test_pcap_wide_host_columns_present() {
    let (ctx, _dir, pcap) = setup(true);
    // The fixture addresses may or may not have PTR records depending on the
    // environment, so only exercise that the reverse DNS columns project and
    // the query executes
    let sql = format!("SELECT src_ip, src_host, dst_host FROM pcap_wide('{pcap}')");
    let out = run(&ctx, &sql).await;
    assert!(out.contains("src_host"), "missing column in:\n{out}");
    assert!(out.contains("1.1.1.1"), "missing row in:\n{out}");
}

#[tokio::test]
async fn test_pcap_wide_narrow_projection_and_aggregation() {
    let (ctx, _dir, pcap) = setup(true);
    // Enrichment composes with projection pruning and aggregation
    let sql = format!(
        "SELECT src_country, count(*) AS packets \
         FROM pcap_wide('{pcap}') WHERE dst_port = 443 GROUP BY src_country"
    );
    let out = run(&ctx, &sql).await;
    assert!(out.contains("AU"), "missing group in:\n{out}");
    assert!(out.contains('1'), "missing count in:\n{out}");
}

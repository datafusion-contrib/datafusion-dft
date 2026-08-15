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

//! Tests for the `geoip` scalar UDF, run against the minimal MaxMind DB
//! (`.mmdb`) file written by the shared [`common`] test support (the single
//! network `1.1.1.0/24` maps to a Sydney, Australia city record)

use datafusion::{logical_expr::ScalarUDF, prelude::SessionContext};
use datafusion_net::GeoIpUdf;

mod common;
use common::write_test_mmdb;

/// Returns a context with `geoip` registered plus the tempdir holding the
/// test database and the database path
async fn setup() -> (SessionContext, tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("test.mmdb");
    write_test_mmdb(&db);
    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(GeoIpUdf::default()));
    let db = db.display().to_string();
    (ctx, dir, db)
}

async fn run(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn test_geoip_hit() {
    let (ctx, _dir, db) = setup().await;
    let sql = format!(
        "SELECT geoip('1.1.1.1', '{db}')['country_code'] AS country_code, \
                geoip('1.1.1.1', '{db}')['country'] AS country, \
                geoip('1.1.1.1', '{db}')['city'] AS city, \
                geoip('1.1.1.1', '{db}')['latitude'] AS latitude, \
                geoip('1.1.1.1', '{db}')['longitude'] AS longitude, \
                geoip('1.1.1.1', '{db}')['time_zone'] AS time_zone"
    );
    let out = run(&ctx, &sql).await;
    assert!(out.contains("AU"), "missing country code in:\n{out}");
    assert!(out.contains("Australia"), "missing country in:\n{out}");
    assert!(out.contains("Sydney"), "missing city in:\n{out}");
    assert!(out.contains("-33.8688"), "missing latitude in:\n{out}");
    assert!(out.contains("151.2093"), "missing longitude in:\n{out}");
    assert!(
        out.contains("Australia/Sydney"),
        "missing time zone in:\n{out}"
    );

    // The error field is NULL on a successful lookup
    let sql = format!("SELECT geoip('1.1.1.1', '{db}')['error'] IS NULL AS no_error");
    let out = run(&ctx, &sql).await;
    assert!(out.contains("true"), "expected NULL error field in:\n{out}");
}

#[tokio::test]
async fn test_geoip_whole_network_matches() {
    let (ctx, _dir, db) = setup().await;
    // Any address in the encoded 1.1.1.0/24 network hits the same record
    let sql = format!("SELECT geoip('1.1.1.42', '{db}')['city'] = 'Sydney' AS same_record");
    let out = run(&ctx, &sql).await;
    assert!(out.contains("true"), "expected a hit in:\n{out}");
}

#[tokio::test]
async fn test_geoip_misses_are_null() {
    let (ctx, _dir, db) = setup().await;
    // An address outside the network, an unparseable address, a NULL
    // address, and an IPv6 address in an IPv4-only database are all NULL
    let sql = format!(
        "SELECT geoip('9.9.9.9', '{db}') IS NULL AS missing, \
                geoip('not-an-ip', '{db}') IS NULL AS unparseable, \
                geoip(NULL, '{db}') IS NULL AS null_input, \
                geoip('::1', '{db}') IS NULL AS wrong_ip_version"
    );
    let out = run(&ctx, &sql).await;
    // Four columns, all true
    assert_eq!(
        out.matches("true").count(),
        4,
        "expected all NULL in:\n{out}"
    );
}

#[tokio::test]
async fn test_geoip_invalid_ip_does_not_open_db() {
    let (ctx, _dir, _db) = setup().await;
    // The address is parsed before the database path is resolved, so an
    // unparseable address is NULL even when the database does not exist
    let out = run(
        &ctx,
        "SELECT geoip('not-an-ip', '/definitely/missing.mmdb') IS NULL AS missing",
    )
    .await;
    assert!(out.contains("true"), "expected NULL in:\n{out}");
}

#[tokio::test]
async fn test_geoip_unopenable_db_reported_in_error_field() {
    let (ctx, _dir, _db) = setup().await;
    // A database that cannot be opened does not fail the query: the
    // location fields are NULL and the error field carries the reason
    let out = run(
        &ctx,
        "SELECT geoip('1.1.1.1', '/definitely/missing.mmdb')['country_code'] IS NULL AS no_country, \
                geoip('1.1.1.1', '/definitely/missing.mmdb')['error'] AS error",
    )
    .await;
    assert!(out.contains("true"), "expected NULL country in:\n{out}");
    assert!(
        out.contains("geoip failed to open database"),
        "expected open failure in error field in:\n{out}"
    );
    assert!(
        out.contains("missing.mmdb"),
        "expected the path in the error field in:\n{out}"
    );
}

#[tokio::test]
async fn test_geoip_default_db_from_constructor_and_missing_config_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("test.mmdb");
    write_test_mmdb(&db);

    // With a default database the single-argument form works
    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(GeoIpUdf::with_db_path(&db)));
    let out = run(&ctx, "SELECT geoip('1.1.1.1')['city'] AS city").await;
    assert!(out.contains("Sydney"), "expected a hit in:\n{out}");

    // Without one (and without the GEOIP_DB environment variable, which the
    // test environment must not set) the query still succeeds and the error
    // field points at the fix
    if std::env::var_os(datafusion_net::GEOIP_DB_ENV_VAR).is_none() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeoIpUdf::default()));
        let out = run(&ctx, "SELECT geoip('1.1.1.1')['error'] AS error").await;
        assert!(
            out.contains("geoip has no database configured"),
            "expected configuration hint in error field in:\n{out}"
        );
    }
}

#[tokio::test]
async fn test_geoip_over_column() {
    let (ctx, _dir, db) = setup().await;
    // Exercise the array (non-literal) code path over a column of addresses
    let sql = format!(
        "SELECT ip, geoip(ip, '{db}')['country_code'] AS cc \
         FROM (VALUES ('1.1.1.1'), ('1.1.1.200'), ('8.8.8.8'), ('nope')) AS t(ip) \
         ORDER BY ip"
    );
    let out = run(&ctx, &sql).await;
    let hits = out.matches("AU").count();
    assert_eq!(hits, 2, "expected two hits in:\n{out}");
}

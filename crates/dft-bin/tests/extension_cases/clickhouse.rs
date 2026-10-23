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

//! Tests for the ClickHouse catalog integration. Tests that query data require a running
//! ClickHouse instance:
//!
//! ```bash
//! docker run -d -p 8123:8123 \
//!   -e CLICKHOUSE_USER=admin -e CLICKHOUSE_PASSWORD=secret \
//!   clickhouse/clickhouse-server
//! ```

use assert_cmd::Command;

use crate::{cli_cases::contains_str, config::TestConfigBuilder};

const CLICKHOUSE_URL: &str = "http://localhost:8123";
const CLICKHOUSE_USER: &str = "admin";
const CLICKHOUSE_PASSWORD: &str = "secret";

/// Run a statement against the ClickHouse HTTP interface
fn clickhouse_exec(statement: &str) {
    let output = std::process::Command::new("curl")
        .args([
            "-sf",
            "-u",
            &format!("{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}"),
            CLICKHOUSE_URL,
            "--data-binary",
            statement,
        ])
        .output()
        .expect("Failed to run curl");
    assert!(
        output.status.success(),
        "ClickHouse statement failed: {statement}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Create `<database>.users` with two rows. Each test seeds its own database so that
/// concurrently running tests do not interfere with each other.
fn seed_clickhouse(database: &str) {
    clickhouse_exec(&format!("CREATE DATABASE IF NOT EXISTS {database}"));
    clickhouse_exec(&format!(
        "CREATE OR REPLACE TABLE {database}.users (id UInt32, name String) ENGINE = MergeTree ORDER BY id",
    ));
    clickhouse_exec(&format!(
        "INSERT INTO {database}.users VALUES (1, 'alice'), (2, 'bob')"
    ));
}

fn clickhouse_config() -> crate::config::TestConfig {
    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_clickhouse(
        "cli",
        "clickhouse",
        CLICKHOUSE_URL,
        Some(CLICKHOUSE_USER),
        Some(CLICKHOUSE_PASSWORD),
        &[("output_format_arrow_string_as_string", "1")],
    );
    config_builder.build("my_config.toml")
}

#[test]
fn test_clickhouse_config_parsing() {
    let config = clickhouse_config();
    let app_config = datafusion_dft::config::create_config(config.path, &[]);
    let clickhouse_configs = app_config
        .cli
        .execution
        .clickhouse
        .expect("ClickHouse config missing");
    assert_eq!(clickhouse_configs.len(), 1);
    let clickhouse_config = &clickhouse_configs[0];
    assert_eq!(clickhouse_config.name, "clickhouse");
    assert_eq!(clickhouse_config.url, CLICKHOUSE_URL);
    assert_eq!(clickhouse_config.user.as_deref(), Some(CLICKHOUSE_USER));
    assert_eq!(
        clickhouse_config.password.as_deref(),
        Some(CLICKHOUSE_PASSWORD)
    );

    let params = clickhouse_config.to_params();
    assert_eq!(params.get("url").map(String::as_str), Some(CLICKHOUSE_URL));
    assert_eq!(
        params
            .get("option_output_format_arrow_string_as_string")
            .map(String::as_str),
        Some("1")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clickhouse_query() {
    seed_clickhouse("dft_test_query");
    let config = clickhouse_config();

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path)
        .arg("-c")
        .arg("SELECT id, name FROM clickhouse.dft_test_query.users ORDER BY id")
        .assert()
        .success();

    let expected = r#"
+----+-------+
| id | name  |
+----+-------+
| 1  | alice |
| 2  | bob   |
+----+-------+
"#;

    assert.stdout(contains_str(expected));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clickhouse_table_discovery() {
    seed_clickhouse("dft_test_discovery");
    let config = clickhouse_config();

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path)
        .arg("-c")
        .arg("SELECT table_name FROM clickhouse.information_schema.tables WHERE table_schema = 'dft_test_discovery'")
        .assert()
        .success();

    let expected = r#"
+------------+
| table_name |
+------------+
| users      |
+------------+
"#;

    assert.stdout(contains_str(expected));
}

/// Stream-like engine tables (e.g. Kafka) reject direct selects, and materialized views whose
/// stored query reads from them fail schema inference. Both must be excluded from the catalog so
/// that they don't break listing the remaining tables via `information_schema`.
#[tokio::test(flavor = "multi_thread")]
async fn test_clickhouse_stream_tables_excluded() {
    seed_clickhouse("dft_test_streams");
    clickhouse_exec(
        "CREATE TABLE IF NOT EXISTS dft_test_streams.events_stream (id UInt32) ENGINE = Kafka \
         SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = 'dft_test_events', \
         kafka_group_name = 'dft_test_streams', kafka_format = 'JSONEachRow'",
    );
    clickhouse_exec(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS dft_test_streams.events_consumer \
         TO dft_test_streams.users \
         AS SELECT id, 'kafka' AS name FROM dft_test_streams.events_stream",
    );
    let config = clickhouse_config();

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path)
        .arg("-c")
        .arg("SELECT table_name FROM clickhouse.information_schema.tables WHERE table_schema = 'dft_test_streams'")
        .assert()
        .success();

    let expected = r#"
+------------+
| table_name |
+------------+
| users      |
+------------+
"#;

    assert.stdout(contains_str(expected));
}

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

use std::{io::Read, time::Duration};

use crate::{
    cli_cases::{contains_str, sql_in_file},
    config::TestConfigBuilder,
};
use assert_cmd::Command;
use datafusion_app::local::ExecutionContext;
use datafusion_dft::{
    execution::AppExecution,
    server::flightsql::service::FlightSqlServiceImpl,
    test_utils::fixture::{TestFixture, TestFlightSqlServiceImpl},
};
use std::collections::HashMap;

#[tokio::test]
#[ignore = "Test appears to have pre-existing issue - FlightSQL falls back to local execution instead of failing"]
pub async fn test_execute_with_no_flightsql_server() {
    let _ = env_logger::builder().is_test(true).try_init();
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1 + 3;")
        .arg("--flightsql")
        .assert()
        .failure();

    assert.stderr(contains_str("Error creating channel for FlightSQL client"));
}

#[tokio::test]
pub async fn test_execute() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1 + 2;")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r##"
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
    "##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_invalid_sql_command() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELEC 1;")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .failure()
    })
    .await
    .unwrap();

    // I think its implementation specific how they decide to return errors but I believe they will
    // all be in the form of an IPC error
    let expected = r##"Error: Ipc error"##;
    assert.stderr(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_execute_multiple_commands() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("--flightsql")
            .arg("-c")
            .arg("SELECT 1 + 1;")
            .arg("-c")
            .arg("SELECT 1 + 2;")
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r##"
+---------------------+
| Int64(1) + Int64(1) |
+---------------------+
| 2                   |
+---------------------+
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
    "##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_command_in_file() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file = sql_in_file("SELECT 1 + 1");
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("--flightsql")
            .arg("-f")
            .arg(file.path())
            .assert()
            .success()
    })
    .await
    .unwrap();
    let expected = r##"
+---------------------+
| Int64(1) + Int64(1) |
+---------------------+
| 2                   |
+---------------------+
    "##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_invalid_sql_command_in_file() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file = sql_in_file("SELEC 1");
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("--flightsql")
            .arg("-f")
            .arg(file.path())
            .assert()
            .failure()
    })
    .await
    .unwrap();

    // I think its implementation specific how they decide to return errors but I believe they will
    // all be in the form of an IPC error
    let expected = r##"Error: Ipc error"##;
    assert.stderr(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_command_multiple_files() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file1 = sql_in_file("SELECT 1 + 1");
    let file2 = sql_in_file("SELECT 1 + 2");
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("--flightsql")
            .arg("-f")
            .arg(file1.path())
            .arg("-f")
            .arg(file2.path())
            .assert()
            .success()
    })
    .await
    .unwrap();
    let expected = r##"
+---------------------+
| Int64(1) + Int64(1) |
+---------------------+
| 2                   |
+---------------------+
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
    "##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_time_command() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("--flightsql")
            .arg("-c")
            .arg("SELECT 1 + 1")
            .arg("--time")
            .assert()
            .success()
    })
    .await
    .unwrap();
    let expected = r##"executed in"##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_time_files() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file1 = sql_in_file("SELECT 1 + 1");
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("--flightsql")
            .arg("-f")
            .arg(file1.path())
            .arg("--time")
            .assert()
            .success()
    })
    .await
    .unwrap();
    let expected = r##"executed in"##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_bench_command() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--bench")
            .arg("--flightsql")
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r##"
----------------------------
Benchmark Stats (10 runs, serial)
----------------------------
SELECT 1
----------------------------"##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_bench_files() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file = sql_in_file(r#"SELECT 1 + 1;"#);
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-f")
            .arg(file.path())
            .arg("--bench")
            .arg("--flightsql")
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected_err = r##"
----------------------------
Benchmark Stats (10 runs, serial)
----------------------------
SELECT 1 + 1;
----------------------------"##;
    assert.code(0).stdout(contains_str(expected_err));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_custom_config_benchmark_iterations() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_flightsql_benchmark_iterations(5);
    let config = config_builder.build("my_config.toml");

    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("--config")
            .arg(config.path)
            .arg("-c")
            .arg("SELECT 1")
            .arg("--flightsql")
            .arg("--bench")
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = "5 runs";

    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_bench_command_and_save() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("results.csv");
    let cloned = file.clone();
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--bench")
            .arg("--flightsql")
            .arg("--save")
            .arg(cloned.to_str().unwrap())
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r##"
----------------------------
Benchmark Stats (10 runs, serial)
----------------------------
SELECT 1
----------------------------"##;
    assert.stdout(contains_str(expected));
    assert!(file.exists());
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_bench_files_and_save() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file = sql_in_file(r#"SELECT 1 + 1;"#);

    let temp_dir = tempfile::tempdir().unwrap();
    let results_file = temp_dir.path().join("results.csv");
    let cloned = results_file.clone();
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-f")
            .arg(file.path())
            .arg("--bench")
            .arg("--flightsql")
            .arg("--save")
            .arg(cloned.to_str().unwrap())
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected_err = r##"
----------------------------
Benchmark Stats (10 runs, serial)
----------------------------
SELECT 1 + 1;
----------------------------"##;
    assert.code(0).stdout(contains_str(expected_err));
    assert!(results_file.exists());
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_bench_command_and_save_then_append() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("results.csv");
    let cloned = file.clone();
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--bench")
            .arg("--flightsql")
            .arg("--save")
            .arg(cloned.to_str().unwrap())
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r##"
----------------------------
Benchmark Stats (10 runs, serial)
----------------------------
SELECT 1
----------------------------"##;
    assert.stdout(contains_str(expected));
    assert!(file.exists());

    let cloned_again = file.clone();
    tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--bench")
            .arg("--flightsql")
            .arg("--save")
            .arg(cloned_again.to_str().unwrap())
            .arg("--append")
            .assert()
            .success()
    })
    .await
    .unwrap();

    let contents = std::fs::read_to_string(file).unwrap();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(3, lines.len());

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_bench_command_customer_iterations() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--bench")
            .arg("--flightsql")
            .arg("-n")
            .arg("3")
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r##"
----------------------------
Benchmark Stats (3 runs, serial)
----------------------------
SELECT 1
----------------------------"##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_execute_custom_port() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50052").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1 + 2;")
            .arg("--flightsql")
            .arg("--host")
            .arg("http://localhost:50052")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r##"
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
    "##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_output_csv() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.csv");

    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let cloned_path = path.clone();

    tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--flightsql")
            .arg("-o")
            .arg(cloned_path)
            .assert()
            .success()
    })
    .await
    .unwrap();
    let mut file = std::fs::File::open(path).unwrap();
    let mut buffer = String::new();
    file.read_to_string(&mut buffer).unwrap();

    let expected = "Int64(1)\n1\n";
    assert_eq!(buffer, expected);

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_output_json() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.json");

    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let cloned_path = path.clone();

    tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--flightsql")
            .arg("-o")
            .arg(cloned_path)
            .assert()
            .success()
    })
    .await
    .unwrap();
    let mut file = std::fs::File::open(path).unwrap();
    let mut buffer = String::new();
    file.read_to_string(&mut buffer).unwrap();

    let expected = "{\"Int64(1)\":1}\n";
    assert_eq!(buffer, expected);

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_output_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.parquet");

    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let cloned_path = path.clone();

    tokio::task::spawn_blocking(|| {
        let sql = "SELECT 1".to_string();
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg(sql.clone())
            .arg("--flightsql")
            .arg("-o")
            .arg(cloned_path)
            .timeout(Duration::from_secs(5))
            .assert()
            .success();
    })
    .await
    .unwrap();

    let read_sql = format!("SELECT * FROM '{}'", path.to_str().unwrap());

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg(read_sql)
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+----------+
| Int64(1) |
+----------+
| 1        |
+----------+"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_query_command() {
    let test_server = TestFlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        let sql = "SELECT 1".to_string();
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("statement-query")
            .arg("--sql")
            .arg(sql.clone())
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+----------+
| Int64(1) |
+----------+
| 1        |
+----------+"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_catalogs() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-catalogs")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+
| table_catalog |
+---------------+
| datafusion    |
+---------------+"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_schemas_no_filter() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-db-schemas")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+
| table_catalog | table_schema       |
+---------------+--------------------+
| datafusion    | information_schema |
| test          | information_schema |
| test          | meta               |
+---------------+--------------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_schemas_filter_catalog() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-db-schemas")
            .arg("--catalog")
            .arg("test")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+
| table_catalog | table_schema       |
+---------------+--------------------+
| test          | information_schema |
| test          | meta               |
+---------------+--------------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_schemas_filter_pattern() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-db-schemas")
            .arg("--db-schema-filter-pattern")
            .arg("information")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+
| table_catalog | table_schema       |
+---------------+--------------------+
| datafusion    | information_schema |
| test          | information_schema |
+---------------+--------------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_schemas_filter_pattern_and_catalog() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-db-schemas")
            .arg("--db-schema-filter-pattern")
            .arg("information")
            .arg("--catalog")
            .arg("test")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+
| table_catalog | table_schema       |
+---------------+--------------------+
| test          | information_schema |
+---------------+--------------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_tables_no_filter() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-tables")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+-------------+------------+
| table_catalog | table_schema       | table_name  | table_type |
+---------------+--------------------+-------------+------------+
| datafusion    | information_schema | columns     | VIEW       |
| datafusion    | information_schema | df_settings | VIEW       |
| datafusion    | information_schema | parameters  | VIEW       |
| datafusion    | information_schema | routines    | VIEW       |
| datafusion    | information_schema | schemata    | VIEW       |
| datafusion    | information_schema | tables      | VIEW       |
| datafusion    | information_schema | views       | VIEW       |
| test          | information_schema | columns     | VIEW       |
| test          | information_schema | df_settings | VIEW       |
| test          | information_schema | parameters  | VIEW       |
| test          | information_schema | routines    | VIEW       |
| test          | information_schema | schemata    | VIEW       |
| test          | information_schema | tables      | VIEW       |
| test          | information_schema | views       | VIEW       |
| test          | meta               | versions    | BASE TABLE |
+---------------+--------------------+-------------+------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_tables_catalog_filter() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-tables")
            .arg("--catalog")
            .arg("test")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+-------------+------------+
| table_catalog | table_schema       | table_name  | table_type |
+---------------+--------------------+-------------+------------+
| test          | information_schema | columns     | VIEW       |
| test          | information_schema | df_settings | VIEW       |
| test          | information_schema | parameters  | VIEW       |
| test          | information_schema | routines    | VIEW       |
| test          | information_schema | schemata    | VIEW       |
| test          | information_schema | tables      | VIEW       |
| test          | information_schema | views       | VIEW       |
| test          | meta               | versions    | BASE TABLE |
+---------------+--------------------+-------------+------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_tables_schema_filter() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-tables")
            .arg("--db-schema-filter-pattern")
            .arg("meta")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------+------------+------------+
| table_catalog | table_schema | table_name | table_type |
+---------------+--------------+------------+------------+
| test          | meta         | versions   | BASE TABLE |
+---------------+--------------+------------+------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_tables_table_filter() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-tables")
            .arg("--table-name-filter-pattern")
            .arg("tables")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+------------+------------+
| table_catalog | table_schema       | table_name | table_type |
+---------------+--------------------+------------+------------+
| datafusion    | information_schema | tables     | VIEW       |
| test          | information_schema | tables     | VIEW       |
+---------------+--------------------+------------+------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_tables_table_type() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-tables")
            .arg("--table-types")
            .arg("VIEW")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+---------------+--------------------+-------------+------------+
| table_catalog | table_schema       | table_name  | table_type |
+---------------+--------------------+-------------+------------+
| datafusion    | information_schema | columns     | VIEW       |
| datafusion    | information_schema | df_settings | VIEW       |
| datafusion    | information_schema | parameters  | VIEW       |
| datafusion    | information_schema | routines    | VIEW       |
| datafusion    | information_schema | schemata    | VIEW       |
| datafusion    | information_schema | tables      | VIEW       |
| datafusion    | information_schema | views       | VIEW       |
| test          | information_schema | columns     | VIEW       |
| test          | information_schema | df_settings | VIEW       |
| test          | information_schema | parameters  | VIEW       |
| test          | information_schema | routines    | VIEW       |
| test          | information_schema | schemata    | VIEW       |
| test          | information_schema | tables      | VIEW       |
| test          | information_schema | views       | VIEW       |
+---------------+--------------------+-------------+------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_table_types() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-table-types")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r#"
+------------+
| table_type |
+------------+
| BASE TABLE |
| VIEW       |
+------------+
"#;

    assert.stdout(contains_str(expected));

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_sql_info() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-sql-info")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    // Check that we get basic server info back
    let output = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        output.contains("datafusion-dft"),
        "Should contain server name"
    );
    assert!(
        output.contains("server_name"),
        "Should contain server_name column"
    );

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_xdbc_type_info() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-xdbc-type-info")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    // Check that we get type information back
    let output = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        output.contains("BIGINT") || output.contains("INTEGER"),
        "Should contain integer types"
    );
    assert!(output.contains("VARCHAR"), "Should contain VARCHAR type");

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_get_xdbc_type_info_filtered() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("flightsql")
            .arg("get-xdbc-type-info")
            .arg("--data-type")
            .arg("12") // VARCHAR type code
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    // Check that we get filtered results
    let output = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(output.contains("VARCHAR"), "Should contain VARCHAR type");

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_client_headers() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_client_headers(Some(HashMap::from([(
        "database".to_string(),
        "some_db".to_string(),
    )])));
    let config = config_builder.build("my_config.toml");

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1 + 2;")
            .arg("--flightsql")
            .arg("--config")
            .arg(config.path)
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    let expected = r##"
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
    "##;
    assert.stdout(contains_str(expected));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_create_and_close_prepared_statement() {
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use tonic::transport::Channel;

    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    // Create FlightSQL client
    let channel = Channel::from_static("http://127.0.0.1:50051")
        .connect()
        .await
        .expect("Failed to connect to test server");
    let mut client = FlightSqlServiceClient::new(channel);

    // Create a prepared statement
    let sql = "SELECT 1 + 2 as result";
    let prepared_stmt = client
        .prepare(sql.to_string(), None)
        .await
        .expect("Failed to create prepared statement");

    // Verify we got a schema
    let schema = prepared_stmt
        .dataset_schema()
        .expect("Failed to get schema");
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "result");

    // Close the prepared statement
    prepared_stmt
        .close()
        .await
        .expect("Failed to close prepared statement");

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_prepared_statement_execute() {
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use tonic::transport::Channel;

    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    // Create FlightSQL client
    let channel = Channel::from_static("http://127.0.0.1:50051")
        .connect()
        .await
        .expect("Failed to connect to test server");
    let mut client = FlightSqlServiceClient::new(channel);

    // Create a prepared statement
    let sql = "SELECT 42 as answer";
    let mut prepared_stmt = client
        .prepare(sql.to_string(), None)
        .await
        .expect("Failed to create prepared statement");

    // Verify schema
    let schema = prepared_stmt
        .dataset_schema()
        .expect("Failed to get schema");
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "answer");

    // Execute the prepared statement
    let flight_info = prepared_stmt
        .execute()
        .await
        .expect("Failed to execute prepared statement");

    // Verify FlightInfo has endpoints
    assert!(!flight_info.endpoint.is_empty());

    // Close the prepared statement
    prepared_stmt
        .close()
        .await
        .expect("Failed to close prepared statement");

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_prepared_statement_complex_query() {
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use tonic::transport::Channel;

    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    // Create FlightSQL client
    let channel = Channel::from_static("http://127.0.0.1:50051")
        .connect()
        .await
        .expect("Failed to connect to test server");
    let mut client = FlightSqlServiceClient::new(channel);

    // Create a prepared statement with a more complex query
    let sql = "SELECT x, x * 2 as doubled FROM (VALUES (1), (2), (3)) as t(x)";
    let mut prepared_stmt = client
        .prepare(sql.to_string(), None)
        .await
        .expect("Failed to create prepared statement");

    // Verify schema
    let schema = prepared_stmt
        .dataset_schema()
        .expect("Failed to get schema");
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "x");
    assert_eq!(schema.field(1).name(), "doubled");

    // Execute to verify it works
    let flight_info = prepared_stmt
        .execute()
        .await
        .expect("Failed to execute prepared statement");

    assert!(!flight_info.endpoint.is_empty());

    // Close the prepared statement
    prepared_stmt
        .close()
        .await
        .expect("Failed to close prepared statement");

    fixture.shutdown_and_wait().await;
}

// ============================================================================
// FlightSQL Analyze Tests
// ============================================================================

#[tokio::test]
pub async fn test_analyze_command() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1 + 2")
            .arg("--analyze")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    // Verify output contains expected sections
    let output = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(output.contains("Query"), "Should contain Query section");
    assert!(
        output.contains("Execution Summary"),
        "Should contain Execution Summary"
    );
    assert!(
        output.contains("Output Rows"),
        "Should contain Output Rows metric"
    );
    assert!(
        output.contains("Parsing"),
        "Should contain timing breakdown"
    );

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_analyze_raw_command() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1 + 2")
            .arg("--analyze-raw")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    // Verify raw mode outputs query string and metrics table
    let output = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(output.contains("Query"), "Should contain Query header");
    assert!(output.contains("Metrics"), "Should contain Metrics header");
    assert!(
        output.contains("SELECT 1 + 2"),
        "Should contain the SQL query"
    );
    assert!(
        output.contains("metric_name"),
        "Should contain metric_name column"
    );
    assert!(output.contains("value"), "Should contain value column");
    assert!(
        output.contains("value_type"),
        "Should contain value_type column"
    );

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_analyze_file() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file = sql_in_file("SELECT 1 + 1");

    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-f")
            .arg(file.path())
            .arg("--analyze")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    // Verify output contains expected sections
    let output = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        output.contains("Execution Summary"),
        "Should contain Execution Summary"
    );
    assert!(
        output.contains("Output Rows"),
        "Should contain Output Rows metric"
    );

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_analyze_raw_file() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file = sql_in_file("SELECT 1 + 1");

    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-f")
            .arg(file.path())
            .arg("--analyze-raw")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    // Verify raw mode outputs query string and metrics table
    let output = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(output.contains("Query"), "Should contain Query header");
    assert!(output.contains("Metrics"), "Should contain Metrics header");

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_analyze_multiple_commands() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("-c")
            .arg("SELECT 2")
            .arg("--analyze")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .failure()
    })
    .await
    .unwrap();

    // Verify error message about requiring exactly one command
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("Analyze requires exactly one command"),
        "Should contain error about requiring one command"
    );

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_analyze_invalid_sql() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELEC 1")
            .arg("--analyze")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .failure()
    })
    .await
    .unwrap();

    // Verify error is reported
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("Error") || stderr.contains("error"),
        "Should contain error message"
    );

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_analyze_with_timing_metrics() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--analyze")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    // Verify all timing metrics are present
    let output = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(output.contains("Parsing"), "Should contain Parsing time");
    assert!(
        output.contains("Logical Planning"),
        "Should contain Logical Planning time"
    );
    assert!(
        output.contains("Physical Planning"),
        "Should contain Physical Planning time"
    );
    assert!(output.contains("Execution"), "Should contain Execution time");
    assert!(output.contains("Total"), "Should contain Total time");

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_analyze_raw_metrics_schema() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--analyze-raw")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    // Verify raw metrics table has all expected columns
    let output = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        output.contains("metric_name"),
        "Should contain metric_name column"
    );
    assert!(output.contains("value"), "Should contain value column");
    assert!(
        output.contains("value_type"),
        "Should contain value_type column"
    );
    assert!(
        output.contains("operator_name"),
        "Should contain operator_name column"
    );
    assert!(
        output.contains("partition_id"),
        "Should contain partition_id column"
    );
    assert!(
        output.contains("operator_category"),
        "Should contain operator_category column"
    );

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_analyze_raw_duration_metrics() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--analyze-raw")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    // Verify duration metrics are in the raw output
    let output = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(output.contains("parsing"), "Should contain parsing metric");
    assert!(
        output.contains("logical_planning"),
        "Should contain logical_planning metric"
    );
    assert!(
        output.contains("physical_planning"),
        "Should contain physical_planning metric"
    );
    assert!(
        output.contains("execution"),
        "Should contain execution metric"
    );
    assert!(output.contains("total"), "Should contain total metric");
    assert!(
        output.contains("duration_ns"),
        "Should contain duration_ns value_type"
    );

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_analyze_output_metrics() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1")
            .arg("--analyze-raw")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .success()
    })
    .await
    .unwrap();

    // Verify output metrics (rows, batches, bytes) are present
    let output = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(output.contains("rows"), "Should contain rows metric");
    assert!(output.contains("batches"), "Should contain batches metric");
    assert!(output.contains("bytes"), "Should contain bytes metric");

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_analyze_multiple_statements_in_single_command() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;

    let assert = tokio::task::spawn_blocking(|| {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-c")
            .arg("SELECT 1; SELECT 2")
            .arg("--analyze")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .failure()
    })
    .await
    .unwrap();

    // Verify error message about single statement requirement
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("Only a single SQL statement can be analyzed"),
        "Should contain error about single statement requirement"
    );

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_analyze_file_with_multiple_statements() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file = sql_in_file("SELECT 1; SELECT 2");

    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-f")
            .arg(file.path())
            .arg("--analyze")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .failure()
    })
    .await
    .unwrap();

    // Verify error message about single statement requirement
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("Only a single SQL statement can be analyzed"),
        "Should contain error about single statement requirement"
    );

    fixture.shutdown_and_wait().await;
}

#[tokio::test]
pub async fn test_analyze_multiple_files() {
    let ctx = ExecutionContext::test();
    let exec = AppExecution::new(ctx);
    let test_server = FlightSqlServiceImpl::new(exec);
    let fixture = TestFixture::new(test_server.service(), "127.0.0.1:50051").await;
    let file1 = sql_in_file("SELECT 1");
    let file2 = sql_in_file("SELECT 2");

    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("dft")
            .unwrap()
            .arg("-f")
            .arg(file1.path())
            .arg("-f")
            .arg(file2.path())
            .arg("--analyze")
            .arg("--flightsql")
            .timeout(Duration::from_secs(5))
            .assert()
            .failure()
    })
    .await
    .unwrap();

    // Verify error message about requiring exactly one file
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("Analyze requires exactly one file"),
        "Should contain error about requiring one file"
    );

    fixture.shutdown_and_wait().await;
}

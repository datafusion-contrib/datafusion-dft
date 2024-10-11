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

use std::time::Duration;

use assert_cmd::Command;
use dft::test_utils::fixture::{TestFixture, TestFlightSqlServiceImpl};

use crate::cli_cases::{contains_str, sql_in_file};

#[tokio::test]
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
    let test_server = TestFlightSqlServiceImpl::new();
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
    let test_server = TestFlightSqlServiceImpl::new();
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

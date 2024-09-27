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

use assert_cmd::Command;
use dft::test_utils::fixture::{FlightSqlServiceImpl, TestFixture};

use crate::cli_cases::contains_str;

#[tokio::test]
pub async fn test_execute_with_no_flightsql_server() {
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1 + 2;")
        .arg("--flightsql")
        .assert()
        .failure();

    assert.stderr(contains_str("Error creating channel for FlightSQL client"));
}

#[tokio::test]
pub async fn test_execute() {
    let test_server = FlightSqlServiceImpl::new();
    // let ts = TestFlightServer::new();
    println!("Created test server");
    let fixture = TestFixture::new(test_server.service(), "localhost:50051").await;
    println!("Created test fixture");
    // let channel = fixture.channel().await;

    let expected = r##"
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
    "##;
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1 + 2;")
        .arg("--flightsql")
        .assert()
        .success();

    assert.stdout(contains_str(expected));
}

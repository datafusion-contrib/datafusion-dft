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
use std::{net::TcpListener, time::Duration};

use tokio::process::Command as TokioCommand;

use crate::cli_cases::contains_str;

#[tokio::test]
pub async fn test_flightsql_custom_host() {
    let bin = assert_cmd::cargo::cargo_bin("dft");

    // Bind to port 0 to have the OS assign an available port.
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to a random port");
    let port = listener.local_addr().unwrap().port();
    // Drop the listener so that the port becomes available for the test server.
    drop(listener);

    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to a random port");
    let metrics_port = listener.local_addr().unwrap().port();
    // Drop the listener so that the port becomes available for the test server.
    drop(listener);

    // Seems like we need to assign variable for server not to drop and kill prematurely
    let _server = TokioCommand::new(bin)
        .arg("serve-flightsql")
        .arg("--port")
        .arg(format!("{port}"))
        .arg("--metrics-port")
        .arg(format!("{metrics_port}"))
        .kill_on_drop(true)
        .spawn()
        .expect("Failed to spawn the server");

    // Give the server time to start
    tokio::time::sleep(Duration::from_secs(3)).await;

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1")
        .arg("--flightsql")
        .arg("--host")
        .arg(format!("http://localhost:{}", port))
        .assert()
        .success();

    let expected = r#"
+----------+
| Int64(1) |
+----------+
| 1        |
+----------+"#;

    assert.stdout(contains_str(expected));
}

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

use std::net::TcpListener;
use std::process::Command as StdCommand;
use std::time::Duration;
use tokio::process::Command as TokioCommand;

#[tokio::test]
pub async fn test_http_custom_host() {
    let bin = assert_cmd::cargo::cargo_bin("dft");

    // Bind to port 0 to have the OS assign an available port.
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to a random port");
    let port = listener.local_addr().unwrap().port();
    // Drop the listener so that the port becomes available for the test server.
    drop(listener);

    // Create the custom host using the chosen port.
    let custom_host = format!("127.0.0.1:{}", port);

    // Seems like we need to assign variable for server not to drop and kill prematurely
    let _server = TokioCommand::new(bin)
        .env("RUST_LOG", "off")
        .arg("serve-http")
        .arg("--host")
        .arg(&custom_host)
        .kill_on_drop(true)
        .spawn()
        .expect("Failed to spawn server");

    // Give the server time to start
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Test connection to the custom host
    let response = StdCommand::new("curl")
        .arg(format!("http://{}", custom_host))
        .output()
        .unwrap();

    assert!(
        String::from_utf8_lossy(&response.stdout).contains("Hello, from DFT!"),
        "Server should respond on custom host"
    );
}

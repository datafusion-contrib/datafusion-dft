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

//! Tests for the `websocket` table function

use std::net::SocketAddr;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use crate::extension_cases::TestExecution;

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Spawns a mock WebSocket server that waits for a subscription message,
/// sends `n_messages` text messages, then either closes the connection or
/// holds it open indefinitely.
async fn spawn_mock_ws_server(n_messages: usize, close_after: bool) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let sub = ws.next().await;
        assert!(matches!(sub, Some(Ok(Message::Text(_)))));
        for i in 0..n_messages {
            ws.send(Message::text(format!("msg-{i}"))).await.unwrap();
        }
        if close_after {
            let _ = ws.close(None).await;
        } else {
            futures::future::pending::<()>().await;
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread")]
async fn test_websocket_close_frame_ends_stream() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let addr = spawn_mock_ws_server(3, true).await;
        let execution = TestExecution::new().await;
        let sql = format!("SELECT message FROM websocket('ws://{addr}', 'subscribe')");
        let output = execution.run_and_format(&sql).await;
        insta::assert_yaml_snapshot!(output, @r#"
        - +---------+
        - "| message |"
        - +---------+
        - "| msg-0   |"
        - "| msg-1   |"
        - "| msg-2   |"
        - +---------+
        "#);
    })
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_websocket_limit_stops_unbounded_stream() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let addr = spawn_mock_ws_server(10, false).await;
        let execution = TestExecution::new().await;
        let sql = format!("SELECT message FROM websocket('ws://{addr}', 'subscribe') LIMIT 2");
        let output = execution.run_and_format(&sql).await;
        insta::assert_yaml_snapshot!(output, @r#"
        - +---------+
        - "| message |"
        - +---------+
        - "| msg-0   |"
        - "| msg-1   |"
        - +---------+
        "#);
    })
    .await
    .expect("test timed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_websocket_invalid_url() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let execution = TestExecution::new().await;
        // The invalid URL is rejected during planning, so use the inner
        // execution directly rather than `run`, which panics on plan errors
        let res = execution
            .execution
            .execute_sql("SELECT * FROM websocket('http://example.com')")
            .await;
        let err = match res {
            Ok(_) => panic!("expected plan error"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("'ws://' or 'wss://'"),
            "unexpected error: {err}"
        );
    })
    .await
    .expect("test timed out");
}

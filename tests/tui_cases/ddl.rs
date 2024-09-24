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

use datafusion::assert_batches_eq;
use dft::app::AppEvent;

use crate::TestApp;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_table_ddl() {
    let mut test_app = TestApp::new();

    let ddl = "CREATE TABLE foo AS VALUES (1);";
    test_app
        .handle_app_event(AppEvent::ExecuteDDL(ddl.to_string()))
        .unwrap();
    test_app.wait_for_ddl().await;

    let sql = "SELECT * FROM foo;";
    let batches = test_app.execute_sql(sql).await.unwrap();

    let expected = [
        "+---------+",
        "| column1 |",
        "+---------+",
        "| 1       |",
        "+---------+",
    ];
    assert_batches_eq!(expected, &batches);
}
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

//! Tests for basic execution

use crate::TestExecution;

#[tokio::test]
async fn test_basic_execution() {
    let mut execution = TestExecution::new();

    let actual = execution.run_and_format("SELECT 1+3, 11").await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------------------+-----------+
    - "| Int64(1) + Int64(3) | Int64(11) |"
    - +---------------------+-----------+
    - "| 4                   | 11        |"
    - +---------------------+-----------+
    "###);
}

#[tokio::test]
async fn test_ddl_statements() {
    let mut execution = TestExecution::new()
        .with_setup("CREATE TABLE foo(x int) as VALUES (11), (12), (13)")
        .await;

    let actual = execution.run_and_format("SELECT x, x+1 FROM foo").await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +----+------------------+
    - "| x  | foo.x + Int64(1) |"
    - +----+------------------+
    - "| 11 | 12               |"
    - "| 12 | 13               |"
    - "| 13 | 14               |"
    - +----+------------------+
    "###);
}

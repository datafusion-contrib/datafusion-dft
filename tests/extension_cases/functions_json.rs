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

//! Tests for datafusion-function-json integration

use crate::TestExecution;

static TEST_TABLE: &str = r#"
CREATE TABLE test_table (
    id INT,
    json_col VARCHAR
) AS VALUES
(1, '{}'),
(2, '{ "a": 1 }'),
(3, '{ "a": 2 }'),
(4, '{ "a": 1, "b": 2 }'),
(5, '{ "a": 1, "b": 2, "c": 3 }')
"#;

/// Ensure one of the functions `json_contains` function is properly registered
#[tokio::test]
async fn test_basic() {
    let mut execution = TestExecution::new().with_setup(TEST_TABLE).await;

    let actual = execution
        .run_and_format("SELECT id, json_contains(json_col, 'b') as json_contains FROM test_table")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +----+---------------+
    - "| id | json_contains |"
    - +----+---------------+
    - "| 1  | false         |"
    - "| 2  | false         |"
    - "| 3  | false         |"
    - "| 4  | true          |"
    - "| 5  | true          |"
    - +----+---------------+
    "###);
}

/// ensure the json operators like -> are properly registered
#[tokio::test]
async fn test_operators() {
    let mut execution = TestExecution::new().with_setup(TEST_TABLE).await;

    let actual = execution
        .run_and_format("SELECT id, json_col->'a' as json_col_a FROM test_table")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +----+------------+
    - "| id | json_col_a |"
    - +----+------------+
    - "| 1  | {null=}    |"
    - "| 2  | {int=1}    |"
    - "| 3  | {int=2}    |"
    - "| 4  | {int=1}    |"
    - "| 5  | {int=1}    |"
    - +----+------------+
    "###);
}

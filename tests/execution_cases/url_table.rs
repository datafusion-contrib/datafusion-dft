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

//! Tests for selecting data from local files
use crate::TestExecution;


#[tokio::test]
async fn local_csv_file() {
    let mut execution = TestExecution::new();

    let actual = execution
        .run_and_format("SELECT * from 'data/alltypes_plain.snappy.parquet'")
        .await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+
    - "| id | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col | timestamp_col       |"
    - +----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+
    - "| 6  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30342f30312f3039 | 30         | 2009-04-01T00:00:00 |"
    - "| 7  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30342f30312f3039 | 31         | 2009-04-01T00:01:00 |"
    - +----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+
    "###);
}

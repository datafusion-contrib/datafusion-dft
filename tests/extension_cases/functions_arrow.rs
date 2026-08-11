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

//! Tests for datafusion-functions-arrow integration

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

use crate::extension_cases::TestExecution;

/// Write an Arrow IPC file with 2 batches of 2 rows and footer metadata.
fn write_ipc(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let file = std::fs::File::create(path).unwrap();
    let mut writer = datafusion::arrow::ipc::writer::FileWriter::try_new(file, &schema).unwrap();
    writer.write_metadata("writer", "dft");
    for i in 0..2 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![i * 2, i * 2 + 1])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
}

/// Ensure the arrow file metadata function is registered and returns the summary
#[tokio::test(flavor = "multi_thread")]
async fn test_arrow_file_metadata() {
    let execution = TestExecution::new().await;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.arrow");
    write_ipc(&path);

    let sql = format!(
        "SELECT version, num_batches, num_dictionaries, total_rows, compressed FROM arrow_file_metadata('{}')",
        path.display()
    );
    let actual = execution.run_and_format(&sql).await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------+-------------+------------------+------------+------------+
    - "| version | num_batches | num_dictionaries | total_rows | compressed |"
    - +---------+-------------+------------------+------------+------------+
    - "| V5      | 2           | 0                | 4          | false      |"
    - +---------+-------------+------------------+------------+------------+
    "###);
}

/// Ensure the arrow schema function is registered and returns one row per field
#[tokio::test(flavor = "multi_thread")]
async fn test_arrow_schema() {
    let execution = TestExecution::new().await;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.arrow");
    write_ipc(&path);

    let sql = format!(
        "SELECT field_name, data_type, nullable FROM arrow_schema('{}')",
        path.display()
    );
    let actual = execution.run_and_format(&sql).await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +------------+-----------+----------+
    - "| field_name | data_type | nullable |"
    - +------------+-----------+----------+
    - "| id         | Int32     | false    |"
    - "| name       | Utf8      | true     |"
    - +------------+-----------+----------+
    "###);
}

/// Ensure the arrow metadata function is registered and returns footer kv pairs
#[tokio::test(flavor = "multi_thread")]
async fn test_arrow_metadata() {
    let execution = TestExecution::new().await;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.arrow");
    write_ipc(&path);

    let sql = format!(
        "SELECT key, value FROM arrow_metadata('{}')",
        path.display()
    );
    let actual = execution.run_and_format(&sql).await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +--------+-------+
    - "| key    | value |"
    - +--------+-------+
    - "| writer | dft   |"
    - +--------+-------+
    "###);
}

/// Ensure the arrow batches function is registered and returns one row per batch
#[tokio::test(flavor = "multi_thread")]
async fn test_arrow_batches() {
    let execution = TestExecution::new().await;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.arrow");
    write_ipc(&path);

    let sql = format!(
        "SELECT batch_index, num_rows, compression_codec FROM arrow_batches('{}')",
        path.display()
    );
    let actual = execution.run_and_format(&sql).await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +-------------+----------+-------------------+
    - "| batch_index | num_rows | compression_codec |"
    - +-------------+----------+-------------------+
    - "| 0           | 2        |                   |"
    - "| 1           | 2        |                   |"
    - +-------------+----------+-------------------+
    "###);
}

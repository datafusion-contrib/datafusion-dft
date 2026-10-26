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

//! Tests for the `stdin` table function

use std::sync::Arc;

use assert_cmd::Command;
use datafusion::arrow::{
    array::{Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
    ipc::writer::{FileWriter, StreamWriter},
};
use parquet::arrow::ArrowWriter;

use super::contains_str;

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .unwrap()
}

#[test]
fn test_stdin_csv() {
    Command::cargo_bin("dft")
        .unwrap()
        .args(["-c", "SELECT sum(a), max(b) FROM stdin('csv')"])
        .write_stdin("a,b\n1,x\n2,y\n")
        .assert()
        .success()
        .stdout(contains_str("| 3"))
        .stdout(contains_str("y"));
}

#[test]
fn test_stdin_json() {
    Command::cargo_bin("dft")
        .unwrap()
        .args(["-c", "SELECT sum(a), max(b) FROM stdin('json')"])
        .write_stdin("{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n")
        .assert()
        .success()
        .stdout(contains_str("| 3"))
        .stdout(contains_str("y"));
}

#[test]
fn test_stdin_parquet() {
    let batch = sample_batch();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    Command::cargo_bin("dft")
        .unwrap()
        .args(["-c", "SELECT sum(a), max(b) FROM stdin('parquet')"])
        .write_stdin(buf)
        .assert()
        .success()
        .stdout(contains_str("| 3"))
        .stdout(contains_str("y"));
}

#[test]
fn test_stdin_arrow_file() {
    let batch = sample_batch();
    let mut buf = Vec::new();
    let mut writer = FileWriter::try_new(&mut buf, &batch.schema()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);

    Command::cargo_bin("dft")
        .unwrap()
        .args(["-c", "SELECT sum(a), max(b) FROM stdin('arrow')"])
        .write_stdin(buf)
        .assert()
        .success()
        .stdout(contains_str("| 3"))
        .stdout(contains_str("y"));
}

#[test]
fn test_stdin_arrow_stream() {
    let batch = sample_batch();
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);

    Command::cargo_bin("dft")
        .unwrap()
        .args(["-c", "SELECT sum(a), max(b) FROM stdin('arrow')"])
        .write_stdin(buf)
        .assert()
        .success()
        .stdout(contains_str("| 3"))
        .stdout(contains_str("y"));
}

/// Input larger than the 1MB schema-inference prefix is streamed: the prefix
/// is replayed and the rest read live from stdin
#[test]
fn test_stdin_input_larger_than_inference_prefix() {
    let mut input = String::from("a,b\n");
    for i in 0..200_000 {
        input.push_str(&format!("{i},row-{i}\n"));
    }
    assert!(input.len() > 1024 * 1024);

    Command::cargo_bin("dft")
        .unwrap()
        .args(["-c", "SELECT count(*), sum(a) FROM stdin('csv')"])
        .write_stdin(input)
        .assert()
        .success()
        .stdout(contains_str("| 200000"));
}

/// When the whole input fits in the inference prefix the capture is
/// replayable, so multiple scans (e.g. a self join) work
#[test]
fn test_stdin_self_join_small_input() {
    Command::cargo_bin("dft")
        .unwrap()
        .args([
            "-c",
            "SELECT count(*) FROM stdin('csv') t1 JOIN stdin('csv') t2 ON t1.a = t2.a",
        ])
        .write_stdin("a,b\n1,x\n2,y\n")
        .assert()
        .success()
        .stdout(contains_str("| 2"));
}

/// Input larger than the inference prefix can only be scanned once
#[test]
fn test_stdin_second_scan_errors_on_large_input() {
    let mut input = String::from("a,b\n");
    for i in 0..200_000 {
        input.push_str(&format!("{i},row-{i}\n"));
    }

    Command::cargo_bin("dft")
        .unwrap()
        .args([
            "-c",
            "SELECT count(*) FROM stdin('csv') t1 JOIN stdin('csv') t2 ON t1.a = t2.a",
        ])
        .write_stdin(input)
        .assert()
        // Execution errors are printed to stdout by the CLI
        .stdout(contains_str("already been consumed"));
}

/// Stdin is captured once per process, so a second format is rejected
#[test]
fn test_stdin_conflicting_formats() {
    Command::cargo_bin("dft")
        .unwrap()
        .args(["-c", "SELECT * FROM stdin('csv') t1, stdin('json') t2"])
        .write_stdin("a,b\n1,x\n")
        .assert()
        .failure()
        .stderr(contains_str("already opened with format 'csv'"));
}

#[test]
fn test_stdin_empty_input() {
    Command::cargo_bin("dft")
        .unwrap()
        .args(["-c", "SELECT * FROM stdin('csv')"])
        .write_stdin("")
        .assert()
        .failure()
        .stderr(contains_str("no data received on stdin"));
}

#[test]
fn test_stdin_unsupported_format() {
    Command::cargo_bin("dft")
        .unwrap()
        .args(["-c", "SELECT * FROM stdin('yaml')"])
        .write_stdin("a\n1\n")
        .assert()
        .failure()
        .stderr(contains_str("unsupported stdin() format 'yaml'"));
}

#[test]
fn test_stdin_missing_format() {
    Command::cargo_bin("dft")
        .unwrap()
        .args(["-c", "SELECT * FROM stdin()"])
        .write_stdin("a\n1\n")
        .assert()
        .failure()
        .stderr(contains_str("requires a single string argument"));
}

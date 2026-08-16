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

//! Tests for local (non-FlightSQL) `--analyze` and `--analyze-raw`.
//!
//! The `data/test_io_formats.{arrow,csv,json,parquet}` files at the repo root
//! are fixtures for these and the FlightSQL analyze tests.

use assert_cmd::Command;

use super::sql_in_file;

#[test]
fn test_analyze_command() {
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1 + 2")
        .arg("--analyze")
        .assert()
        .success();

    let output = String::from_utf8_lossy(&assert.get_output().stdout).to_string();
    assert!(output.contains("Query"), "Should contain Query section");
    assert!(
        output.contains("Execution Summary"),
        "Should contain Execution Summary"
    );
    assert!(
        output.contains("Parsing"),
        "Should contain timing breakdown"
    );
    assert!(
        output.contains("Compute Summary"),
        "Should contain Compute Summary"
    );
}

#[test]
fn test_analyze_file() {
    let file = sql_in_file("SELECT 1 + 1");
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-f")
        .arg(file.path())
        .arg("--analyze")
        .assert()
        .success();

    let output = String::from_utf8_lossy(&assert.get_output().stdout).to_string();
    assert!(
        output.contains("Execution Summary"),
        "Should contain Execution Summary"
    );
}

#[test]
fn test_analyze_raw_command() {
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1 + 2")
        .arg("--analyze-raw")
        .assert()
        .success();

    // Raw mode prints the metrics table, not the formatted summary
    let output = String::from_utf8_lossy(&assert.get_output().stdout).to_string();
    assert!(
        output.contains("metric_name"),
        "Should contain metric_name column"
    );
    assert!(output.contains("node_id"), "Should contain node_id column");
    assert!(
        output.contains("parent_node_id"),
        "Should contain parent_node_id column"
    );
    assert!(
        output.contains("query.rows"),
        "Should contain query.rows metric"
    );
    assert!(
        output.contains("stage.execution"),
        "Should contain stage.execution metric"
    );
    assert!(
        !output.contains("Execution Summary"),
        "Raw mode should not print the formatted summary"
    );
}

#[test]
fn test_analyze_raw_parquet_io_metrics() {
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT * FROM parquet_scan WHERE value > 50")
        .arg("--analyze-raw")
        .arg("--run-before")
        .arg("CREATE EXTERNAL TABLE parquet_scan STORED AS PARQUET LOCATION 'data/test_io_formats.parquet'")
        .assert()
        .success();

    let output = String::from_utf8_lossy(&assert.get_output().stdout).to_string();
    assert!(
        output.contains("io.parquet.bytes_scanned"),
        "Should contain io.parquet.bytes_scanned for a Parquet scan"
    );
    assert!(
        output.contains("io.parquet.rg_pruned"),
        "Should contain io.parquet.rg_pruned for a Parquet scan"
    );
    assert!(
        output.contains("io.parquet.rg_matched"),
        "Should contain io.parquet.rg_matched for a Parquet scan"
    );
}

#[test]
fn test_analyze_raw_output_to_file() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("metrics.csv");

    Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1")
        .arg("--analyze-raw")
        .arg("--output")
        .arg(&out)
        .assert()
        .success();

    let contents = std::fs::read_to_string(&out).unwrap();
    assert!(
        contents.contains("metric_name"),
        "Output file should contain the metrics table header"
    );
    assert!(
        contents.contains("query.rows"),
        "Output file should contain query.rows metric"
    );
}

#[test]
fn test_analyze_multiple_commands_fails() {
    Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1")
        .arg("-c")
        .arg("SELECT 2")
        .arg("--analyze")
        .assert()
        .failure();
}

#[test]
fn test_analyze_and_bench_mutually_exclusive() {
    Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1")
        .arg("--analyze")
        .arg("--bench")
        .assert()
        .failure();
}

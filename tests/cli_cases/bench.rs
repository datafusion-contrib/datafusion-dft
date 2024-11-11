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

//! Tests for the CLI (e.g. run from files)

use assert_cmd::Command;

use super::{contains_str, sql_in_file};

#[test]
fn test_bench_command() {
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1")
        .arg("--bench")
        .assert()
        .success();

    let expected = r##"
----------------------------
Benchmark Stats (10 runs)
----------------------------
SELECT 1
----------------------------"##;
    assert.stdout(contains_str(expected));
}

#[test]
fn test_bench_files() {
    let file = sql_in_file(r#"SELECT 1 + 1;"#);

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-f")
        .arg(file.path())
        .arg("--bench")
        .assert()
        .success();

    let expected_err = r##"
----------------------------
Benchmark Stats (10 runs)
----------------------------
SELECT 1 + 1;
----------------------------"##;
    assert.code(0).stdout(contains_str(expected_err));
}

#[test]
fn test_bench_command_with_run_before() {
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT * FROM t")
        .arg("--bench")
        .arg("--run-before")
        .arg("CREATE TABLE t AS VALUES (1)")
        .assert()
        .success();

    let expected = r##"
----------------------------
Benchmark Stats (10 runs)
----------------------------
SELECT * FROM t
----------------------------"##;
    assert.stdout(contains_str(expected));
}

#[test]
fn test_bench_files_with_run_before() {
    let file = sql_in_file(r#"SELECT * FROM t;"#);

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-f")
        .arg(file.path())
        .arg("--bench")
        .arg("--run-before")
        .arg("CREATE TABLE t AS VALUES (1)")
        .assert()
        .success();

    let expected_err = r##"
----------------------------
Benchmark Stats (10 runs)
----------------------------
SELECT * FROM t;
----------------------------"##;
    assert.code(0).stdout(contains_str(expected_err));
}

#[test]
fn test_bench_command_with_save() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("results.csv");

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1")
        .arg("--bench")
        .arg("--save")
        .arg(file.to_str().unwrap())
        .assert()
        .success();

    let expected = r##"
----------------------------
Benchmark Stats (10 runs)
----------------------------
SELECT 1
----------------------------"##;
    assert.stdout(contains_str(expected));

    assert!(file.exists());
}

#[test]
fn test_bench_command_with_save_and_append() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("results.csv");

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1")
        .arg("--bench")
        .arg("--save")
        .arg(file.to_str().unwrap())
        .assert()
        .success();

    let expected = r##"
----------------------------
Benchmark Stats (10 runs)
----------------------------
SELECT 1
----------------------------"##;
    assert.stdout(contains_str(expected));

    assert!(file.exists());

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1")
        .arg("--bench")
        .arg("--save")
        .arg(file.to_str().unwrap())
        .arg("--append")
        .assert()
        .success();

    let expected = r##"
----------------------------
Benchmark Stats (10 runs)
----------------------------
SELECT 1
----------------------------"##;
    assert.stdout(contains_str(expected));

    let contents = std::fs::read_to_string(file).unwrap();
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 3);
}

#[test]
fn test_bench_command_with_custom_iterations() {
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1")
        .arg("--bench")
        .arg("-n")
        .arg("3")
        .assert()
        .success();

    let expected = r##"
----------------------------
Benchmark Stats (3 runs)
----------------------------
SELECT 1
----------------------------"##;
    assert.stdout(contains_str(expected));
}

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
use predicates::str::ContainsPredicate;
use std::path::PathBuf;
use tempfile::NamedTempFile;

#[test]
fn test_help() {
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("--help")
        .assert()
        .success();

    assert.stdout(contains_str("dft"));
}

#[test]
#[ignore]
fn test_logging() {
    // currently fails with
    // Error: Device not configured (os error 6)
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .env("RUST_LOG", "info")
        .assert()
        .success();

    assert.stdout(contains_str("INFO"));
}

#[test]
fn test_command_in_file() {
    let expected = r##"
+---------------------+
| Int64(1) + Int64(1) |
+---------------------+
| 2                   |
+---------------------+
    "##;

    let file = sql_in_file("SELECT 1 + 1");
    assert_output_contains(vec![file], expected);

    // same test but with a semicolon at the end
    let file = sql_in_file("SELECT 1 + 1;");
    assert_output_contains(vec![file], expected);
}

#[test]
fn test_multiple_commands_in_file() {
    let expected = r##"
+---------+
| column1 |
+---------+
| 42      |
+---------+
+------------------------+
| foo.column1 + Int64(2) |
+------------------------+
| 44                     |
+------------------------+
    "##;

    let sql = r#"
-- The first line is a comment
CREATE TABLE foo as values (42);
-- lets ignore some whitespace

    SELECT column1 FROM foo;

-- Another comment
SELECT column1 + 2 FROM foo
    "#;

    let file = sql_in_file(sql);
    assert_output_contains(vec![file], expected);

    // same test but with a semicolon at the end of second command
    let file = sql_in_file(format!("{sql};"));
    assert_output_contains(vec![file], expected);
}

#[test]
fn test_multiple_commands_in_multiple_files() {
    let expected = r##"
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
+----------+
| Int64(1) |
+----------+
| 1        |
+----------+
+----------+
| Int64(2) |
+----------+
| 2        |
+----------+
    "##;

    let file1 = sql_in_file("SELECT 1 + 2");
    let file2 = sql_in_file("SELECT 1;\nselect 2;");
    assert_output_contains(vec![file1, file2], expected);
}

#[test]
fn test_non_existent_file() {
    let file = sql_in_file("SELECT 1 + 1");
    let p = PathBuf::from(file.path());
    // dropping the file makes it non existent
    drop(file);

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-f")
        .arg(&p)
        .assert()
        .failure();

    let expected = format!("File does not exist: '{}'", p.to_string_lossy());
    assert.code(2).stderr(contains_str(&expected));
}

#[test]
fn test_one_existent_and_one_non_existent_file() {
    let file1 = sql_in_file("SELECT 1 + 1");
    let file2 = sql_in_file("SELECT 3 + 4");
    let p1 = PathBuf::from(file1.path());
    let p2 = PathBuf::from(file2.path());
    // dropping the file makes it non existent
    drop(file2);

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-f")
        .arg(p1)
        .arg("-f")
        .arg(&p2)
        .assert()
        .failure();

    let expected_err = format!("File does not exist: '{}'", p2.to_string_lossy());
    assert.code(2).stderr(contains_str(&expected_err));
}

#[test]
fn test_sql_err_in_file() {
    let file = sql_in_file("SELECT this is not valid SQL");

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-f")
        .arg(file.path())
        .assert()
        .failure();

    let expected_err =
        "Expected: [NOT] NULL or TRUE|FALSE or [NOT] DISTINCT FROM after IS, found: not";
    assert.code(1).stderr(contains_str(expected_err));
}

#[test]
fn test_sql_err_in_file_after_first() {
    let file = sql_in_file(
        r#"
-- First line is valid SQL
SELECT 1 + 1;
-- Second line is not
SELECT this is not valid SQL
    "#,
    );

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-f")
        .arg(file.path())
        .assert()
        .failure();

    let expected_err =
        "Expected: [NOT] NULL or TRUE|FALSE or [NOT] DISTINCT FROM after IS, found: not";
    assert.code(1).stderr(contains_str(expected_err));
}

#[test]
fn test_sql_in_file_and_arg() {
    let file = sql_in_file("SELECT 1 + 1");

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-f")
        .arg(file.path())
        // also specify a query on the command line
        .arg("-c")
        .arg("SELECT 3 + 4")
        .assert()
        .failure();

    assert.code(1).stderr(contains_str(
        "Error: Cannot execute both files and commands at the same time",
    ));
}

#[test]
fn test_sql_in_arg() {
    let expected = r##"
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
    "##;
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1 + 2;")
        .assert()
        .success();

    assert.stdout(contains_str(expected));
}

#[test]
fn test_multiple_sql_in_arg() {
    let expected = r##"
+------------+
| sum(foo.x) |
+------------+
| 3          |
+------------+
    "##;
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        // use multiple SQL statements in one argument that need to run in the same
        // context
        .arg("CREATE TABLE foo(x int) as values (1), (2); SELECT sum(x) FROM foo")
        .assert()
        .success();

    assert.stdout(contains_str(expected));
}
#[test]
fn test_multiple_sql_in_multiple_args() {
    let expected = r##"
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
+---------------------+
| Int64(3) + Int64(5) |
+---------------------+
| 8                   |
+---------------------+
    "##;
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1 + 2")
        .arg("SELECT 3 + 5")
        .assert()
        .success();

    assert.stdout(contains_str(expected));
}

#[test]
fn test_multiple_sql_in_multiple_args2() {
    let expected = r##"
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
+---------------------+
| Int64(3) + Int64(5) |
+---------------------+
| 8                   |
+---------------------+
    "##;
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1 + 2")
        .arg("-c") // add second -c
        .arg("SELECT 3 + 5")
        .assert()
        .success();

    assert.stdout(contains_str(expected));
}


// Validate that the CLI is correctly connected to query files
#[test]
fn test_query_file() {
    // Note that the results come out as two batches as the batch size is set to 1
    let expected = r##"
+------------+
| double_col |
+------------+
| 0.0        |
+------------+
+------------+
| double_col |
+------------+
| 10.1       |
+------------+
    "##;
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT double_col from 'data/alltypes_plain.snappy.parquet'")
        .assert()
        .success();

    assert.stdout(contains_str(expected));
}


/// Creates a temporary file with the given SQL content
pub fn sql_in_file(sql: impl AsRef<str>) -> NamedTempFile {
    let file = NamedTempFile::new().unwrap();
    std::fs::write(file.path(), sql.as_ref()).unwrap();
    file
}

/// Returns a predicate that expects the given string to be contained in the
/// output
///
/// Whitespace is trimmed from the start and end of the string
pub fn contains_str(s: &str) -> ContainsPredicate {
    predicates::str::contains(s.trim())
}

/// Invokes `dft -f` with the given files and asserts that it exited
/// successfully and the output contains the given string
pub fn assert_output_contains(files: Vec<NamedTempFile>, expected_output: &str) {
    let mut cmd = Command::cargo_bin("dft").unwrap();
    for file in &files {
        cmd.arg("-f").arg(file.path());
    }

    let assert = cmd.assert().success();

    // Since temp files are deleted when they go out of scope ensure they are
    // dropped only after the command is run
    drop(files);

    assert.stdout(contains_str(expected_output));
}

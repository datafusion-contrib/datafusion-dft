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
use std::{io::Read, path::PathBuf};

use super::{assert_output_contains, contains_str, sql_in_file};

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

#[test]
fn test_time_command() {
    let expected = r##"executed in"##;
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg("SELECT 1 + 2")
        .arg("--time")
        .assert()
        .success();

    assert.stdout(contains_str(expected));
}

#[test]
fn test_time_files() {
    let file = sql_in_file(
        r#"
SELECT 1 + 1;
    "#,
    );

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-f")
        .arg(file.path())
        .arg("--time")
        .assert()
        .success();

    let expected_err = "executed in";
    assert.code(0).stdout(contains_str(expected_err));
}

#[test]
fn test_write_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("test_write_file.csv");

    let sql = format!("COPY (SELECT 1 + 1) TO '{}'", file.to_string_lossy());
    Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg(sql)
        .assert()
        .success();

    assert!(file.exists());
}

#[test]
fn test_query_local_file() {
    let sql = "SELECT c1 FROM 'data/aggregate_test_100.csv' LIMIT 1".to_string();
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg(sql)
        .assert()
        .success();

    let expected = "
+----+
| c1 |
+----+
| c  |
+----+
";

    assert.stdout(contains_str(expected));
}

#[test]
fn test_query_non_existent_local_file() {
    let sql = "SELECT c1 FROM 'data/nofile.csv' LIMIT 1".to_string();
    Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg(sql)
        .assert()
        .failure();
}

#[test]
fn test_more_than_one_command_with_output() {
    let sql = "SELECT 1".to_string();
    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg(sql.clone())
        .arg("-c")
        .arg(sql)
        .arg("-o")
        .arg("test.csv")
        .assert()
        .failure();
    let expected = "Error: Output can only be saved for a single file or command";
    assert.stderr(contains_str(expected));
}

#[test]
fn test_output_csv() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.csv");

    let sql = "SELECT 1".to_string();
    Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg(sql.clone())
        .arg("-o")
        .arg(path.clone())
        .assert()
        .success();

    let mut file = std::fs::File::open(path).unwrap();
    let mut buffer = String::new();
    file.read_to_string(&mut buffer).unwrap();

    let expected = "Int64(1)\n1\n";
    assert_eq!(buffer, expected);
}

#[test]
fn test_output_json() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.json");

    let sql = "SELECT 1".to_string();
    Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg(sql.clone())
        .arg("-o")
        .arg(path.clone())
        .assert()
        .success();

    let mut file = std::fs::File::open(path).unwrap();
    let mut buffer = String::new();
    file.read_to_string(&mut buffer).unwrap();

    let expected = "{\"Int64(1)\":1}\n";
    assert_eq!(buffer, expected);
}

#[test]
fn test_output_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.parquet");

    let sql = "SELECT 1".to_string();
    Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg(sql.clone())
        .arg("-o")
        .arg(path.clone())
        .assert()
        .success();

    let read_sql = format!("SELECT * FROM '{}'", path.to_str().unwrap());

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("-c")
        .arg(read_sql)
        .assert()
        .success();

    let expected = r#"
+----------+
| Int64(1) |
+----------+
| 1        |
+----------+"#;

    assert.stdout(contains_str(expected));
}

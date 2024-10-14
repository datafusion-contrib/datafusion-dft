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

//! Tests for the CLI (e.g. run from files) to make sure config works as expected

use crate::config::TestConfigBuilder;
use assert_cmd::Command;
use std::io::Write;

use super::contains_str;

#[test]
fn test_custom_config() {
    let tempdir = tempfile::tempdir().unwrap();
    let ddl_path = tempdir.path().join("my_ddl.sql");
    let mut file = std::fs::File::create(ddl_path.clone()).unwrap();
    let ddl = "CREATE TABLE x AS VALUES (1)";
    file.write_all(ddl.as_bytes()).unwrap();
    file.flush().unwrap();
    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_ddl_path(ddl_path);
    let config = config_builder.build("my_config.toml");

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path)
        .arg("--run-ddl")
        .arg("-c")
        .arg("SELECT * FROM x")
        .assert()
        .success();

    let expected = r#"
+---------+
| column1 |
+---------+
| 1       |
+---------+"#;

    assert.stdout(contains_str(expected));
}

#[test]
fn test_custom_config_multiple_ddl() {
    let tempdir = tempfile::tempdir().unwrap();
    let ddl_path = tempdir.path().join("my_ddl.sql");
    let mut file = std::fs::File::create(ddl_path.clone()).unwrap();
    let ddl = "CREATE TABLE x AS VALUES (1);\nCREATE TABLE y AS VALUES (2)";
    file.write_all(ddl.as_bytes()).unwrap();
    file.flush().unwrap();
    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_ddl_path(ddl_path);
    let config = config_builder.build("my_config.toml");

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path)
        .arg("--run-ddl")
        .arg("-c")
        .arg("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('x', 'y') ORDER BY table_name ASC")
        .assert()
        .success();

    let expected = r##"+------------+
| table_name |
+------------+
| x          |
| y          |
+------------+
"##;

    assert.stdout(contains_str(expected));
}

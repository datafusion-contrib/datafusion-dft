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

use std::io::Write;

use assert_cmd::Command;

use crate::{cli_cases::contains_str, config::TestConfigBuilder};

#[tokio::test(flavor = "multi_thread")]
async fn test_s3_basic() {
    let tempdir = tempfile::tempdir().unwrap();
    let ddl_path = tempdir.path().join("my_ddl.sql");
    let mut file = std::fs::File::create(ddl_path.clone()).unwrap();
    let ddl = "CREATE EXTERNAL TABLE a STORED AS CSV LOCATION 's3://test/aggregate_test_100.csv';";
    file.write_all(ddl.as_bytes()).unwrap();
    file.flush().unwrap();
    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_ddl_path("cli", ddl_path);
    config_builder.with_s3_object_store(
        "cli",
        "s3",
        "test",
        "s3://test",
        "http://localhost:4566",
        "LSIAQAAAAAAVNCBMPNSG",
        "5555555555555555555555555555555555555555",
        true,
    );
    let config = config_builder.build("my_config.toml");

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path)
        .arg("--run-ddl")
        .arg("-c")
        .arg("SELECT c1 FROM a LIMIT 1")
        .assert()
        .success();

    let expected = r#"
+----+
| c1 |
+----+
| c  |
+----+
"#;

    assert.stdout(contains_str(expected));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_s3_credential_chain_env_vars() {
    // Set AWS environment variables for credential chain
    std::env::set_var("AWS_ACCESS_KEY_ID", "LSIAQAAAAAAVNCBMPNSG");
    std::env::set_var(
        "AWS_SECRET_ACCESS_KEY",
        "5555555555555555555555555555555555555555",
    );
    std::env::set_var("AWS_ENDPOINT_URL_S3", "http://localhost:4566");

    let tempdir = tempfile::tempdir().unwrap();
    let ddl_path = tempdir.path().join("my_ddl.sql");
    let mut file = std::fs::File::create(ddl_path.clone()).unwrap();
    let ddl = "CREATE EXTERNAL TABLE a STORED AS CSV LOCATION 's3://test/aggregate_test_100.csv';";
    file.write_all(ddl.as_bytes()).unwrap();
    file.flush().unwrap();

    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_ddl_path("cli", ddl_path);
    config_builder.with_s3_credential_chain("cli", "s3", "test", "s3://test", true);
    let config = config_builder.build("my_config.toml");

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path)
        .arg("--run-ddl")
        .arg("-c")
        .arg("SELECT c1 FROM a LIMIT 1")
        .assert()
        .success();

    // Clean up env vars
    std::env::remove_var("AWS_ACCESS_KEY_ID");
    std::env::remove_var("AWS_SECRET_ACCESS_KEY");
    std::env::remove_var("AWS_ENDPOINT_URL_S3");

    let expected = r#"
+----+
| c1 |
+----+
| c  |
+----+
"#;

    assert.stdout(contains_str(expected));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_s3_credential_chain_with_static_override() {
    // Set wrong credentials in environment
    std::env::set_var("AWS_ACCESS_KEY_ID", "WRONG_KEY");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "WRONG_SECRET");
    std::env::set_var("AWS_ENDPOINT_URL_S3", "http://localhost:4566");

    let tempdir = tempfile::tempdir().unwrap();
    let ddl_path = tempdir.path().join("my_ddl.sql");
    let mut file = std::fs::File::create(ddl_path.clone()).unwrap();
    let ddl = "CREATE EXTERNAL TABLE a STORED AS CSV LOCATION 's3://test/aggregate_test_100.csv';";
    file.write_all(ddl.as_bytes()).unwrap();
    file.flush().unwrap();

    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_ddl_path("cli", ddl_path);

    // Build config with credential chain enabled but provide correct static credentials
    // that override the wrong environment credentials
    config_builder.with_s3_credential_chain_and_static_override(
        "cli",
        "s3",
        "test",
        "s3://test",
        "http://localhost:4566",
        "LSIAQAAAAAAVNCBMPNSG",
        "5555555555555555555555555555555555555555",
        true,
    );

    let config = config_builder.build("my_config.toml");

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path)
        .arg("--run-ddl")
        .arg("-c")
        .arg("SELECT c1 FROM a LIMIT 1")
        .assert()
        .success();

    // Clean up env vars
    std::env::remove_var("AWS_ACCESS_KEY_ID");
    std::env::remove_var("AWS_SECRET_ACCESS_KEY");
    std::env::remove_var("AWS_ENDPOINT_URL_S3");

    let expected = r#"
+----+
| c1 |
+----+
| c  |
+----+
"#;

    assert.stdout(contains_str(expected));
}

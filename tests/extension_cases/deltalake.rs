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

use url::Url;

use crate::extension_cases::TestExecution;

#[tokio::test(flavor = "multi_thread")]
async fn test_deltalake() {
    let test_exec = TestExecution::new();

    let cwd = std::env::current_dir().unwrap();
    let path = Url::from_file_path(cwd.join("data/deltalake/simple_table")).unwrap();

    let test_exec = test_exec
        .await
        .with_setup(&format!(
            "CREATE EXTERNAL TABLE d STORED AS DELTATABLE LOCATION '{}';",
            path
        ))
        .await;

    let output = test_exec
        .run_and_format("SELECT id FROM d ORDER BY id")
        .await;
    assert_eq!(
        output,
        vec!["+----+", "| id |", "+----+", "| 5  |", "| 7  |", "| 9  |", "+----+"]
    );
}

#[cfg(feature = "s3")]
#[tokio::test(flavor = "multi_thread")]
async fn test_deltalake_s3() {
    use assert_cmd::Command;
    use std::io::Write;

    use crate::{cli_cases::contains_str, config::TestConfigBuilder};

    let tempdir = tempfile::tempdir().unwrap();
    let ddl_path = tempdir.path().join("my_ddl.sql");
    let mut file = std::fs::File::create(ddl_path.clone()).unwrap();
    let ddl = "CREATE EXTERNAL TABLE delta_tbl STORED AS DELTATABLE LOCATION 's3://test/deltalake/simple_table';";
    file.write_all(ddl.as_bytes()).unwrap();
    file.flush().unwrap();

    // Create a temp db directory to avoid conflicts with existing db files
    let db_dir = tempdir.path().join("db");
    std::fs::create_dir(&db_dir).unwrap();

    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_ddl_path("cli", ddl_path);
    config_builder.with_db_path(&format!("file://{}", db_dir.display()));
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
        .arg("SELECT id FROM delta_tbl ORDER BY id")
        .assert()
        .success();

    let expected = r#"
+----+
| id |
+----+
| 5  |
| 7  |
| 9  |
+----+
"#;

    assert.stdout(contains_str(expected));
}

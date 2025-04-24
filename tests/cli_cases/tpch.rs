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

#[test]
fn test_custom_config() {
    let tempdir = tempfile::tempdir().unwrap();
    let db_path = tempdir.path().join("db");
    std::fs::create_dir_all(&db_path).unwrap();
    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_db_path(&format!("file://{}", db_path.to_str().unwrap()));
    let config = config_builder.build("my_config.toml");

    Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path)
        .arg("generate-tpch")
        .assert()
        .success();

    let needed_dirs = [
        "customer", "orders", "lineitem", "nation", "part", "partsupp", "region", "supplier",
    ];
    let tables_path = db_path.join("tables").join("dft").join("tpch");
    std::fs::read_dir(tables_path).unwrap().for_each(|e| {
        let entry = e.unwrap();
        assert!(needed_dirs.contains(&entry.file_name().to_str().unwrap()));
        let data = entry.path().join("data.parquet");
        assert!(data.exists());
        let metadata = data.metadata().unwrap();
        assert!(metadata.len() > 1000);
    })
}

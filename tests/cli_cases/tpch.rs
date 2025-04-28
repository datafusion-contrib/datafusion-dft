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
use object_store::{aws::AmazonS3Builder, path::Path, ObjectStore};

#[test]
fn test_custom_config() {
    let tempdir = tempfile::tempdir().unwrap();
    let db_path = tempdir.path().join("db/");
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

#[tokio::test]
async fn test_custom_config_with_s3() {
    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_db_path("s3://tpch-db/db/");
    let bucket = "tpch-db";
    let endpoint = "http://localhost:4566";
    let access_key = "LSIAQAAAAAAVNCBMPNSG";
    let secret = "5555555555555555555555555555555555555555";
    let allow_http = true;
    config_builder.with_s3_object_store(
        "cli",
        "s3",
        bucket,
        "s3://tpch-db",
        endpoint,
        access_key,
        secret,
        allow_http,
    );
    let config = config_builder.build("my_config.toml");

    let a = Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path)
        .arg("generate-tpch")
        .assert()
        .success();

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_endpoint(endpoint)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret)
        .with_allow_http(allow_http)
        .build()
        .unwrap();

    let r = s3
        .list_with_delimiter(Some(&Path::parse("db/tables/dft/tpch/").unwrap()))
        .await
        .unwrap();

    let needed_dirs = [
        "customer", "orders", "lineitem", "nation", "part", "partsupp", "region", "supplier",
    ];

    let prefixes: Vec<_> = r
        .common_prefixes
        .iter()
        .map(|p| p.parts().last().unwrap().as_ref().to_string())
        .collect();
    for dir in needed_dirs {
        assert!(prefixes.contains(&dir.to_string()));
    }
}

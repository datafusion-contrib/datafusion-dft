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

//! Tests for the MongoDB catalog integration. Tests that query data require a running
//! MongoDB instance in a container named `mongodb` (seeding is done via `docker exec`):
//!
//! ```bash
//! docker run -d --name mongodb -p 27017:27017 \
//!   -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=secret \
//!   mongo:7.0
//! ```

use assert_cmd::Command;

use crate::{cli_cases::contains_str, config::TestConfigBuilder};

const MONGODB_CONTAINER: &str = "mongodb";
const MONGODB_HOST: &str = "localhost";
const MONGODB_PORT: u16 = 27017;
const MONGODB_USER: &str = "admin";
const MONGODB_PASSWORD: &str = "secret";

/// Run a mongosh script inside the MongoDB container
fn mongodb_exec(script: &str) {
    let output = std::process::Command::new("docker")
        .args([
            "exec",
            "-i",
            MONGODB_CONTAINER,
            "mongosh",
            "-u",
            MONGODB_USER,
            "-p",
            MONGODB_PASSWORD,
            "--authenticationDatabase",
            "admin",
            "--quiet",
            "--eval",
            script,
        ])
        .output()
        .expect("Failed to run mongosh via docker exec");
    assert!(
        output.status.success(),
        "MongoDB script failed: {script}\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Create a `users` collection with two documents in `database`. Each test seeds its own
/// database so that concurrently running tests do not interfere with each other.
fn seed_mongodb(database: &str) {
    mongodb_exec(&format!(
        "const db2 = db.getSiblingDB('{database}'); \
         db2.users.drop(); \
         db2.users.insertMany([ \
             {{ id: NumberInt(1), name: 'alice' }}, \
             {{ id: NumberInt(2), name: 'bob' }} \
         ]);"
    ));
}

fn mongodb_config(database: Option<&str>) -> crate::config::TestConfig {
    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_mongodb(
        "cli",
        "mongodb",
        MONGODB_HOST,
        MONGODB_PORT,
        Some(MONGODB_USER),
        Some(MONGODB_PASSWORD),
        database,
        &[("auth_source", "admin"), ("sslmode", "disabled")],
    );
    config_builder.build("my_config.toml")
}

#[test]
fn test_mongodb_config_parsing() {
    let config = mongodb_config(Some("dft_mongo_parsing"));
    let app_config = datafusion_dft::config::create_config(config.path, &[]);
    let mongodb_configs = app_config
        .cli
        .execution
        .mongodb
        .expect("MongoDB config missing");
    assert_eq!(mongodb_configs.len(), 1);
    let mongodb_config = &mongodb_configs[0];
    assert_eq!(mongodb_config.name, "mongodb");
    assert_eq!(mongodb_config.host.as_deref(), Some(MONGODB_HOST));
    assert_eq!(mongodb_config.port, Some(MONGODB_PORT));
    assert_eq!(mongodb_config.user.as_deref(), Some(MONGODB_USER));
    assert_eq!(mongodb_config.password.as_deref(), Some(MONGODB_PASSWORD));
    assert_eq!(
        mongodb_config.database.as_deref(),
        Some("dft_mongo_parsing")
    );

    let params = mongodb_config.to_params();
    assert_eq!(params.get("host").map(String::as_str), Some(MONGODB_HOST));
    assert_eq!(params.get("port").map(String::as_str), Some("27017"));
    assert_eq!(params.get("user").map(String::as_str), Some(MONGODB_USER));
    assert_eq!(
        params.get("pass").map(String::as_str),
        Some(MONGODB_PASSWORD)
    );
    assert_eq!(
        params.get("db").map(String::as_str),
        Some("dft_mongo_parsing")
    );
    assert_eq!(params.get("auth_source").map(String::as_str), Some("admin"));
    assert_eq!(params.get("sslmode").map(String::as_str), Some("disabled"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mongodb_query() {
    seed_mongodb("dft_mongo_query");
    let config = mongodb_config(Some("dft_mongo_query"));

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path)
        .arg("-c")
        .arg("SELECT id, name FROM mongodb.dft_mongo_query.users ORDER BY id")
        .assert()
        .success();

    let expected = r#"
+----+-------+
| id | name  |
+----+-------+
| 1  | alice |
| 2  | bob   |
+----+-------+
"#;

    assert.stdout(contains_str(expected));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mongodb_database_discovery() {
    seed_mongodb("dft_mongo_discovery");
    // No database configured, so all non-system databases are discovered as schemas
    let config = mongodb_config(None);

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path)
        .arg("-c")
        .arg("SELECT id, name FROM mongodb.dft_mongo_discovery.users ORDER BY id")
        .assert()
        .success();

    let expected = r#"
+----+-------+
| id | name  |
+----+-------+
| 1  | alice |
| 2  | bob   |
+----+-------+
"#;

    assert.stdout(contains_str(expected));
}

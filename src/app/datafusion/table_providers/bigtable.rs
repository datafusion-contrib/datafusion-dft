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

use datafusion::execution::context::ExecutionContext;
use http::Uri;
use log::debug;
use serde::Deserialize;
use std::str::FromStr;
use std::fs::File;

#[cfg(feature="bigtable")]
pub fn register_bigtable(ctx: ExecutionContext) -> ExecutionContext {
    #[derive(Deserialize, Debug)]
    enum QualifierType {
        Int(i64),
        Utf(String),
    }
    
    #[derive(Deserialize, Debug)]
    struct BigTableConfig {
        project: String,
        instance: String,
        table: String,
        column_family: String,
        table_partition_cols: Vec<String>,
        table_partition_separator: String,
        qualifiers: Vec<QualifierType>,
        secret_access_key: String,
    }

    #[derive(Deserialize, Debug)]
    struct BigTableConfigs {
        tables: Vec<BigTableConfig>
    }
    
    let home = dirs::home_dir();
    if let Some(p) = home {
        let bigtable_config_path = p.join(".datafusion/table_providers/bigtable.json");
        let bigtable = if bigtable_config_path.exists() {
            let cfg: BigTableConfigs =
                serde_json::from_reader(File::open(bigtable_config_path).unwrap()).unwrap();
            // let s3 = config_to_s3(cfg).await;
            debug!("BigTable Config: {:?}", cfg);
            // Arc::new(s3)
        };
    }
    ctx
}





#[cfg(feature = "s3")]
pub async fn register_s3(ctx: ExecutionContext) -> ExecutionContext {
    use aws_sdk_s3::Endpoint;
    use aws_types::credentials::{Credentials, SharedCredentialsProvider};
    use datafusion_objectstore_s3::object_store::s3::S3FileSystem;
    use http::Uri;
    use serde::Deserialize;
    use std::str::FromStr;

    #[derive(Deserialize, Debug)]
    struct S3Config {
        endpoint: String,
        access_key_id: String,
        secret_access_key: String,
    }

    async fn config_to_s3(cfg: S3Config) -> S3FileSystem {
        info!("Creating S3 from: {:?}", cfg);
        S3FileSystem::new(
            Some(SharedCredentialsProvider::new(Credentials::new(
                cfg.access_key_id,
                cfg.secret_access_key,
                None,
                None,
                "Static",
            ))), // Credentials provider
            None, // Region
            Some(Endpoint::immutable(
                Uri::from_str(cfg.endpoint.as_str()).unwrap(),
            )), // Endpoint
            None, // RetryConfig
            None, // AsyncSleep
            None, // TimeoutConfig
        )
        .await
    }

    let home = dirs::home_dir();
    if let Some(p) = home {
        let s3_config_path = p.join(".datafusion/object_stores/s3.json");
        let s3 = if s3_config_path.exists() {
            let cfg: S3Config =
                serde_json::from_reader(File::open(s3_config_path).unwrap()).unwrap();
            let s3 = config_to_s3(cfg).await;
            info!("Created S3FileSystem from custom endpoint");
            Arc::new(s3)
        } else {
            let s3 = S3FileSystem::default().await;
            info!("Created S3FileSystem from default AWS credentials");
            Arc::new(s3)
        };

        ctx.register_object_store("s3", s3);
        info!("Registered S3 ObjectStore");
    }
    ctx
}
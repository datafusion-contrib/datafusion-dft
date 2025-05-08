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

//! AWS S3 Integration: [AwsS3Extension]

use crate::config::ExecutionConfig;
use crate::extensions::{DftSessionStateBuilder, Extension};
use log::{debug, info};
use std::sync::Arc;

use url::Url;

#[derive(Debug, Default)]
pub struct AwsS3Extension {}

impl AwsS3Extension {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Extension for AwsS3Extension {
    async fn register(
        &self,
        config: ExecutionConfig,
        builder: &mut DftSessionStateBuilder,
    ) -> datafusion::common::Result<()> {
        let Some(object_store_config) = &config.object_store else {
            return Ok(());
        };

        let Some(s3_configs) = &object_store_config.s3 else {
            return Ok(());
        };

        info!("S3 configs exists");
        for s3_config in s3_configs {
            match s3_config.to_object_store() {
                Ok(object_store) => {
                    debug!("created object store: {}", object_store);
                    if let Some(object_store_url) = s3_config.object_store_url() {
                        if let Ok(parsed_endpoint) = Url::parse(object_store_url) {
                            builder
                                .runtime_env()
                                .register_object_store(&parsed_endpoint, Arc::new(object_store));
                            info!("registered s3 object store at {object_store_url}");
                        }
                    }
                }
                Err(e) => {
                    log::error!("error creating object store: {:?}", e);
                }
            }
        }

        Ok(())
    }
}

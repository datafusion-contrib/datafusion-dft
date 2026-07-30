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

//! MongoDB integration: [MongoDbExtension]

use crate::catalog::mongodb::MongoDbCatalogProvider;
use crate::config::ExecutionConfig;
use crate::extensions::{DftSessionStateBuilder, Extension};
use datafusion::common::Result;
use log::info;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Default)]
pub struct MongoDbExtension {}

impl MongoDbExtension {
    pub fn new() -> Self {
        Self {}
    }
}

/// Render a short description of the MongoDB endpoint described by `params`, for use in error
/// messages.
fn endpoint_desc(params: &std::collections::HashMap<String, String>) -> String {
    if let Some(cs) = params.get("connection_string") {
        return cs.clone();
    }
    let host = params.get("host").cloned().unwrap_or_default();
    if let Some(port) = params.get("port") {
        format!("{host}:{port}")
    } else {
        host
    }
}

#[async_trait::async_trait]
impl Extension for MongoDbExtension {
    async fn register(
        &self,
        config: ExecutionConfig,
        builder: &mut DftSessionStateBuilder,
    ) -> Result<()> {
        for mongodb_config in config.mongodb.iter().flatten() {
            let params = mongodb_config.to_params();
            let endpoint = endpoint_desc(&params);
            let timeout = Duration::from_secs(mongodb_config.connect_timeout);

            let catalog_fut = MongoDbCatalogProvider::try_new(params);
            let catalog = if mongodb_config.connect_timeout == 0 {
                catalog_fut.await?
            } else {
                tokio::time::timeout(timeout, catalog_fut)
                    .await
                    .map_err(|_| {
                        datafusion::common::DataFusionError::Execution(format!(
                            "Timed out connecting to MongoDB catalog '{}' at {endpoint} after \
                             {timeout:?}. Is the database reachable?",
                            mongodb_config.name
                        ))
                    })??
            };

            builder.add_catalog_provider(&mongodb_config.name, Arc::new(catalog));
            info!("Registered MongoDB catalog '{}'", mongodb_config.name);
        }
        Ok(())
    }
}

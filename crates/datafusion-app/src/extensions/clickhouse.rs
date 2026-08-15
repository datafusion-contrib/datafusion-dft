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

//! ClickHouse integration: [ClickHouseExtension]

use crate::catalog::clickhouse::ClickHouseCatalogProvider;
use crate::config::ExecutionConfig;
use crate::extensions::{DftSessionStateBuilder, Extension};
use datafusion::common::{DataFusionError, Result};
use datafusion_table_providers::sql::db_connection_pool::clickhousepool::ClickHouseConnectionPool;
use datafusion_table_providers::util::secrets::to_secret_map;
use log::info;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Default)]
pub struct ClickHouseExtension {}

impl ClickHouseExtension {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Extension for ClickHouseExtension {
    async fn register(
        &self,
        config: ExecutionConfig,
        builder: &mut DftSessionStateBuilder,
    ) -> Result<()> {
        for clickhouse_config in config.clickhouse.iter().flatten() {
            let params = to_secret_map(clickhouse_config.to_params());
            let timeout = Duration::from_secs(clickhouse_config.connect_timeout);

            let pool_fut = ClickHouseConnectionPool::new(params);
            let pool = if clickhouse_config.connect_timeout == 0 {
                pool_fut
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            } else {
                tokio::time::timeout(timeout, pool_fut)
                    .await
                    .map_err(|_| {
                        DataFusionError::Execution(format!(
                            "Timed out connecting to ClickHouse catalog '{}' at {} after {timeout:?}. \
                             Is the database reachable?",
                            clickhouse_config.name, clickhouse_config.url
                        ))
                    })?
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            };

            let database = clickhouse_config.database.clone();
            let catalog_fut = ClickHouseCatalogProvider::try_new(Arc::new(pool), database);
            let catalog = if clickhouse_config.connect_timeout == 0 {
                catalog_fut.await?
            } else {
                tokio::time::timeout(timeout, catalog_fut)
                    .await
                    .map_err(|_| {
                        DataFusionError::Execution(format!(
                            "Timed out discovering schemas for ClickHouse catalog '{}' at {} \
                             after {timeout:?}.",
                            clickhouse_config.name, clickhouse_config.url
                        ))
                    })??
            };

            builder.add_catalog_provider(&clickhouse_config.name, Arc::new(catalog));
            info!(
                "Registered ClickHouse catalog '{}' for {}",
                clickhouse_config.name, clickhouse_config.url
            );
        }
        Ok(())
    }
}

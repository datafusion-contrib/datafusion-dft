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

//! Iceberg integration: [IcebergExtension]

use crate::config::ExecutionConfig;
use crate::extensions::{DftSessionStateBuilder, Extension};
use datafusion_common::DataFusionError;
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_datafusion::{IcebergCatalogProvider, IcebergTableProviderFactory};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct IcebergExtension {}

impl IcebergExtension {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Extension for IcebergExtension {
    async fn register(
        &self,
        config: ExecutionConfig,
        builder: &mut DftSessionStateBuilder,
    ) -> datafusion_common::Result<()> {
        for cfg in config.iceberg.rest_catalogs {
            let rest_catalog_config = RestCatalogConfig::builder().uri(cfg.addr).build();
            let rest_catalog = RestCatalog::new(rest_catalog_config);
            let catalog_provider = IcebergCatalogProvider::try_new(Arc::new(rest_catalog))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            builder.add_catalog_provider(&cfg.name, Arc::new(catalog_provider));
        }
        // TODO Add Iceberg Catalog
        let factory = Arc::new(IcebergTableProviderFactory {});
        builder.add_table_factory("ICEBERG", factory);
        Ok(())
    }
}

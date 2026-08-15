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

//! DeltaLake integration: [DeltaLakeExtension]

use crate::config::ExecutionConfig;
use crate::extensions::{DftSessionStateBuilder, Extension};
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::logical_plan::CreateExternalTable;
use deltalake::delta_datafusion::TableProviderBuilder;
use deltalake::table::builder::ensure_table_uri;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct DeltaLakeExtension {}

impl DeltaLakeExtension {
    pub fn new() -> Self {
        Self {}
    }
}

/// A wrapper around delta-rs's table factory that normalizes option keys passed
/// through DataFusion's `CREATE EXTERNAL TABLE ... OPTIONS (...)` clause.
///
/// DataFusion's `parse_options_map` prepends `"format."` to any option key that
/// does not contain a dot (e.g. `'aws_endpoint' '...'` becomes `"format.aws_endpoint"`
/// in `cmd.options`).  delta-rs's `S3ObjectStoreFactory` parses these keys via
/// `AmazonS3ConfigKey::from_str`, which does not recognise the `"format."` prefix,
/// so credentials and endpoint settings are silently ignored and the AWS SDK
/// credential chain is invoked instead.
///
/// This factory strips the leading `"format."` prefix before forwarding the options
/// to `open_table_with_storage_options`, allowing callers to supply S3 credentials
/// directly in the DDL without needing ambient environment variables.
#[derive(Debug, Default)]
struct DeltaTableFactory {}

#[async_trait::async_trait]
impl TableProviderFactory for DeltaTableFactory {
    async fn create(
        &self,
        _ctx: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let table_url =
            ensure_table_uri(&cmd.location).map_err(|e| DataFusionError::External(Box::new(e)))?;

        let provider = if cmd.options.is_empty() {
            deltalake::open_table(table_url)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            // DataFusion prepends "format." to option keys that don't contain a dot.
            // Strip that prefix so delta-rs can recognise the keys.
            let options: HashMap<String, String> = cmd
                .options
                .iter()
                .map(|(k, v)| {
                    let key = k
                        .strip_prefix("format.")
                        .map(str::to_string)
                        .unwrap_or_else(|| k.clone());
                    (key, v.clone())
                })
                .collect();
            deltalake::open_table_with_storage_options(table_url, options)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        };

        let table_provider = TableProviderBuilder::default()
            .with_log_store(provider.log_store())
            .build()
            .await?;

        Ok(Arc::new(table_provider))
    }
}

#[async_trait::async_trait]
impl Extension for DeltaLakeExtension {
    async fn register(
        &self,
        _config: ExecutionConfig,
        builder: &mut DftSessionStateBuilder,
    ) -> datafusion::common::Result<()> {
        builder.add_table_factory("DELTATABLE", Arc::new(DeltaTableFactory {}));
        Ok(())
    }
}

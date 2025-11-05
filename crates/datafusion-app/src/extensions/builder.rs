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

//! [`DftSessionStateBuilder`] for configuring DataFusion [`SessionState`]

use color_eyre::{eyre, Result};
use datafusion::catalog::MemoryCatalogProviderList;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, TableProviderFactory};
use datafusion::datasource::file_format::{
    csv::CsvFormatFactory, json::JsonFormatFactory, parquet::ParquetFormatFactory,
    FileFormatFactory,
};
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::config::ExecutionConfig;

use super::{enabled_extensions, Extension};

/// Builds a DataFusion [`SessionState`] with any necessary configuration
///
/// Ideally we would use the DataFusion [`SessionStateBuilder`], but it doesn't
/// currently have all the needed APIs. Once we have a good handle on the needed
/// APIs we can upstream them to DataFusion.
///
/// List of things that would be nice to add upstream:
/// TODO: Implement Debug for SessionStateBuilder upstream
///  <https://github.com/apache/datafusion/issues/12555>
/// TODO: Implement some way to get access to the current RuntimeEnv (to register object stores)
///  <https://github.com/apache/datafusion/issues/12553>
/// TODO: Implement a way to add just a single TableProviderFactory
///  <https://github.com/apache/datafusion/issues/12552>
/// TODO: Make TableFactoryProvider implement Debug
///   <https://github.com/apache/datafusion/pull/12557>
/// TODO: rename RuntimeEnv::new() to RuntimeEnv::try_new() as it returns a Result:
///   <https://github.com/apache/datafusion/issues/12554>
//#[derive(Debug)]
pub struct DftSessionStateBuilder {
    execution_config: Option<ExecutionConfig>,
    session_config: SessionConfig,
    table_factories: Option<HashMap<String, Arc<dyn TableProviderFactory>>>,
    file_format_factories: Vec<Arc<dyn FileFormatFactory>>,
    catalog_providers: Option<HashMap<String, Arc<dyn CatalogProvider>>>,
    runtime_env: Option<Arc<RuntimeEnv>>,
}

impl Debug for DftSessionStateBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DftSessionStateBuilder")
            .field("session_config", &self.session_config)
            .field(
                "table_factories",
                &"TODO TableFactory does not implement Debug",
            )
            .field("runtime_env", &self.runtime_env)
            .finish()
    }
}

impl Default for DftSessionStateBuilder {
    fn default() -> Self {
        Self {
            session_config: SessionConfig::default().with_information_schema(true),
            execution_config: None,
            table_factories: None,
            file_format_factories: vec![
                Arc::new(ParquetFormatFactory::new()),
                Arc::new(CsvFormatFactory::new()),
                Arc::new(JsonFormatFactory::new()),
            ],
            catalog_providers: None,
            runtime_env: None,
        }
    }
}

impl DftSessionStateBuilder {
    /// Create a new builder
    pub fn try_new(config: Option<ExecutionConfig>) -> Result<Self> {
        let session_config = if let Some(cfg) = config.clone().unwrap_or_default().datafusion {
            SessionConfig::from_string_hash_map(&cfg)?.with_information_schema(true)
        } else {
            SessionConfig::default().with_information_schema(true)
        };

        let builder = Self {
            session_config,
            execution_config: config,
            table_factories: None,
            file_format_factories: vec![
                Arc::new(ParquetFormatFactory::new()),
                Arc::new(CsvFormatFactory::new()),
                Arc::new(JsonFormatFactory::new()),
            ],

            catalog_providers: None,
            runtime_env: None,
        };
        Ok(builder)
    }

    /// Add a table factory to the list of factories on this builder
    pub fn add_table_factory(&mut self, name: &str, factory: Arc<dyn TableProviderFactory>) {
        if self.table_factories.is_none() {
            self.table_factories = Some(HashMap::from([(name.to_string(), factory)]));
        } else {
            self.table_factories
                .as_mut()
                .unwrap()
                .insert(name.to_string(), factory);
        }
    }

    /// Add a file format factory to the list of file format factories on this builder
    pub fn add_file_format_factory(&mut self, factory: Arc<dyn FileFormatFactory>) {
        let _ = self.file_format_factories.push(factory);
    }

    /// Add a catalog provider to the list of providers on this builder
    pub fn add_catalog_provider(&mut self, name: &str, factory: Arc<dyn CatalogProvider>) {
        if self.catalog_providers.is_none() {
            self.catalog_providers = Some(HashMap::from([(name.to_string(), factory)]));
        } else {
            self.catalog_providers
                .as_mut()
                .unwrap()
                .insert(name.to_string(), factory);
        }
    }

    /// Return the current [`RuntimeEnv`], creating a default if it doesn't exist
    pub fn runtime_env(&mut self) -> &RuntimeEnv {
        if self.runtime_env.is_none() {
            self.runtime_env = Some(Arc::new(RuntimeEnv::default()));
        }
        self.runtime_env.as_ref().unwrap()
    }

    pub async fn register_extension(
        &mut self,
        config: ExecutionConfig,
        extension: Arc<dyn Extension>,
    ) -> color_eyre::Result<()> {
        extension
            .register(config, self)
            .await
            .map_err(|_| eyre::eyre!("E"))
    }

    pub async fn with_extensions(mut self) -> color_eyre::Result<Self> {
        let extensions = enabled_extensions();

        for extension in extensions {
            let execution_config = self.execution_config.clone();
            self.register_extension(execution_config.unwrap_or_default(), extension)
                .await?;
        }

        Ok(self)
    }

    /// Build the [`SessionState`] from the specified configuration
    pub fn build(self) -> datafusion::common::Result<SessionState> {
        let Self {
            session_config,
            table_factories,
            file_format_factories,
            catalog_providers,
            runtime_env,
            ..
        } = self;

        let mut builder = SessionStateBuilder::new()
            .with_default_features()
            .with_config(session_config);

        if let Some(runtime_env) = runtime_env {
            builder = builder.with_runtime_env(runtime_env);
        }
        if let Some(table_factories) = table_factories {
            builder = builder.with_table_factories(table_factories);
        }
        builder = builder.with_file_formats(file_format_factories);

        if let Some(catalog_providers) = catalog_providers {
            let catalogs_list = MemoryCatalogProviderList::new();
            for (k, v) in catalog_providers {
                catalogs_list.register_catalog(k, v);
            }
            builder = builder.with_catalog_list(Arc::new(catalogs_list));
        }

        Ok(builder.build())
    }
}

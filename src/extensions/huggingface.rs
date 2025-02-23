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

//! Huggingface Integration: [HuggingFaceExtension]

use crate::config::ExecutionConfig;
use crate::extensions::{DftSessionStateBuilder, Extension};
use datafusion::common::DataFusionError;
use log::info;
use std::sync::Arc;

use opendal::{services::Huggingface, Operator};
use url::Url;

#[derive(Debug, Default)]
pub struct HuggingFaceExtension {}

impl HuggingFaceExtension {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Extension for HuggingFaceExtension {
    async fn register(
        &self,
        config: ExecutionConfig,
        builder: &mut DftSessionStateBuilder,
    ) -> datafusion::common::Result<()> {
        let Some(object_store_config) = &config.object_store else {
            return Ok(());
        };

        let Some(huggingface_configs) = &object_store_config.huggingface else {
            return Ok(());
        };

        for huggingface_config in huggingface_configs {
            // I'm not that famliar with Huggingface so I'm not sure what permutations of config
            // values are supposed to work.

            let mut hf_builder = Huggingface::default();
            if let Some(repo_type) = &huggingface_config.repo_type {
                hf_builder = hf_builder.repo_type(repo_type);
            };
            if let Some(revision) = &huggingface_config.revision {
                hf_builder = hf_builder.revision(revision);
            };
            if let Some(root) = &huggingface_config.root {
                hf_builder = hf_builder.root(root);
            };
            if let Some(token) = &huggingface_config.token {
                hf_builder = hf_builder.token(token);
            };
            if let Some(repo_id) = &huggingface_config.repo_id {
                hf_builder = hf_builder.repo_id(repo_id);

                let operator = Operator::new(hf_builder)
                    .map_err(|e| {
                        datafusion::common::error::DataFusionError::External(e.to_string().into())
                    })?
                    .finish();

                let store = object_store_opendal::OpendalStore::new(operator);

                // `repo_id` seems to always have a '/' to separate the organization and repo name
                // but this causes issues with registering external tables and the URLs don't fully
                // reflect the organization and repo name (it only shows the organization name).
                // So we replace the '/' with a '-' so that the URL has both.
                //
                // An example URL to use is:
                //      'hf://huggingfacetb-finemath/finemath-3plus/train-00000-of-00128.parquet'
                //
                // Where 'huggingfacetb' is the organization name and 'finemath' is the repo name
                let url = Url::try_from(format!("hf://{}", repo_id.replace("/", "-")).as_str())
                    .map_err(|e| DataFusionError::External(e.to_string().into()))?;
                info!("Registering store for huggingface url: {url}");
                builder
                    .runtime_env()
                    .register_object_store(&url, Arc::new(store));
            };
        }

        Ok(())
    }
}

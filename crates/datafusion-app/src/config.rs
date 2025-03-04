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

//! Configuration management handling

use std::path::PathBuf;

#[cfg(feature = "udfs-wasm")]
use datafusion_udfs_wasm::WasmInputDataType;
// use directories::{ProjectDirs, UserDirs};
use log::info;
use serde::Deserialize;
#[cfg(feature = "udfs-wasm")]
use std::collections::HashMap;

#[cfg(feature = "s3")]
use color_eyre::Result;
#[cfg(feature = "s3")]
use object_store::aws::{AmazonS3, AmazonS3Builder};

#[derive(Clone, Debug, Deserialize)]
pub struct ExecutionConfig {
    pub object_store: Option<ObjectStoreConfig>,
    #[serde(default = "default_ddl_path")]
    pub ddl_path: Option<PathBuf>,
    #[serde(default = "default_benchmark_iterations")]
    pub benchmark_iterations: usize,
    #[serde(default = "default_cli_batch_size")]
    pub cli_batch_size: usize,
    #[serde(default = "default_tui_batch_size")]
    pub tui_batch_size: usize,
    #[serde(default = "default_flightsql_server_batch_size")]
    pub flightsql_server_batch_size: usize,
    #[serde(default = "default_dedicated_executor_enabled")]
    pub dedicated_executor_enabled: bool,
    #[serde(default = "default_dedicated_executor_threads")]
    pub dedicated_executor_threads: usize,
    #[serde(default = "default_iceberg_config")]
    pub iceberg: IcebergConfig,
    #[cfg(feature = "udfs-wasm")]
    #[serde(default = "default_wasm_udf")]
    pub wasm_udf: WasmUdfConfig,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            object_store: None,
            ddl_path: default_ddl_path(),
            benchmark_iterations: default_benchmark_iterations(),
            cli_batch_size: default_cli_batch_size(),
            tui_batch_size: default_tui_batch_size(),
            flightsql_server_batch_size: default_flightsql_server_batch_size(),
            dedicated_executor_enabled: default_dedicated_executor_enabled(),
            dedicated_executor_threads: default_dedicated_executor_threads(),
            iceberg: default_iceberg_config(),
            #[cfg(feature = "udfs-wasm")]
            wasm_udf: default_wasm_udf(),
        }
    }
}

fn default_ddl_path() -> Option<PathBuf> {
    info!("Creating default ExecutionConfig");
    if let Some(user_dirs) = directories::UserDirs::new() {
        let ddl_path = user_dirs
            .home_dir()
            .join(".config")
            .join("dft")
            .join("ddl.sql");
        Some(ddl_path)
    } else {
        None
    }
}

// TODO - Move this root dft
fn default_cli_batch_size() -> usize {
    8092
}

// TODO - Move this root dft
fn default_tui_batch_size() -> usize {
    100
}

// TODO - Move this root dft
fn default_flightsql_server_batch_size() -> usize {
    8092
}

fn default_benchmark_iterations() -> usize {
    10
}

fn default_dedicated_executor_enabled() -> bool {
    false
}

fn default_dedicated_executor_threads() -> usize {
    // By default we slightly over provision CPUs.  For example, if you have N CPUs available we
    // have N CPUs for the [`DedicatedExecutor`] and 1 for the main / IO runtime.
    //
    // Ref: https://github.com/datafusion-contrib/datafusion-dft/pull/247#discussion_r1848270250
    num_cpus::get()
}

fn default_iceberg_config() -> IcebergConfig {
    IcebergConfig {
        rest_catalogs: Vec::new(),
    }
}

#[cfg(feature = "udfs-wasm")]
fn default_wasm_udf() -> WasmUdfConfig {
    WasmUdfConfig {
        module_functions: HashMap::new(),
    }
}

#[cfg(feature = "s3")]
#[derive(Clone, Debug, Deserialize)]
pub struct S3Config {
    bucket_name: String,
    object_store_url: Option<String>,
    aws_access_key_id: Option<String>,
    aws_secret_access_key: Option<String>,
    _aws_default_region: Option<String>,
    aws_endpoint: Option<String>,
    aws_session_token: Option<String>,
    aws_allow_http: Option<bool>,
}

#[cfg(feature = "s3")]
impl S3Config {
    pub fn object_store_url(&self) -> &Option<String> {
        &self.object_store_url
    }
}

#[cfg(feature = "s3")]
impl S3Config {
    pub fn to_object_store(&self) -> Result<AmazonS3> {
        let mut builder = AmazonS3Builder::new();
        builder = builder.with_bucket_name(&self.bucket_name);
        if let Some(access_key) = &self.aws_access_key_id {
            builder = builder.with_access_key_id(access_key)
        }
        if let Some(secret) = &self.aws_secret_access_key {
            builder = builder.with_secret_access_key(secret)
        }
        if let Some(endpoint) = &self.aws_endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(token) = &self.aws_session_token {
            builder = builder.with_token(token)
        }
        if let Some(allow_http) = &self.aws_allow_http {
            builder = builder.with_allow_http(*allow_http)
        }

        Ok(builder.build()?)
    }
}

#[cfg(feature = "huggingface")]
#[derive(Clone, Debug, Deserialize)]
pub struct HuggingFaceConfig {
    pub repo_type: Option<String>,
    pub repo_id: Option<String>,
    pub revision: Option<String>,
    pub root: Option<String>,
    pub token: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ObjectStoreConfig {
    #[cfg(feature = "s3")]
    pub s3: Option<Vec<S3Config>>,
    #[cfg(feature = "huggingface")]
    pub huggingface: Option<Vec<HuggingFaceConfig>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RestCatalogConfig {
    pub name: String,
    pub addr: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct IcebergConfig {
    pub rest_catalogs: Vec<RestCatalogConfig>,
}

#[cfg(feature = "udfs-wasm")]
#[derive(Clone, Debug, Deserialize)]
pub struct WasmFuncDetails {
    pub name: String,
    pub input_types: Vec<String>,
    pub return_type: String,
    pub input_data_type: WasmInputDataType,
}

#[cfg(feature = "udfs-wasm")]
#[derive(Clone, Debug, Deserialize)]
pub struct WasmUdfConfig {
    pub module_functions: HashMap<PathBuf, Vec<WasmFuncDetails>>,
}

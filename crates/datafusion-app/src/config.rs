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
use serde::Deserialize;
use std::collections::HashMap;

#[cfg(feature = "s3")]
use {
    color_eyre::Result,
    object_store::aws::{AmazonS3, AmazonS3Builder},
};

// Merges a shared config with a priority config. If a field is present in the priority config that
// it replaces the entire field from the shared config.
//
// TODO: Implement full merge so that nested fields can be maintained from the shared config and
// only selected fields are overwritten.
pub fn merge_configs(shared: ExecutionConfig, priority: ExecutionConfig) -> ExecutionConfig {
    // Baseline is the shared config
    let mut merged = shared;

    if let Some(object_store_config) = priority.object_store {
        merged.object_store = Some(object_store_config)
    }
    if let Some(ddl_path) = priority.ddl_path {
        merged.ddl_path = Some(ddl_path)
    }
    if let Some(datafusion) = priority.datafusion {
        merged.datafusion = Some(datafusion)
    }

    if merged.benchmark_iterations != priority.benchmark_iterations {
        merged.benchmark_iterations = priority.benchmark_iterations;
    }
    if merged.dedicated_executor_enabled != priority.dedicated_executor_enabled {
        merged.dedicated_executor_enabled = priority.dedicated_executor_enabled
    }
    if merged.dedicated_executor_threads != priority.dedicated_executor_threads {
        merged.dedicated_executor_threads = priority.dedicated_executor_threads
    }
    if merged.iceberg != priority.iceberg {
        merged.iceberg = priority.iceberg
    }

    #[cfg(feature = "udfs-wasm")]
    if merged.wasm_udf != priority.wasm_udf {
        merged.wasm_udf = priority.wasm_udf
    }

    merged
}

#[derive(Clone, Debug, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub object_store: Option<ObjectStoreConfig>,
    #[serde(default = "default_ddl_path")]
    pub ddl_path: Option<PathBuf>,
    #[serde(default = "default_benchmark_iterations")]
    pub benchmark_iterations: usize,
    #[serde(default)]
    pub datafusion: Option<HashMap<String, String>>,
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
            datafusion: None,
            dedicated_executor_enabled: default_dedicated_executor_enabled(),
            dedicated_executor_threads: default_dedicated_executor_threads(),
            iceberg: default_iceberg_config(),
            #[cfg(feature = "udfs-wasm")]
            wasm_udf: default_wasm_udf(),
        }
    }
}

fn default_ddl_path() -> Option<PathBuf> {
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

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct RestCatalogConfig {
    pub name: String,
    pub addr: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct IcebergConfig {
    pub rest_catalogs: Vec<RestCatalogConfig>,
}

#[cfg(feature = "udfs-wasm")]
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct WasmFuncDetails {
    pub name: String,
    pub input_types: Vec<String>,
    pub return_type: String,
    pub input_data_type: WasmInputDataType,
}

#[cfg(feature = "udfs-wasm")]
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct WasmUdfConfig {
    pub module_functions: HashMap<PathBuf, Vec<WasmFuncDetails>>,
}

#[cfg(feature = "flightsql")]
#[derive(Clone, Default)]
pub struct FlightSQLConfig {
    pub connection_url: String,
    pub benchmark_iterations: usize,
    pub auth: AuthConfig,
}

#[cfg(feature = "flightsql")]
impl FlightSQLConfig {
    pub fn new(connection_url: String, benchmark_iterations: usize, auth: AuthConfig) -> Self {
        Self {
            connection_url,
            benchmark_iterations,
            auth,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct AuthConfig {
    pub basic_auth: Option<BasicAuth>,
    pub bearer_token: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

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

use datafusion_app::config::ExecutionConfig;
#[cfg(feature = "udfs-wasm")]
use datafusion_udfs_wasm::WasmInputDataType;
use directories::{ProjectDirs, UserDirs};
use lazy_static::lazy_static;
use serde::Deserialize;
#[cfg(feature = "udfs-wasm")]
use std::collections::HashMap;

#[cfg(feature = "s3")]
use color_eyre::Result;
// #[cfg(feature = "s3")]
// use object_store::aws::{AmazonS3, AmazonS3Builder};
#[cfg(feature = "flightsql")]
use datafusion_app::config::{AuthConfig, BasicAuth};

lazy_static! {
    pub static ref PROJECT_NAME: String = env!("CARGO_CRATE_NAME").to_uppercase().to_string();
    pub static ref DATA_FOLDER: Option<PathBuf> =
        std::env::var(format!("{}_DATA", PROJECT_NAME.clone()))
            .ok()
            .map(PathBuf::from);
    pub static ref LOG_ENV: String = format!("{}_LOGLEVEL", PROJECT_NAME.clone());
    pub static ref LOG_FILE: String = format!("{}.log", env!("CARGO_PKG_NAME"));
}

fn project_directory() -> PathBuf {
    if let Some(user_dirs) = UserDirs::new() {
        return user_dirs.home_dir().join(".config").join("dft");
    };

    let maybe_project_dirs = ProjectDirs::from("", "", env!("CARGO_PKG_NAME"));
    if let Some(project_dirs) = maybe_project_dirs {
        project_dirs.data_local_dir().to_path_buf()
    } else {
        panic!("No known data directory")
    }
}

pub fn get_data_dir() -> PathBuf {
    if let Some(data_dir) = DATA_FOLDER.clone() {
        data_dir
    } else {
        project_directory()
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_execution_config")]
    pub execution: ExecutionConfig,
    #[serde(default = "default_display_config")]
    pub display: DisplayConfig,
    #[serde(default = "default_interaction_config")]
    pub interaction: InteractionConfig,
    #[cfg(feature = "flightsql")]
    #[serde(default = "default_flightsql_config")]
    pub flightsql: FlightSQLConfig,
    #[serde(default = "default_editor_config")]
    pub editor: EditorConfig,
    #[cfg(feature = "flightsql")]
    #[serde(default = "default_auth_config")]
    pub auth: AuthConfig,
}

fn default_execution_config() -> ExecutionConfig {
    ExecutionConfig::default()
}

fn default_display_config() -> DisplayConfig {
    DisplayConfig::default()
}

fn default_interaction_config() -> InteractionConfig {
    InteractionConfig::default()
}

#[cfg(feature = "flightsql")]
fn default_flightsql_config() -> FlightSQLConfig {
    FlightSQLConfig::default()
}

#[derive(Clone, Debug, Deserialize)]
pub struct DisplayConfig {
    #[serde(default = "default_frame_rate")]
    pub frame_rate: f64,
}

fn default_frame_rate() -> f64 {
    30.0
}

impl Default for DisplayConfig {
    fn default() -> Self {
        Self { frame_rate: 30.0 }
    }
}

// #[cfg(feature = "s3")]
// #[derive(Clone, Debug, Deserialize)]
// pub struct S3Config {
//     bucket_name: String,
//     object_store_url: Option<String>,
//     aws_access_key_id: Option<String>,
//     aws_secret_access_key: Option<String>,
//     _aws_default_region: Option<String>,
//     aws_endpoint: Option<String>,
//     aws_session_token: Option<String>,
//     aws_allow_http: Option<bool>,
// }
//
// #[cfg(feature = "s3")]
// impl S3Config {
//     pub fn object_store_url(&self) -> &Option<String> {
//         &self.object_store_url
//     }
// }
//
// #[cfg(feature = "s3")]
// impl S3Config {
//     pub fn to_object_store(&self) -> Result<AmazonS3> {
//         let mut builder = AmazonS3Builder::new();
//         builder = builder.with_bucket_name(&self.bucket_name);
//         if let Some(access_key) = &self.aws_access_key_id {
//             builder = builder.with_access_key_id(access_key)
//         }
//         if let Some(secret) = &self.aws_secret_access_key {
//             builder = builder.with_secret_access_key(secret)
//         }
//         if let Some(endpoint) = &self.aws_endpoint {
//             builder = builder.with_endpoint(endpoint);
//         }
//         if let Some(token) = &self.aws_session_token {
//             builder = builder.with_token(token)
//         }
//         if let Some(allow_http) = &self.aws_allow_http {
//             builder = builder.with_allow_http(*allow_http)
//         }
//
//         Ok(builder.build()?)
//     }
// }

// #[cfg(feature = "huggingface")]
// #[derive(Clone, Debug, Deserialize)]
// pub struct HuggingFaceConfig {
//     pub repo_type: Option<String>,
//     pub repo_id: Option<String>,
//     pub revision: Option<String>,
//     pub root: Option<String>,
//     pub token: Option<String>,
// }

// #[derive(Clone, Debug, Deserialize)]
// pub struct ObjectStoreConfig {
//     #[cfg(feature = "s3")]
//     pub s3: Option<Vec<S3Config>>,
//     #[cfg(feature = "huggingface")]
//     pub huggingface: Option<Vec<HuggingFaceConfig>>,
// }

// #[derive(Clone, Debug, Deserialize)]
// pub struct ExecutionConfig {
//     pub object_store: Option<ObjectStoreConfig>,
//     #[serde(default = "default_ddl_path")]
//     pub ddl_path: Option<PathBuf>,
//     #[serde(default = "default_benchmark_iterations")]
//     pub benchmark_iterations: usize,
//     #[serde(default = "default_cli_batch_size")]
//     pub cli_batch_size: usize,
//     #[serde(default = "default_tui_batch_size")]
//     pub tui_batch_size: usize,
//     #[serde(default = "default_flightsql_server_batch_size")]
//     pub flightsql_server_batch_size: usize,
//     #[serde(default = "default_dedicated_executor_enabled")]
//     pub dedicated_executor_enabled: bool,
//     #[serde(default = "default_dedicated_executor_threads")]
//     pub dedicated_executor_threads: usize,
//     #[serde(default = "default_iceberg_config")]
//     pub iceberg: IcebergConfig,
//     #[cfg(feature = "udfs-wasm")]
//     #[serde(default = "default_wasm_udf")]
//     pub wasm_udf: WasmUdfConfig,
// }

// fn default_ddl_path() -> Option<PathBuf> {
//     info!("Creating default ExecutionConfig");
//     if let Some(user_dirs) = directories::UserDirs::new() {
//         let ddl_path = user_dirs
//             .home_dir()
//             .join(".config")
//             .join("dft")
//             .join("ddl.sql");
//         Some(ddl_path)
//     } else {
//         None
//     }
// }

fn default_benchmark_iterations() -> usize {
    10
}

// fn default_cli_batch_size() -> usize {
//     8092
// }
//
// fn default_tui_batch_size() -> usize {
//     100
// }
//
// fn default_flightsql_server_batch_size() -> usize {
//     8092
// }

// fn default_dedicated_executor_enabled() -> bool {
//     false
// }

// fn default_dedicated_executor_threads() -> usize {
//     // By default we slightly over provision CPUs.  For example, if you have N CPUs available we
//     // have N CPUs for the [`DedicatedExecutor`] and 1 for the main / IO runtime.
//     //
//     // Ref: https://github.com/datafusion-contrib/datafusion-dft/pull/247#discussion_r1848270250
//     num_cpus::get()
// }

// fn default_iceberg_config() -> IcebergConfig {
//     IcebergConfig {
//         rest_catalogs: Vec::new(),
//     }
// }
//
// #[cfg(feature = "udfs-wasm")]
// fn default_wasm_udf() -> WasmUdfConfig {
//     WasmUdfConfig {
//         module_functions: HashMap::new(),
//     }
// }

// impl Default for ExecutionConfig {
//     fn default() -> Self {
//         Self {
//             object_store: None,
//             ddl_path: default_ddl_path(),
//             benchmark_iterations: default_benchmark_iterations(),
//             cli_batch_size: default_cli_batch_size(),
//             tui_batch_size: default_tui_batch_size(),
//             flightsql_server_batch_size: default_flightsql_server_batch_size(),
//             dedicated_executor_enabled: default_dedicated_executor_enabled(),
//             dedicated_executor_threads: default_dedicated_executor_threads(),
//             iceberg: default_iceberg_config(),
//             #[cfg(feature = "udfs-wasm")]
//             wasm_udf: default_wasm_udf(),
//         }
//     }
// }

// #[derive(Clone, Debug, Deserialize)]
// pub struct RestCatalogConfig {
//     pub name: String,
//     pub addr: String,
// }
//
// #[derive(Clone, Debug, Deserialize)]
// pub struct IcebergConfig {
//     pub rest_catalogs: Vec<RestCatalogConfig>,
// }

// #[cfg(feature = "udfs-wasm")]
// #[derive(Clone, Debug, Deserialize)]
// pub struct WasmFuncDetails {
//     pub name: String,
//     pub input_types: Vec<String>,
//     pub return_type: String,
//     pub input_data_type: WasmInputDataType,
// }
//
// #[cfg(feature = "udfs-wasm")]
// #[derive(Clone, Debug, Deserialize)]
// pub struct WasmUdfConfig {
//     pub module_functions: HashMap<PathBuf, Vec<WasmFuncDetails>>,
// }

#[derive(Clone, Debug, Default, Deserialize)]
pub struct InteractionConfig {
    #[serde(default = "default_mouse")]
    pub mouse: bool,
    #[serde(default = "default_paste")]
    pub paste: bool,
}

fn default_mouse() -> bool {
    false
}

fn default_paste() -> bool {
    false
}

#[cfg(feature = "flightsql")]
#[derive(Clone, Debug, Deserialize)]
pub struct FlightSQLConfig {
    #[serde(default = "default_connection_url")]
    pub connection_url: String,
    #[serde(default = "default_benchmark_iterations")]
    pub benchmark_iterations: usize,
    #[cfg(feature = "experimental-flightsql-server")]
    #[serde(default = "default_server_metrics_port")]
    pub server_metrics_port: String,
}

#[cfg(feature = "flightsql")]
impl Default for FlightSQLConfig {
    fn default() -> Self {
        Self {
            connection_url: default_connection_url(),
            benchmark_iterations: default_benchmark_iterations(),
            #[cfg(feature = "experimental-flightsql-server")]
            server_metrics_port: default_server_metrics_port(),
        }
    }
}

#[cfg(feature = "flightsql")]
pub fn default_connection_url() -> String {
    "http://localhost:50051".to_string()
}

#[cfg(feature = "experimental-flightsql-server")]
fn default_server_metrics_port() -> String {
    "0.0.0.0:9000".to_string()
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct EditorConfig {
    pub experimental_syntax_highlighting: bool,
}

fn default_editor_config() -> EditorConfig {
    EditorConfig::default()
}

#[cfg(feature = "flightsql")]
fn default_auth_config() -> AuthConfig {
    AuthConfig::default()
}

// #[cfg(feature = "flightsql")]
// #[derive(Clone, Debug, Default, Deserialize)]
// pub struct AuthConfig {
//     pub client_basic_auth: Option<BasicAuth>,
//     pub client_bearer_token: Option<String>,
//     pub server_basic_auth: Option<BasicAuth>,
//     pub server_bearer_token: Option<String>,
// }
//
// #[cfg(feature = "auth")]
// #[derive(Clone, Debug, Default, Deserialize)]
// pub struct BasicAuth {
//     pub username: String,
//     pub password: String,
// }

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
use directories::{ProjectDirs, UserDirs};
use lazy_static::lazy_static;
use serde::Deserialize;

#[cfg(feature = "flightsql")]
use datafusion_app::config::AuthConfig;

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
pub struct CliConfig {
    #[serde(default = "default_execution_config")]
    pub execution: ExecutionConfig,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TuiConfig {
    #[serde(default = "default_execution_config")]
    pub execution: ExecutionConfig,
    #[serde(default = "default_display_config")]
    pub display: DisplayConfig,
    #[serde(default = "default_interaction_config")]
    pub interaction: InteractionConfig,
    #[serde(default = "default_editor_config")]
    pub editor: EditorConfig,
}

#[cfg(feature = "flightsql")]
#[derive(Clone, Debug, Deserialize)]
pub struct FlightSQLServerConfig {
    #[serde(default = "default_execution_config")]
    pub execution: ExecutionConfig,
    #[serde(default = "default_connection_url")]
    pub connection_url: String,
    #[serde(default = "default_server_metrics_port")]
    pub server_metrics_port: String,
    #[serde(default = "default_auth_config")]
    pub auth: AuthConfig,
}

impl Default for FlightSQLServerConfig {
    fn default() -> Self {
        Self {
            execution: default_execution_config(),
            connection_url: default_connection_url(),
            server_metrics_port: default_server_metrics_port(),
            auth: default_auth_config(),
        }
    }
}

#[cfg(feature = "flightsql")]
#[derive(Clone, Debug, Deserialize)]
pub struct FlightSQLClientConfig {
    #[serde(default = "default_connection_url")]
    pub connection_url: String,
    #[serde(default = "default_benchmark_iterations")]
    pub benchmark_iterations: usize,
    #[serde(default = "default_auth_config")]
    pub auth: AuthConfig,
}

impl Default for FlightSQLClientConfig {
    fn default() -> Self {
        Self {
            connection_url: default_connection_url(),
            benchmark_iterations: default_benchmark_iterations(),
            auth: default_auth_config(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub shared: ExecutionConfig,
    #[serde(default)]
    pub cli: CliConfig,
    #[serde(default)]
    pub tui: TuiConfig,
    #[cfg(feature = "flightsql")]
    #[serde(default)]
    pub flightsql_client: FlightSQLClientConfig,
    #[cfg(feature = "flightsql")]
    #[serde(default)]
    pub flightsql_server: FlightSQLServerConfig,
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

#[cfg(feature = "flightsql")]
fn default_benchmark_iterations() -> usize {
    10
}

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

// #[cfg(feature = "flightsql")]
// #[derive(Clone, Debug, Deserialize)]
// pub struct FlightSQLConfig {
//     #[serde(default = "default_connection_url")]
//     pub connection_url: String,
//     #[serde(default = "default_benchmark_iterations")]
//     pub benchmark_iterations: usize,
//     #[serde(default = "default_server_metrics_port")]
//     pub server_metrics_port: String,
// }
//
// #[cfg(feature = "flightsql")]
// impl Default for FlightSQLConfig {
//     fn default() -> Self {
//         Self {
//             connection_url: default_connection_url(),
//             benchmark_iterations: default_benchmark_iterations(),
//             server_metrics_port: default_server_metrics_port(),
//         }
//     }
// }

#[cfg(feature = "flightsql")]
pub fn default_connection_url() -> String {
    "http://localhost:50051".to_string()
}

#[cfg(feature = "flightsql")]
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

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

use directories::{ProjectDirs, UserDirs};
use lazy_static::lazy_static;
use log::info;
use serde::Deserialize;

#[cfg(feature = "s3")]
use color_eyre::Result;
#[cfg(feature = "s3")]
use object_store::aws::{AmazonS3, AmazonS3Builder};

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

#[derive(Debug, Default, Deserialize)]
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

#[derive(Debug, Deserialize)]
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

#[derive(Clone, Debug, Deserialize)]
pub struct ObjectStoreConfig {
    #[cfg(feature = "s3")]
    pub s3: Option<Vec<S3Config>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ExecutionConfig {
    pub object_store: Option<ObjectStoreConfig>,
    #[serde(default = "default_ddl_path")]
    pub ddl_path: Option<PathBuf>,
    #[serde(default = "default_benchmark_iterations")]
    pub benchmark_iterations: Option<usize>,
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

fn default_benchmark_iterations() -> Option<usize> {
    Some(10)
}

#[derive(Debug, Default, Deserialize)]
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
}

#[cfg(feature = "flightsql")]
impl Default for FlightSQLConfig {
    fn default() -> Self {
        Self {
            connection_url: default_connection_url(),
        }
    }
}

#[cfg(feature = "flightsql")]
pub fn default_connection_url() -> String {
    "http://localhost:50051".to_string()
}

#[derive(Debug, Default, Deserialize)]
pub struct EditorConfig {
    pub experimental_syntax_highlighting: bool,
}

fn default_editor_config() -> EditorConfig {
    EditorConfig::default()
}

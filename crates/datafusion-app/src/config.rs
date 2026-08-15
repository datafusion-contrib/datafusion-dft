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
    // if merged.iceberg != priority.iceberg {
    //     merged.iceberg = priority.iceberg
    // }

    #[cfg(feature = "udfs-wasm")]
    if !priority.wasm_udf.module_functions.is_empty() {
        merged.wasm_udf = priority.wasm_udf
    }

    #[cfg(feature = "clickhouse")]
    if let Some(clickhouse) = priority.clickhouse {
        merged.clickhouse = Some(clickhouse)
    }

    #[cfg(feature = "mongodb")]
    if let Some(mongodb) = priority.mongodb {
        merged.mongodb = Some(mongodb)
    }

    #[cfg(feature = "net")]
    if let Some(geoip_db_path) = priority.net.geoip_db_path {
        merged.net.geoip_db_path = Some(geoip_db_path)
    }

    merged
}

/// Configuration for the `net` feature
#[cfg(feature = "net")]
#[derive(Clone, Debug, Default, Deserialize)]
pub struct NetConfig {
    /// Path to a MaxMind-format (`.mmdb`) database, such as GeoLite2-City,
    /// used by the single-argument form of the `geoip` UDF. The `GEOIP_DB`
    /// environment variable takes precedence over this value.
    #[serde(default)]
    pub geoip_db_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub object_store: Option<ObjectStoreConfig>,
    #[cfg(feature = "clickhouse")]
    #[serde(default)]
    pub clickhouse: Option<Vec<ClickHouseConfig>>,
    #[cfg(feature = "mongodb")]
    #[serde(default)]
    pub mongodb: Option<Vec<MongoDbConfig>>,
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
    // #[serde(default = "default_iceberg_config")]
    // pub iceberg: IcebergConfig,
    #[cfg(feature = "udfs-wasm")]
    #[serde(default = "default_wasm_udf")]
    pub wasm_udf: WasmUdfConfig,
    #[cfg(feature = "net")]
    #[serde(default)]
    pub net: NetConfig,
    #[serde(default = "default_catalog")]
    pub catalog: CatalogConfig,
    #[cfg(feature = "observability")]
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            object_store: None,
            #[cfg(feature = "clickhouse")]
            clickhouse: None,
            #[cfg(feature = "mongodb")]
            mongodb: None,
            ddl_path: default_ddl_path(),
            benchmark_iterations: default_benchmark_iterations(),
            datafusion: None,
            dedicated_executor_enabled: default_dedicated_executor_enabled(),
            dedicated_executor_threads: default_dedicated_executor_threads(),
            // iceberg: default_iceberg_config(),
            #[cfg(feature = "udfs-wasm")]
            wasm_udf: default_wasm_udf(),
            #[cfg(feature = "net")]
            net: NetConfig::default(),
            catalog: default_catalog(),
            #[cfg(feature = "observability")]
            observability: default_observability(),
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

// fn default_iceberg_config() -> IcebergConfig {
//     IcebergConfig {
//         rest_catalogs: Vec::new(),
//     }
// }

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
    /// Enable AWS credential chain (environment variables, ~/.aws/credentials, IAM roles).
    /// When true, credentials are resolved via the standard AWS credential provider chain.
    /// Static credentials in this config take precedence over environment-based credentials.
    #[serde(default)]
    use_credential_chain: bool,
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
        // Choose builder based on credential chain preference
        let mut builder = if self.use_credential_chain {
            // Use from_env() to enable AWS credential chain
            // This reads AWS_* environment variables and enables:
            // - Environment variable credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
            // - Web identity token authentication (AWS_WEB_IDENTITY_TOKEN_FILE for EKS/IRSA)
            // - Container credentials (ECS via AWS_CONTAINER_CREDENTIALS_RELATIVE_URI)
            // - EC2 instance profile via IMDSv2
            AmazonS3Builder::from_env()
        } else {
            // Traditional static configuration only
            AmazonS3Builder::new()
        };

        // Always set bucket name (required)
        builder = builder.with_bucket_name(&self.bucket_name);

        // Apply TOML-specified credentials if provided
        // These will override environment-based credentials due to precedence
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

#[cfg(feature = "clickhouse")]
fn default_clickhouse_catalog_name() -> String {
    "clickhouse".to_string()
}

#[cfg(any(feature = "clickhouse", feature = "mongodb"))]
fn default_connect_timeout_secs() -> u64 {
    5
}

/// Connection details for a ClickHouse instance that is registered as a catalog. All of the
/// tables from the instance (excluding system tables) are available under the registered catalog
/// name.
#[cfg(feature = "clickhouse")]
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ClickHouseConfig {
    /// Name of the DataFusion catalog the ClickHouse databases and tables are registered under
    #[serde(default = "default_clickhouse_catalog_name")]
    pub name: String,
    /// HTTP(S) url of the ClickHouse instance, for example "http://localhost:8123"
    pub url: String,
    pub user: Option<String>,
    pub password: Option<String>,
    /// Limit the catalog to a single ClickHouse database. When unset all non-system databases
    /// are registered as schemas.
    pub database: Option<String>,
    /// Compression to use for transport ("lz4" or "none")
    pub compression: Option<String>,
    /// Maximum number of seconds to wait for a connection (and initial schema discovery) to
    /// the ClickHouse instance before failing. Set to 0 to disable the timeout.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout: u64,
    /// Additional ClickHouse client settings applied to queries. For example
    /// `output_format_arrow_string_as_string = "1"` returns ClickHouse `String` columns as
    /// Arrow `Utf8` instead of `Binary`.
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[cfg(feature = "clickhouse")]
impl ClickHouseConfig {
    /// Convert to the parameter map expected by
    /// [`datafusion_table_providers::sql::db_connection_pool::clickhousepool::ClickHouseConnectionPool`]
    pub fn to_params(&self) -> HashMap<String, String> {
        let mut params = HashMap::from([("url".to_string(), self.url.clone())]);
        if let Some(user) = &self.user {
            params.insert("user".to_string(), user.clone());
        }
        if let Some(password) = &self.password {
            params.insert("password".to_string(), password.clone());
        }
        if let Some(database) = &self.database {
            params.insert("database".to_string(), database.clone());
        }
        if let Some(compression) = &self.compression {
            params.insert("compression".to_string(), compression.clone());
        }
        for (key, value) in &self.options {
            params.insert(format!("option_{key}"), value.clone());
        }
        params
    }
}

#[cfg(feature = "mongodb")]
fn default_mongodb_catalog_name() -> String {
    "mongodb".to_string()
}

/// Connection details for a MongoDB instance that is registered as a catalog. Databases are
/// exposed as schemas and collections as tables, with Arrow schemas inferred by sampling
/// documents.
#[cfg(feature = "mongodb")]
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct MongoDbConfig {
    /// Name of the DataFusion catalog the MongoDB databases and collections are registered under
    #[serde(default = "default_mongodb_catalog_name")]
    pub name: String,
    /// Full MongoDB connection string, for example
    /// "mongodb://user:pass@localhost:27017/mydb?authSource=admin". When set it takes precedence
    /// over the individual connection fields and the catalog is limited to the database in the
    /// connection string.
    pub connection_string: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<String>,
    /// Limit the catalog to a single MongoDB database. When unset (and no `connection_string` is
    /// provided) all non-system databases are registered as schemas.
    pub database: Option<String>,
    /// Maximum number of seconds to wait for a connection (and initial schema discovery) to
    /// the MongoDB instance before failing. Set to 0 to disable the timeout.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout: u64,
    /// Additional connection parameters passed through to the underlying pool, such as
    /// `auth_source`, `srv`, `sslmode`, `unnest_depth` or `schema_infer_max_records`.
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[cfg(feature = "mongodb")]
impl MongoDbConfig {
    /// Convert to the parameter map expected by
    /// [`datafusion_table_providers::mongodb::connection_pool::MongoDBConnectionPool`]
    pub fn to_params(&self) -> HashMap<String, String> {
        let mut params = HashMap::new();
        if let Some(connection_string) = &self.connection_string {
            params.insert("connection_string".to_string(), connection_string.clone());
        }
        if let Some(host) = &self.host {
            params.insert("host".to_string(), host.clone());
        }
        if let Some(port) = &self.port {
            params.insert("port".to_string(), port.to_string());
        }
        if let Some(user) = &self.user {
            params.insert("user".to_string(), user.clone());
        }
        if let Some(password) = &self.password {
            params.insert("pass".to_string(), password.clone());
        }
        if let Some(database) = &self.database {
            params.insert("db".to_string(), database.clone());
        }
        for (key, value) in &self.options {
            params.insert(key.clone(), value.clone());
        }
        params
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
#[derive(Clone, Debug)]
pub struct FlightSQLConfig {
    pub connection_url: String,
    pub benchmark_iterations: usize,
    pub auth: AuthConfig,
    pub headers: HashMap<String, String>,
    /// Maximum size (in bytes) of a decoded gRPC message. `None` uses tonic's default (4MB).
    pub max_decoding_message_size: Option<usize>,
    /// Maximum size (in bytes) of an encoded gRPC message. `None` uses tonic's default (4MB).
    pub max_encoding_message_size: Option<usize>,
}

#[cfg(feature = "flightsql")]
impl Default for FlightSQLConfig {
    fn default() -> Self {
        Self {
            connection_url: "http://localhost:50051".to_string(),
            benchmark_iterations: 10,
            auth: AuthConfig::default(),
            headers: HashMap::new(),
            max_decoding_message_size: None,
            max_encoding_message_size: None,
        }
    }
}

#[cfg(feature = "flightsql")]
impl FlightSQLConfig {
    pub fn new(
        connection_url: String,
        benchmark_iterations: usize,
        auth: AuthConfig,
        headers: HashMap<String, String>,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    ) -> Self {
        Self {
            connection_url,
            benchmark_iterations,
            auth,
            headers,
            max_decoding_message_size,
            max_encoding_message_size,
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

#[derive(Clone, Debug, Deserialize)]
pub struct CatalogConfig {
    #[serde(default = "default_catalog_name")]
    pub name: String,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            name: default_catalog_name(),
        }
    }
}

fn default_catalog() -> CatalogConfig {
    CatalogConfig::default()
}

fn default_catalog_name() -> String {
    "dft".to_string()
}

#[cfg(feature = "observability")]
#[derive(Clone, Debug, Deserialize)]
pub struct ObservabilityConfig {
    #[serde(default = "default_observability_schema_name")]
    pub schema_name: String,
    #[serde(default = "default_tokio_metrics_enabled")]
    pub tokio_metrics_enabled: bool,
    #[serde(default = "default_tokio_metrics_interval_secs")]
    pub tokio_metrics_interval_secs: u64,
}

#[cfg(feature = "observability")]
impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            schema_name: default_observability_schema_name(),
            tokio_metrics_enabled: default_tokio_metrics_enabled(),
            tokio_metrics_interval_secs: default_tokio_metrics_interval_secs(),
        }
    }
}

#[cfg(feature = "observability")]
fn default_observability() -> ObservabilityConfig {
    ObservabilityConfig::default()
}

#[cfg(feature = "observability")]
fn default_observability_schema_name() -> String {
    "observability".to_string()
}

#[cfg(feature = "observability")]
fn default_tokio_metrics_enabled() -> bool {
    true
}

#[cfg(feature = "observability")]
fn default_tokio_metrics_interval_secs() -> u64 {
    10
}

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

//! [`CatalogProvider`] and [`SchemaProvider`] implementations backed by a ClickHouse instance

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::sql::TableReference;
use datafusion_table_providers::clickhouse::ClickHouseTableFactory;
use datafusion_table_providers::sql::db_connection_pool::clickhousepool::ClickHouseConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
use log::warn;

fn to_external_err(e: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Discover the tables of a ClickHouse database, excluding tables backed by stream-like engines
/// (Kafka, RabbitMQ, etc.). ClickHouse rejects direct selects on those (see its
/// `stream_like_engine_allow_direct_select` setting), so they cannot be queried through the
/// catalog and would break table listing via `information_schema` (which infers the schema of
/// every table by querying it).
async fn non_stream_tables(pool: &ClickHouseConnectionPool, database: &str) -> Result<Vec<String>> {
    pool.client()
        .query(
            "SELECT name FROM system.tables \
             WHERE database = ? AND engine NOT IN ('Kafka', 'RabbitMQ', 'NATS', 'FileLog')",
        )
        .bind(database)
        .fetch_all::<String>()
        .await
        .map_err(to_external_err)
}

/// A [`CatalogProvider`] that exposes the databases of a ClickHouse instance as schemas. The
/// available databases and tables are fetched once when the catalog is created; the data itself
/// is queried from ClickHouse on demand.
#[derive(Debug)]
pub struct ClickHouseCatalogProvider {
    schemas: HashMap<String, Arc<ClickHouseSchemaProvider>>,
}

impl ClickHouseCatalogProvider {
    /// Discover the databases (optionally limited to `database`) and tables available in the
    /// ClickHouse instance behind `pool`
    pub async fn try_new(
        pool: Arc<ClickHouseConnectionPool>,
        database: Option<String>,
    ) -> Result<Self> {
        let client = pool.client();
        let schema_names = match database {
            Some(database) => vec![database],
            None => client.schemas().await.map_err(to_external_err)?,
        };

        let mut schemas = HashMap::with_capacity(schema_names.len());
        for schema_name in schema_names {
            let tables = non_stream_tables(&pool, &schema_name).await?;
            let provider = ClickHouseSchemaProvider {
                pool: Arc::clone(&pool),
                schema_name: schema_name.clone(),
                tables,
            };
            schemas.insert(schema_name, Arc::new(provider));
        }

        Ok(Self { schemas })
    }
}

impl CatalogProvider for ClickHouseCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .map(|s| Arc::clone(s) as Arc<dyn SchemaProvider>)
    }
}

/// A [`SchemaProvider`] for a single ClickHouse database. Table names are cached at creation
/// time; the Arrow schema for a table is only fetched when the table is used in a query.
#[derive(Debug)]
pub struct ClickHouseSchemaProvider {
    pool: Arc<ClickHouseConnectionPool>,
    schema_name: String,
    tables: Vec<String>,
}

#[async_trait::async_trait]
impl SchemaProvider for ClickHouseSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.tables.clone()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if !self.table_exist(name) {
            return Ok(None);
        }
        let factory = ClickHouseTableFactory::new(Arc::clone(&self.pool));
        let table_reference = TableReference::partial(self.schema_name.as_str(), name);
        // Schema inference can fail for tables that exist but are not queryable through the
        // catalog (for example materialized views whose stored query reads from a stream-like
        // engine table, or views that error when executed). Hide those tables instead of
        // erroring so that they don't break listing the remaining tables via
        // `information_schema`.
        match factory.table_provider(table_reference, None).await {
            Ok(provider) => Ok(Some(provider)),
            Err(e) => {
                warn!(
                    "Failed to infer schema for ClickHouse table '{}.{}', hiding it from the catalog: {e}",
                    self.schema_name, name
                );
                Ok(None)
            }
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.iter().any(|t| t == name)
    }
}

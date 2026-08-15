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

//! [`CatalogProvider`] and [`SchemaProvider`] implementations backed by a MongoDB instance

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::sql::TableReference;
use datafusion_table_providers::mongodb::connection_pool::MongoDBConnectionPool;
use datafusion_table_providers::mongodb::MongoDBTableFactory;
use datafusion_table_providers::util::secrets::to_secret_map;

fn to_external_err(e: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// MongoDB databases that are excluded when discovering schemas
const SYSTEM_DATABASES: [&str; 3] = ["admin", "config", "local"];

/// A [`CatalogProvider`] that exposes the databases of a MongoDB instance as schemas and its
/// collections as tables. The available databases and collections are fetched once when the
/// catalog is created; the data itself is queried from MongoDB on demand.
///
/// The underlying connection pool is bound to a single database, so when no database is
/// configured one pool per discovered database is created.
#[derive(Debug)]
pub struct MongoDbCatalogProvider {
    schemas: HashMap<String, Arc<MongoDbSchemaProvider>>,
}

impl MongoDbCatalogProvider {
    /// Discover the databases and collections available in the MongoDB instance described by
    /// `params`. When `params` pins a database (via the `db` key or a `connection_string`) only
    /// that database is registered as a schema; otherwise all non-system databases are.
    pub async fn try_new(params: HashMap<String, String>) -> Result<Self> {
        let single_database = params.contains_key("db") || params.contains_key("connection_string");
        let base_pool = Arc::new(
            MongoDBConnectionPool::new(to_secret_map(params.clone()))
                .await
                .map_err(to_external_err)?,
        );
        let base_conn = base_pool.connect().await.map_err(to_external_err)?;

        let mut schemas = HashMap::new();
        if single_database {
            let schema_name = base_conn.db_name.clone();
            let provider = Self::build_schema(base_pool).await?;
            schemas.insert(schema_name, provider);
        } else {
            let database_names = base_conn
                .client
                .list_database_names()
                .await
                .map_err(to_external_err)?;
            for database_name in database_names {
                if SYSTEM_DATABASES.contains(&database_name.as_str()) {
                    continue;
                }
                let mut db_params = params.clone();
                db_params.insert("db".to_string(), database_name.clone());
                let pool = Arc::new(
                    MongoDBConnectionPool::new(to_secret_map(db_params))
                        .await
                        .map_err(to_external_err)?,
                );
                let provider = Self::build_schema(pool).await?;
                schemas.insert(database_name, provider);
            }
        }

        Ok(Self { schemas })
    }

    async fn build_schema(pool: Arc<MongoDBConnectionPool>) -> Result<Arc<MongoDbSchemaProvider>> {
        let conn = pool.connect().await.map_err(to_external_err)?;
        let tables = conn.tables().await.map_err(to_external_err)?;
        Ok(Arc::new(MongoDbSchemaProvider { pool, tables }))
    }
}

impl CatalogProvider for MongoDbCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .map(|s| Arc::clone(s) as Arc<dyn SchemaProvider>)
    }
}

/// A [`SchemaProvider`] for a single MongoDB database. Collection names are cached at creation
/// time; the Arrow schema for a collection is only inferred (by sampling documents) when the
/// collection is used in a query.
#[derive(Debug)]
pub struct MongoDbSchemaProvider {
    pool: Arc<MongoDBConnectionPool>,
    tables: Vec<String>,
}

#[async_trait::async_trait]
impl SchemaProvider for MongoDbSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.tables.clone()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if !self.table_exist(name) {
            return Ok(None);
        }
        let factory = MongoDBTableFactory::new(Arc::clone(&self.pool));
        // The pool is bound to this schema's database so only the collection name is needed
        let table_reference = TableReference::bare(name);
        let provider = factory
            .table_provider(table_reference)
            .await
            .map_err(DataFusionError::External)?;
        Ok(Some(provider))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.iter().any(|t| t == name)
    }
}

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

use std::sync::Arc;

use color_eyre::{Report, Result};
use datafusion::{
    catalog::{MemoryCatalogProvider, MemorySchemaProvider},
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    prelude::SessionContext,
};
use log::info;
use serde::Deserialize;
use url::Url;

#[derive(Debug, Clone, Deserialize)]
pub struct DbConfig {
    #[serde(default = "default_db_path")]
    pub path: Url,
}

impl Default for DbConfig {
    fn default() -> Self {
        default_db_config()
    }
}

pub fn default_db_config() -> DbConfig {
    DbConfig {
        path: default_db_path(),
    }
}

fn default_db_path() -> Url {
    let base = directories::BaseDirs::new().expect("Base directories should be available");
    let path = base
        .data_dir()
        .to_path_buf()
        .join("dft/")
        .to_str()
        .unwrap()
        .to_string();
    let with_schema = format!("file://{path}");
    Url::parse(&with_schema).unwrap()
}

pub async fn register_db(ctx: &SessionContext, db_config: &DbConfig) -> Result<()> {
    info!("registering tables to database");
    let tables_url = db_config.path.join("tables")?;
    let listing_tables_url = ListingTableUrl::parse(tables_url.clone())?;
    let store_url = listing_tables_url.object_store();
    let store = ctx.runtime_env().object_store(store_url)?;
    let tables_path = object_store::path::Path::from_url_path(tables_url.path())?;
    let catalogs = store.list_with_delimiter(Some(&tables_path)).await?;
    for catalog in catalogs.common_prefixes {
        let catalog_name = catalog
            .filename()
            .ok_or(Report::msg("missing catalog name"))?;
        info!("...handling {catalog_name} catalog");
        let maybe_catalog = ctx.catalog(catalog_name);
        let catalog_provider = match maybe_catalog {
            Some(catalog) => catalog,
            None => {
                info!("...catalog does not exist, createing");
                let mem_catalog_provider = Arc::new(MemoryCatalogProvider::new());
                ctx.register_catalog(catalog_name, mem_catalog_provider);
                ctx.catalog(catalog_name).ok_or(Report::msg(format!(
                    "missing catalog {catalog_name}, shouldnt be possible"
                )))?
            }
        };
        let schemas = store.list_with_delimiter(Some(&catalog)).await?;
        for schema in schemas.common_prefixes {
            let schema_name = schema
                .filename()
                .ok_or(Report::msg("missing schema name"))?;
            info!("...handling {schema_name} schema");
            let maybe_schema = catalog_provider.schema(schema_name);
            let schema_provider = match maybe_schema {
                Some(schema) => schema,
                None => {
                    info!("...schema does not exist, creating");
                    let mem_schema_provider = Arc::new(MemorySchemaProvider::new());
                    catalog_provider.register_schema(schema_name, mem_schema_provider)?;
                    catalog_provider
                        .schema(schema_name)
                        .ok_or(Report::msg(format!(
                            "missing schema {schema_name}, shouldnt be possible"
                        )))?
                }
            };
            let tables = store.list_with_delimiter(Some(&schema)).await?;
            for table_path in tables.common_prefixes {
                let table_name = table_path
                    .filename()
                    .ok_or(Report::msg("missing table name"))?;
                info!("...handling table \"{catalog_name}.{schema_name}.{table_name}\"");

                let p = tables_url
                    .join(&format!("{catalog_name}/"))?
                    .join(&format!("{schema_name}/"))?
                    .join(&format!("{table_name}/"))?;
                let table_url = ListingTableUrl::parse(p)?;
                let file_format = ParquetFormat::new();
                let listing_options =
                    ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");
                // Resolve the schema
                let resolved_schema = listing_options
                    .infer_schema(&ctx.state(), &table_url)
                    .await?;
                let config = ListingTableConfig::new(table_url)
                    .with_listing_options(listing_options)
                    .with_schema(resolved_schema);
                // Create a new TableProvider
                let provider = Arc::new(ListingTable::try_new(config)?);
                info!("...table registered");
                schema_provider.register_table(table_name.to_string(), provider)?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use datafusion::{
        assert_batches_eq,
        dataframe::DataFrameWriteOptions,
        prelude::{SessionConfig, SessionContext},
    };

    use crate::{config::DbConfig, db::register_db};

    fn setup() -> SessionContext {
        let config = SessionConfig::default().with_information_schema(true);
        SessionContext::new_with_config(config)
    }

    #[tokio::test]
    async fn test_register_db_no_tables() {
        let ctx = setup();
        let config = DbConfig::default();

        register_db(&ctx, &config).await.unwrap();

        let batches = ctx
            .sql("SHOW TABLES")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+---------------+--------------------+-------------+------------+",
            "| table_catalog | table_schema       | table_name  | table_type |",
            "+---------------+--------------------+-------------+------------+",
            "| datafusion    | information_schema | tables      | VIEW       |",
            "| datafusion    | information_schema | views       | VIEW       |",
            "| datafusion    | information_schema | columns     | VIEW       |",
            "| datafusion    | information_schema | df_settings | VIEW       |",
            "| datafusion    | information_schema | schemata    | VIEW       |",
            "| datafusion    | information_schema | routines    | VIEW       |",
            "| datafusion    | information_schema | parameters  | VIEW       |",
            "+---------------+--------------------+-------------+------------+",
        ];

        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_register_db_single_table() {
        let ctx = setup();
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let path = format!("file://{}/", db_path.to_str().unwrap());
        let db_url = url::Url::parse(&path).unwrap();
        let config = DbConfig { path: db_url };
        let data_path = db_path.join("tables").join("dft").join("stuff").join("hi");

        let df = ctx.sql("SELECT 1").await.unwrap();
        let write_opts = DataFrameWriteOptions::new();

        df.write_parquet(data_path.as_path().to_str().unwrap(), write_opts, None)
            .await
            .unwrap();

        register_db(&ctx, &config).await.unwrap();

        let batches = ctx
            .sql("SELECT * FROM information_schema.tables ORDER BY table_catalog, table_schema, table_name")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+---------------+--------------------+-------------+------------+",
            "| table_catalog | table_schema       | table_name  | table_type |",
            "+---------------+--------------------+-------------+------------+",
            "| datafusion    | information_schema | columns     | VIEW       |",
            "| datafusion    | information_schema | df_settings | VIEW       |",
            "| datafusion    | information_schema | parameters  | VIEW       |",
            "| datafusion    | information_schema | routines    | VIEW       |",
            "| datafusion    | information_schema | schemata    | VIEW       |",
            "| datafusion    | information_schema | tables      | VIEW       |",
            "| datafusion    | information_schema | views       | VIEW       |",
            "| dft           | information_schema | columns     | VIEW       |",
            "| dft           | information_schema | df_settings | VIEW       |",
            "| dft           | information_schema | parameters  | VIEW       |",
            "| dft           | information_schema | routines    | VIEW       |",
            "| dft           | information_schema | schemata    | VIEW       |",
            "| dft           | information_schema | tables      | VIEW       |",
            "| dft           | information_schema | views       | VIEW       |",
            "| dft           | stuff              | hi          | BASE TABLE |",
            "+---------------+--------------------+-------------+------------+",
        ];

        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_register_db_multiple_tables() {
        let ctx = setup();
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let path = format!("file://{}/", db_path.to_str().unwrap());
        let db_url = url::Url::parse(&path).unwrap();
        let config = DbConfig { path: db_url };
        let data_1_path = db_path.join("tables").join("dft").join("stuff").join("hi");
        let data_2_path = db_path.join("tables").join("dft").join("stuff").join("bye");

        let df = ctx.sql("SELECT 1").await.unwrap();
        let write_opts = DataFrameWriteOptions::new();
        df.clone()
            .write_parquet(data_1_path.as_path().to_str().unwrap(), write_opts, None)
            .await
            .unwrap();

        let write_opts = DataFrameWriteOptions::new();
        df.write_parquet(data_2_path.as_path().to_str().unwrap(), write_opts, None)
            .await
            .unwrap();

        register_db(&ctx, &config).await.unwrap();

        let batches = ctx
            .sql("SELECT * FROM information_schema.tables ORDER BY table_catalog, table_schema, table_name")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+---------------+--------------------+-------------+------------+",
            "| table_catalog | table_schema       | table_name  | table_type |",
            "+---------------+--------------------+-------------+------------+",
            "| datafusion    | information_schema | columns     | VIEW       |",
            "| datafusion    | information_schema | df_settings | VIEW       |",
            "| datafusion    | information_schema | parameters  | VIEW       |",
            "| datafusion    | information_schema | routines    | VIEW       |",
            "| datafusion    | information_schema | schemata    | VIEW       |",
            "| datafusion    | information_schema | tables      | VIEW       |",
            "| datafusion    | information_schema | views       | VIEW       |",
            "| dft           | information_schema | columns     | VIEW       |",
            "| dft           | information_schema | df_settings | VIEW       |",
            "| dft           | information_schema | parameters  | VIEW       |",
            "| dft           | information_schema | routines    | VIEW       |",
            "| dft           | information_schema | schemata    | VIEW       |",
            "| dft           | information_schema | tables      | VIEW       |",
            "| dft           | information_schema | views       | VIEW       |",
            "| dft           | stuff              | bye         | BASE TABLE |",
            "| dft           | stuff              | hi          | BASE TABLE |",
            "+---------------+--------------------+-------------+------------+",
        ];

        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_register_db_multiple_schemas() {
        let ctx = setup();
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let path = format!("file://{}/", db_path.to_str().unwrap());
        let db_url = url::Url::parse(&path).unwrap();
        let config = DbConfig { path: db_url };
        let data_1_path = db_path.join("tables").join("dft").join("stuff").join("hi");
        let data_2_path = db_path
            .join("tables")
            .join("dft")
            .join("things")
            .join("bye");

        let df = ctx.sql("SELECT 1").await.unwrap();
        let write_opts = DataFrameWriteOptions::new();
        df.clone()
            .write_parquet(data_1_path.as_path().to_str().unwrap(), write_opts, None)
            .await
            .unwrap();

        let write_opts = DataFrameWriteOptions::new();
        df.write_parquet(data_2_path.as_path().to_str().unwrap(), write_opts, None)
            .await
            .unwrap();

        register_db(&ctx, &config).await.unwrap();

        let batches = ctx
            .sql("SELECT * FROM information_schema.tables ORDER BY table_catalog, table_schema, table_name")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+---------------+--------------------+-------------+------------+",
            "| table_catalog | table_schema       | table_name  | table_type |",
            "+---------------+--------------------+-------------+------------+",
            "| datafusion    | information_schema | columns     | VIEW       |",
            "| datafusion    | information_schema | df_settings | VIEW       |",
            "| datafusion    | information_schema | parameters  | VIEW       |",
            "| datafusion    | information_schema | routines    | VIEW       |",
            "| datafusion    | information_schema | schemata    | VIEW       |",
            "| datafusion    | information_schema | tables      | VIEW       |",
            "| datafusion    | information_schema | views       | VIEW       |",
            "| dft           | information_schema | columns     | VIEW       |",
            "| dft           | information_schema | df_settings | VIEW       |",
            "| dft           | information_schema | parameters  | VIEW       |",
            "| dft           | information_schema | routines    | VIEW       |",
            "| dft           | information_schema | schemata    | VIEW       |",
            "| dft           | information_schema | tables      | VIEW       |",
            "| dft           | information_schema | views       | VIEW       |",
            "| dft           | stuff              | hi          | BASE TABLE |",
            "| dft           | things             | bye         | BASE TABLE |",
            "+---------------+--------------------+-------------+------------+",
        ];

        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_register_db_multiple_catalogs() {
        let ctx = setup();
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db");
        let path = format!("file://{}/", db_path.to_str().unwrap());
        let db_url = url::Url::parse(&path).unwrap();
        let config = DbConfig { path: db_url };
        let data_1_path = db_path.join("tables").join("dft2").join("stuff").join("hi");
        let data_2_path = db_path
            .join("tables")
            .join("dft")
            .join("things")
            .join("bye");

        let df = ctx.sql("SELECT 1").await.unwrap();
        let write_opts = DataFrameWriteOptions::new();
        df.clone()
            .write_parquet(data_1_path.as_path().to_str().unwrap(), write_opts, None)
            .await
            .unwrap();

        let write_opts = DataFrameWriteOptions::new();
        df.write_parquet(data_2_path.as_path().to_str().unwrap(), write_opts, None)
            .await
            .unwrap();

        register_db(&ctx, &config).await.unwrap();

        let batches = ctx
            .sql("SELECT * FROM information_schema.tables ORDER BY table_catalog, table_schema, table_name")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+---------------+--------------------+-------------+------------+",
            "| table_catalog | table_schema       | table_name  | table_type |",
            "+---------------+--------------------+-------------+------------+",
            "| datafusion    | information_schema | columns     | VIEW       |",
            "| datafusion    | information_schema | df_settings | VIEW       |",
            "| datafusion    | information_schema | parameters  | VIEW       |",
            "| datafusion    | information_schema | routines    | VIEW       |",
            "| datafusion    | information_schema | schemata    | VIEW       |",
            "| datafusion    | information_schema | tables      | VIEW       |",
            "| datafusion    | information_schema | views       | VIEW       |",
            "| dft           | information_schema | columns     | VIEW       |",
            "| dft           | information_schema | df_settings | VIEW       |",
            "| dft           | information_schema | parameters  | VIEW       |",
            "| dft           | information_schema | routines    | VIEW       |",
            "| dft           | information_schema | schemata    | VIEW       |",
            "| dft           | information_schema | tables      | VIEW       |",
            "| dft           | information_schema | views       | VIEW       |",
            "| dft           | things             | bye         | BASE TABLE |",
            "| dft2          | information_schema | columns     | VIEW       |",
            "| dft2          | information_schema | df_settings | VIEW       |",
            "| dft2          | information_schema | parameters  | VIEW       |",
            "| dft2          | information_schema | routines    | VIEW       |",
            "| dft2          | information_schema | schemata    | VIEW       |",
            "| dft2          | information_schema | tables      | VIEW       |",
            "| dft2          | information_schema | views       | VIEW       |",
            "| dft2          | stuff              | hi          | BASE TABLE |",
            "+---------------+--------------------+-------------+------------+",
        ];

        assert_batches_eq!(expected, &batches);
    }
}

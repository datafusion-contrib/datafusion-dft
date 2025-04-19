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

use std::{fs::read_dir, sync::Arc};

use color_eyre::{Report, Result};
use datafusion::{
    catalog::{MemoryCatalogProvider, MemorySchemaProvider},
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    prelude::SessionContext,
};
use log::{error, info};

use crate::config::DbConfig;

pub async fn register_db(ctx: &SessionContext, db_config: &DbConfig) -> Result<()> {
    info!("registering tables to database");
    let catalogs = read_dir(db_config.path.join("tables"))?;
    info!("...reading catalogs");
    for maybe_catalog in catalogs {
        let catalog = maybe_catalog?;
        let catalog_file_name = catalog.file_name();
        let catalog_name = catalog_file_name.to_str().ok_or(Report::msg(format!(
            "invalid catalog path {catalog_file_name:?}"
        )))?;
        // Every catalog should be a directory
        if !catalog.path().is_dir() {
            error!("catalog {catalog_name:?} is not a directory, skipping");
            continue;
        }
        let catalog_path = catalog.path();
        info!("...handling {:?} catalog", catalog_name);
        let maybe_catalog = ctx.catalog(catalog_name);
        let catalog_provider = match maybe_catalog {
            None => {
                info!("...catalog does not exist, createing");
                let mem_catalog_provider = Arc::new(MemoryCatalogProvider::new());
                ctx.register_catalog(catalog_name, mem_catalog_provider)
                    .ok_or(Report::msg(format!(
                        "missing catalog {catalog_name}, shouldnt be possible"
                    )))?
            }
            Some(catalog) => catalog,
        };
        for maybe_schema in read_dir(&catalog_path)? {
            let schema = maybe_schema?;
            let schema_file_name = schema.file_name();
            let schema_name = schema_file_name.to_str().ok_or(Report::msg(format!(
                "invalid schema path {schema_file_name:?}"
            )))?;
            // Every schema should be a directory
            if !schema.path().is_dir() {
                error!("schema {schema_name:?} is not a directory, skipping",);
                continue;
            }
            let schema_path = schema.path();
            info!("...handling {:?} schema", schema_name);
            let maybe_schema = catalog_provider.schema(schema_name);
            let schema_provider = match maybe_schema {
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
                Some(schema) => schema,
            };
            for maybe_table in read_dir(schema_path)? {
                let table = maybe_table?;
                // Every table should be a directory even if there is a single data file
                if !table.path().is_dir() {
                    error!("table {:?} is not a directory, skipping", catalog.path());
                    continue;
                }
                let table_path = table.path();
                let table_file_name = table.file_name();
                let table_name = table_file_name.to_str().ok_or(Report::msg(format!(
                    "invalid table path {table_file_name:?}"
                )))?;
                info!("...handling table {table_name:?}");
                let table_url = ListingTableUrl::parse(table_path.to_str().ok_or(Report::msg(
                    format!("Invalid table path for {table_path:?}"),
                ))?)?;
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
                info!("...registering {table_name}");
                schema_provider.register_table(table_name.to_string(), provider)?;
            }
        }
    }
    Ok(())
}

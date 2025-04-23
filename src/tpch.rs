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

use color_eyre::{eyre, Result};
use datafusion::{arrow::record_batch::RecordBatch, datasource::listing::ListingTableUrl};
use datafusion_app::{
    config::merge_configs, extensions::DftSessionStateBuilder, local::ExecutionContext,
};
use log::info;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow, RegionArrow,
    SupplierArrow,
};
use url::Url;

use crate::config::AppConfig;

enum GeneratorType {
    Customer,
    Order,
    LineItem,
    Nation,
    Part,
    PartSupp,
    Region,
    Supplier,
}

impl TryFrom<&str> for GeneratorType {
    type Error = color_eyre::Report;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        // `/` suffix is used so that the final path part is interpretted as a directory
        match value {
            "customer/" => Ok(Self::Customer),
            "orders/" => Ok(Self::Order),
            "lineitem/" => Ok(Self::LineItem),
            "nation/" => Ok(Self::Nation),
            "part/" => Ok(Self::Part),
            "partsupp/" => Ok(Self::PartSupp),
            "region/" => Ok(Self::Region),
            "supplier/" => Ok(Self::Supplier),
            _ => Err(eyre::Report::msg(format!("unknown generator type {value}"))),
        }
    }
}

fn create_tpch_dirs(config: &AppConfig) -> Result<Vec<(GeneratorType, Url)>> {
    info!("...configured DB directory is {:?}", config.db.path);
    // `/` suffix is used so that the final path part is interpretted as a directory
    let tpch_dir = config
        .db
        .path
        .join("tables/")?
        .join("dft/")?
        .join("tpch/")?;
    let needed_dirs = [
        "customer/",
        "orders/",
        "lineitem/",
        "nation/",
        "part/",
        "partsupp/",
        "region/",
        "supplier/",
    ];
    let mut table_paths = Vec::new();
    for dir in needed_dirs {
        let table_path = tpch_dir.join(dir)?;
        info!("table path {:?} for table {dir}", table_path.path());
        table_paths.push((GeneratorType::try_from(dir)?, table_path))
    }
    Ok(table_paths)
}

async fn write_batches_to_parquet<I>(
    mut batches: std::iter::Peekable<I>,
    table_path: &Url,
    table_type: &str,
    store: Arc<dyn ObjectStore>,
) -> Result<()>
where
    I: Iterator<Item = RecordBatch>,
{
    let first = batches.peek().ok_or(eyre::Error::msg(format!(
        "unable to generate {table_type} TPC-H data"
    )))?;

    let file_url = table_path.join("data.parquet")?;
    info!("...file URL '{file_url}'");
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, Arc::clone(first.schema_ref()), None)?;
        info!("...writing {table_type} batches");
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.finish()?;
    }
    let file_path = object_store::path::Path::from_url_path(file_url.path())?;
    info!("...putting to file path {}", file_path);
    store.put(&file_path, buf.into()).await?;
    Ok(())
}

pub async fn generate(config: AppConfig, scale_factor: f64) -> Result<()> {
    let merged_exec_config = merge_configs(config.shared.clone(), config.cli.execution.clone());
    let session_state_builder = DftSessionStateBuilder::try_new(Some(merged_exec_config.clone()))?
        .with_extensions()
        .await?;

    let session_state = session_state_builder.build()?;
    let execution_ctx = ExecutionContext::try_new(
        &merged_exec_config,
        session_state,
        crate::APP_NAME,
        env!("CARGO_PKG_VERSION"),
    )?;

    let tables_path = config.db.path.join("tables")?;
    let tables_url = ListingTableUrl::parse(tables_path)?;
    let store_url = tables_url.object_store();
    let store = execution_ctx
        .session_ctx()
        .runtime_env()
        .object_store(store_url)?;
    info!("configured db store: {store:?}");
    info!("generating TPC-H data");
    let table_paths = create_tpch_dirs(&config)?;
    for (table, table_path) in table_paths {
        match table {
            GeneratorType::Customer => {
                info!("...generating customers");
                let arrow_generator =
                    CustomerArrow::new(CustomerGenerator::new(scale_factor, 1, 1));
                write_batches_to_parquet(
                    arrow_generator.peekable(),
                    &table_path,
                    "Customer",
                    Arc::clone(&store),
                )
                .await?;
            }
            GeneratorType::Order => {
                info!("...generating orders");
                let arrow_generator = OrderArrow::new(OrderGenerator::new(scale_factor, 1, 1));
                write_batches_to_parquet(
                    arrow_generator.peekable(),
                    &table_path,
                    "Order",
                    Arc::clone(&store),
                )
                .await?;
            }
            GeneratorType::LineItem => {
                info!("...generating LineItems");
                let arrow_generator =
                    LineItemArrow::new(LineItemGenerator::new(scale_factor, 1, 1));
                write_batches_to_parquet(
                    arrow_generator.peekable(),
                    &table_path,
                    "LineItem",
                    Arc::clone(&store),
                )
                .await?;
            }
            GeneratorType::Nation => {
                info!("...generating Nations");
                let arrow_generator = NationArrow::new(NationGenerator::new(scale_factor, 1, 1));
                write_batches_to_parquet(
                    arrow_generator.peekable(),
                    &table_path,
                    "Nation",
                    Arc::clone(&store),
                )
                .await?;
            }
            GeneratorType::Part => {
                info!("...generating Parts");
                let arrow_generator = PartArrow::new(PartGenerator::new(scale_factor, 1, 1));
                write_batches_to_parquet(
                    arrow_generator.peekable(),
                    &table_path,
                    "Part",
                    Arc::clone(&store),
                )
                .await?;
            }
            GeneratorType::PartSupp => {
                info!("...generating PartSupps");
                let arrow_generator =
                    PartSuppArrow::new(PartSuppGenerator::new(scale_factor, 1, 1));
                write_batches_to_parquet(
                    arrow_generator.peekable(),
                    &table_path,
                    "PartSupp",
                    Arc::clone(&store),
                )
                .await?;
            }
            GeneratorType::Region => {
                info!("...generating Regions");
                let arrow_generator = RegionArrow::new(RegionGenerator::new(scale_factor, 1, 1));
                write_batches_to_parquet(
                    arrow_generator.peekable(),
                    &table_path,
                    "Region",
                    Arc::clone(&store),
                )
                .await?;
            }
            GeneratorType::Supplier => {
                info!("...generating Suppliers");
                let arrow_generator =
                    SupplierArrow::new(SupplierGenerator::new(scale_factor, 1, 1));
                write_batches_to_parquet(
                    arrow_generator.peekable(),
                    &table_path,
                    "Supplier",
                    Arc::clone(&store),
                )
                .await?;
            }
        }
    }

    Ok(())
}

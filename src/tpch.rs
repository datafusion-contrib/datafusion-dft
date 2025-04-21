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

use std::{
    fs::create_dir_all,
    path::{Path, PathBuf},
    sync::Arc,
};

use color_eyre::{eyre, Result};
use datafusion::arrow::record_batch::RecordBatch;
use log::info;
use parquet::arrow::ArrowWriter;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow, RegionArrow,
    SupplierArrow,
};

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
        match value {
            "customer" => Ok(Self::Customer),
            "orders" => Ok(Self::Order),
            "lineitem" => Ok(Self::LineItem),
            "nation" => Ok(Self::Nation),
            "part" => Ok(Self::Part),
            "partsupp" => Ok(Self::PartSupp),
            "region" => Ok(Self::Region),
            "supplier" => Ok(Self::Supplier),
            _ => Err(eyre::Report::msg(format!("Unknown generator type {value}"))),
        }
    }
}

fn create_tpch_dirs(config: &AppConfig) -> Result<Vec<(GeneratorType, PathBuf)>> {
    info!("...configured DB directory is {:?}", config.db.path);
    if config.db.path.is_file() {
        eyre::bail!("config DB directory is a file and it must be a directory")
    }

    if !config.db.path.exists() {
        info!("...DB directory does not exist, creating");
        std::fs::create_dir_all(config.db.path.clone())?;
    } else {
        info!("...DB directory exists");
    }
    let catalog_dir = config.db.path.join("tables").join("dft");
    let tpch_dir = catalog_dir.join("tpch");
    if !tpch_dir.exists() {
        info!(
            "...TPC-H table directory ({:?}) does not exist, creating",
            config.db.path
        );
        create_dir_all(&tpch_dir)?;
    } else {
        info!("...TPC-H table directory ({tpch_dir:?}) exists");
    };
    let needed_dirs = [
        "customer", "orders", "lineitem", "nation", "part", "partsupp", "region", "supplier",
    ];
    let mut table_paths = Vec::new();
    for dir in needed_dirs {
        let table_path = tpch_dir.join(dir);
        create_dir_all(&table_path)?;
        table_paths.push((GeneratorType::try_from(dir)?, table_path))
    }
    Ok(table_paths)
}

fn write_batches_to_parquet<I>(
    mut batches: std::iter::Peekable<I>,
    table_path: &Path,
    table_type: &str,
) -> Result<()>
where
    I: Iterator<Item = RecordBatch>,
{
    let first = batches.peek().ok_or(eyre::Error::msg(format!(
        "unable to generate {table_type} TPC-H data"
    )))?;

    let file_path = table_path.join("data.parquet");
    let file = std::fs::File::create(file_path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(first.schema_ref()), None)?;
    info!("...writing {table_type} batches");
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.finish()?;
    Ok(())
}

pub fn generate(config: AppConfig, scale_factor: f64) -> Result<()> {
    info!("Generating TPC-H data");
    let table_paths = create_tpch_dirs(&config)?;
    for (table, table_path) in table_paths {
        if table_path.is_dir() {
            match table {
                GeneratorType::Customer => {
                    info!("...generating customers");
                    let arrow_generator =
                        CustomerArrow::new(CustomerGenerator::new(scale_factor, 1, 1));
                    write_batches_to_parquet(arrow_generator.peekable(), &table_path, "Customer")?;
                }
                GeneratorType::Order => {
                    info!("...generating orders");
                    let arrow_generator = OrderArrow::new(OrderGenerator::new(scale_factor, 1, 1));
                    write_batches_to_parquet(arrow_generator.peekable(), &table_path, "Order")?;
                }
                GeneratorType::LineItem => {
                    info!("...generating LineItems");
                    let arrow_generator =
                        LineItemArrow::new(LineItemGenerator::new(scale_factor, 1, 1));
                    write_batches_to_parquet(arrow_generator.peekable(), &table_path, "LineItem")?;
                }
                GeneratorType::Nation => {
                    info!("...generating Nations");
                    let arrow_generator =
                        NationArrow::new(NationGenerator::new(scale_factor, 1, 1));
                    write_batches_to_parquet(arrow_generator.peekable(), &table_path, "Nation")?;
                }
                GeneratorType::Part => {
                    info!("...generating Parts");
                    let arrow_generator = PartArrow::new(PartGenerator::new(scale_factor, 1, 1));
                    write_batches_to_parquet(arrow_generator.peekable(), &table_path, "Part")?;
                }
                GeneratorType::PartSupp => {
                    info!("...generating PartSupps");
                    let arrow_generator =
                        PartSuppArrow::new(PartSuppGenerator::new(scale_factor, 1, 1));
                    write_batches_to_parquet(arrow_generator.peekable(), &table_path, "PartSupp")?;
                }
                GeneratorType::Region => {
                    info!("...generating Regions");
                    let arrow_generator =
                        RegionArrow::new(RegionGenerator::new(scale_factor, 1, 1));
                    write_batches_to_parquet(arrow_generator.peekable(), &table_path, "Region")?;
                }
                GeneratorType::Supplier => {
                    info!("...generating Suppliers");
                    let arrow_generator =
                        SupplierArrow::new(SupplierGenerator::new(scale_factor, 1, 1));
                    write_batches_to_parquet(arrow_generator.peekable(), &table_path, "Supplier")?;
                }
            };
        }
    }

    Ok(())
}

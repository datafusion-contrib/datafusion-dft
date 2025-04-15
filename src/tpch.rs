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
use log::{debug, info};
use parquet::arrow::ArrowWriter;
use tpchgen::generators::CustomerGenerator;
use tpchgen_arrow::CustomerArrow;

use crate::config::AppConfig;

pub fn generate(config: AppConfig, scale_factor: f64) -> Result<()> {
    let customer_generator = CustomerGenerator::new(scale_factor, 1, 1);
    let customer_arrow_generator = CustomerArrow::new(customer_generator);

    let mut peekable = customer_arrow_generator.peekable();
    let first = peekable
        .peek()
        .ok_or(eyre::Error::msg("Unable to generate Customer TPC-H data"))?;

    info!("Configured `file_cache_dir` is {:?}", config.file_cache_dir);
    if config.file_cache_dir.is_file() {
        eyre::bail!(" Config `file_cache_path` is a file and it must be a dir")
    }
    if !config.file_cache_dir.exists() {
        info!(
            "Configured `file_cache_dir` ({:?}) does not exist, creating",
            config.file_cache_dir
        );
        std::fs::create_dir_all(config.file_cache_dir.clone())?;
    } else {
        debug!(
            "Configured `file_cache_dir` ({:?}) exists",
            config.file_cache_dir
        );
    }
    let tpch_dir = config.file_cache_dir.join("tables").join("tpch");
    if tpch_dir.is_dir() && !tpch_dir.exists() {
        info!(
            "TPC-H table directory ({:?}) does not exist, creating",
            config.file_cache_dir
        );
        std::fs::create_dir_all(&tpch_dir)?;
    } else {
        debug!("TPC-H table directory exists");
    };
    let customer_file_path = tpch_dir.join("customers").join("data.parquet");
    let customer_file = std::fs::File::create(customer_file_path)?;
    let customer_writer =
        ArrowWriter::try_new(customer_file, Arc::clone(first.schema_ref()), None)?;
    while let Some(batch) = peekable.next() {}

    Ok(())
}

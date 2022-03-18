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

use clap::Parser;
use std::path::Path;

use super::print_format::PrintFormat;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
pub struct Args {
    #[clap(
        short = 'p',
        long,
        help = "Path to your data, default to current directory",
        validator(is_valid_data_dir)
    )]
    pub data_path: Option<String>,

    #[clap(
        short = 'c',
        long,
        help = "The batch size of each query, or use DataFusion default",
        validator(is_valid_batch_size)
    )]
    pub batch_size: Option<usize>,

    #[clap(
        short,
        long,
        multiple_values = true,
        help = "Execute commands from file(s), then exit",
        validator(is_valid_file)
    )]
    pub file: Vec<String>,

    #[clap(
        short = 'r',
        long,
        multiple_values = true,
        help = "Run the provided files on startup instead of ~/.datafusionrc",
        validator(is_valid_file),
        conflicts_with = "file"
    )]
    pub rc: Option<Vec<String>>,

    #[clap(long, arg_enum, default_value_t = PrintFormat::Table)]
    pub format: PrintFormat,

    #[clap(long, help = "Ballista scheduler host")]
    pub host: Option<String>,

    #[clap(long, help = "Ballista scheduler port")]
    pub port: Option<u16>,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    pub quiet: bool,
}

fn is_valid_file(dir: &str) -> std::result::Result<(), String> {
    if Path::new(dir).is_file() {
        Ok(())
    } else {
        Err(format!("Invalid file '{}'", dir))
    }
}

fn is_valid_data_dir(dir: &str) -> std::result::Result<(), String> {
    if Path::new(dir).is_dir() {
        Ok(())
    } else {
        Err(format!("Invalid data directory '{}'", dir))
    }
}

fn is_valid_batch_size(size: &str) -> std::result::Result<(), String> {
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(()),
        _ => Err(format!("Invalid batch size '{}'", size)),
    }
}

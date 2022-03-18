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

// Tabs: Context, Catalog, Logs, Sql Editors, Query History, Help for commands / functions
use std::error::Error;

use clap::Parser;
use datafusion_tui::app::App;
use datafusion_tui::cli::args::Args;
use datafusion_tui::run_app;
use log::LevelFilter;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tui_logger::init_logger(LevelFilter::Trace).unwrap();
    tui_logger::set_default_level(LevelFilter::Trace);
    let args = Args::parse();
    let mut app = App::new(args).await;
    let res = run_app(&mut app).await;

    if let Err(err) = res {
        println!("{:?}", err)
    }

    Ok(())
}

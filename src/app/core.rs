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

use std::fs::File;
use std::io::{BufWriter, Write};

use datafusion::prelude::ExecutionConfig;
use log::info;

use crate::app::datafusion::context::{Context, QueryResults};
use crate::app::editor::Editor;
use crate::app::error::Result;
use crate::app::handlers::key_event_handler;
use crate::cli::args::Args;
use crate::events::Key;

const DATAFUSION_RC: &str = ".datafusion/.datafusionrc";

pub struct Tabs {
    pub titles: Vec<&'static str>,
    pub index: usize,
}

impl Tabs {
    fn new() -> Self {
        Tabs {
            titles: vec![
                "SQL Editor [1]",
                "Query History [2]",
                "Context [3]",
                "Logs [4]",
            ],
            index: 0,
        }
    }
}

pub enum InputMode {
    Normal,
    Editing,
    Rc,
}

/// Status that determines whether app should continue or exit
#[derive(PartialEq)]
pub enum AppReturn {
    Continue,
    Exit,
}

/// App holds the state of the application
pub struct App {
    /// Application tabs
    pub tabs: Tabs,
    /// Current input mode
    pub input_mode: InputMode,
    /// SQL Editor and it's state
    pub editor: Editor,
    /// DataFusion `ExecutionContext`
    pub context: Context,
    /// Results from DataFusion query
    pub query_results: Option<QueryResults>,
}

impl App {
    pub async fn new(args: Args) -> App {
        let execution_config = ExecutionConfig::new().with_information_schema(true);
        let mut ctx: Context = match (args.host, args.port) {
            (Some(ref h), Some(p)) => Context::new_remote(h, p).unwrap(),
            _ => Context::new_local(&execution_config).await,
        };

        let files = args.file;

        let rc = App::get_rc_files(args.rc);

        if !files.is_empty() {
            ctx.exec_files(files).await
        } else if !rc.is_empty() {
            info!("Executing '~/.datafusion/.datafusionrc' file");
            ctx.exec_files(rc).await
        }

        App {
            tabs: Tabs::new(),
            input_mode: InputMode::Normal,
            editor: Editor::default(),
            context: ctx,
            query_results: None,
        }
    }

    fn get_rc_files(rc: Option<Vec<String>>) -> Vec<String> {
        match rc {
            Some(file) => file,
            None => {
                let mut files = Vec::new();
                let home = dirs::home_dir();
                if let Some(p) = home {
                    let home_rc = p.join(DATAFUSION_RC);
                    if home_rc.exists() {
                        files.push(home_rc.into_os_string().into_string().unwrap());
                    }
                }
                files
            }
        }
    }

    pub async fn reload_rc(&mut self) {
        let rc = App::get_rc_files(None);
        self.context.exec_files(rc).await;
        info!("Reloaded .datafusionrc");
    }

    pub fn write_rc(&self) -> Result<()> {
        let text = self.editor.input.combine_lines();
        let rc = App::get_rc_files(None);
        let file = File::create(rc[0].clone())?;
        let mut writer = BufWriter::new(file);
        writer.write_all(text.as_bytes())?;
        Ok(())
    }

    pub async fn key_handler(&mut self, key: Key) -> AppReturn {
        key_event_handler(self, key).await.unwrap()
    }

    pub fn update_on_tick(&mut self) -> AppReturn {
        AppReturn::Continue
    }
}

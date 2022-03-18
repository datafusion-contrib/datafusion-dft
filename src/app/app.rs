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

use datafusion::prelude::ExecutionConfig;
use log::debug;

use crate::app::datafusion::context::{Context, QueryResults};
use crate::app::editor::Editor;
use crate::app::handlers::key_event_handler;
use crate::cli::args::Args;
use crate::events::Key;

pub struct Tabs {
    pub titles: Vec<&'static str>,
    pub index: usize,
}

impl Tabs {
    fn new() -> Self {
        Tabs {
            titles: vec![
                "SQL Editor [0]",
                "Query History [1]",
                "Context [2]",
                "Logs [3]",
            ],
            index: 0,
        }
    }
}

pub enum InputMode {
    Normal,
    Editing,
}

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
        let rc = match args.rc {
            Some(file) => file,
            None => {
                let mut files = Vec::new();
                let home = dirs::home_dir();
                if let Some(p) = home {
                    let home_rc = p.join(".datafusionrc");
                    if home_rc.exists() {
                        files.push(home_rc.into_os_string().into_string().unwrap());
                    }
                }
                files
            }
        };

        if !files.is_empty() {
            ctx.exec_files(files).await
        } else {
            if !rc.is_empty() {
                ctx.exec_files(rc).await
            }
        }

        App {
            tabs: Tabs::new(),
            input_mode: InputMode::Normal,
            editor: Editor::default(),
            context: ctx,
            query_results: None,
        }
    }

    pub async fn key_handler(&mut self, key: Key) -> AppReturn {
        debug!("Key event: {:?}", key);
        key_event_handler(self, key).await.unwrap()
    }

    pub fn update_on_tick(&mut self) -> AppReturn {
        AppReturn::Continue
    }
}

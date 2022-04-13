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

#[derive(Debug, Copy, Eq, PartialEq, Clone)]
pub enum TabItem {
    Editor,
    QueryHistory,
    Context,
    Logs,
}

impl Default for TabItem {
    fn default() -> TabItem {
        TabItem::Editor
    }
}

impl TabItem {
    pub(crate) fn all_values() -> Vec<TabItem> {
        vec![
            TabItem::Editor,
            TabItem::QueryHistory,
            TabItem::Context,
            TabItem::Logs,
        ]
    }

    pub(crate) fn list_index(&self) -> usize {
        return TabItem::all_values()
            .iter()
            .position(|x| x == self)
            .unwrap();
    }

    pub(crate) fn title_with_key(&self) -> String {
        return format!("{} [{}]", self.title(), self.list_index() + 1);
    }

    pub(crate) fn title(&self) -> &'static str {
        match self {
            TabItem::Editor => "SQL Editor",
            TabItem::QueryHistory => "Query History",
            TabItem::Context => "Context",
            TabItem::Logs => "Logs",
        }
    }
}

impl TryFrom<char> for TabItem {
    type Error = String;

    fn try_from(value: char) -> std::result::Result<Self, Self::Error> {
        match value {
            '1' => Ok(TabItem::Editor),
            '2' => Ok(TabItem::QueryHistory),
            '3' => Ok(TabItem::Context),
            '4' => Ok(TabItem::Logs),
            i => Err(format!(
                "Invalid tab index: {}, valid range is [1..={}]",
                i, 4
            )),
        }
    }
}

impl From<TabItem> for usize {
    fn from(item: TabItem) -> Self {
        match item {
            TabItem::Editor => 1,
            TabItem::QueryHistory => 2,
            TabItem::Context => 3,
            TabItem::Logs => 4,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum InputMode {
    Normal,
    Editing,
    Rc,
}

impl Default for InputMode {
    fn default() -> InputMode {
        InputMode::Normal
    }
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
    pub tab_item: TabItem,
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
            tab_item: Default::default(),
            input_mode: Default::default(),
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_tab_item_from_char() {
        assert!(TabItem::try_from('0').is_err());
        assert_eq!(TabItem::Editor, TabItem::try_from('1').unwrap());
        assert_eq!(TabItem::QueryHistory, TabItem::try_from('2').unwrap());
        assert_eq!(TabItem::Context, TabItem::try_from('3').unwrap());
        assert_eq!(TabItem::Logs, TabItem::try_from('4').unwrap());
        assert!(TabItem::try_from('5').is_err());
    }

    #[test]
    fn test_tab_item_to_usize() {
        (0_usize..TabItem::all_values().len()).for_each(|i| {
            assert_eq!(
                TabItem::all_values()[i],
                TabItem::try_from(format!("{}", i + 1).chars().next().unwrap()).unwrap()
            );
            assert_eq!(TabItem::all_values()[i].list_index(), i);
        });
    }
}

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

use datafusion::prelude::SessionConfig;
use log::info;
use ratatui::buffer::Buffer;
use ratatui::crossterm::event::KeyEvent;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;
use tui_logger::TuiWidgetState;

use crate::app::datafusion::context::{Context, QueryResults};
use crate::app::editor::Editor;
use crate::app::error::Result;
use crate::app::handlers::key_event_handler;
use crate::cli::args::Args;
use crate::events::{Event, Key};

use super::ui::{draw_context_tab, draw_logs_tab, draw_query_history_tab, draw_sql_editor_tab};

const DATAFUSION_RC: &str = ".datafusion/.datafusionrc";

pub struct Tabs {
    pub titles: Vec<&'static str>,
    pub index: usize,
}

#[derive(Default, Debug, Copy, Eq, PartialEq, Clone)]
pub enum TabItem {
    #[default]
    Editor,
    QueryHistory,
    Context,
    Logs,
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
        format!("{} [{}]", self.title(), self.list_index() + 1)
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

#[derive(Default, Debug, Copy, Clone)]
pub enum InputMode {
    #[default]
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

pub struct Logs {
    pub state: TuiWidgetState,
}

/// App holds the state of the application
pub struct App<'app> {
    /// Application tabs
    pub tab_item: TabItem,
    /// Current input mode
    pub input_mode: InputMode,
    /// SQL Editor and it's state
    pub editor: Editor<'app>,
    /// DataFusion `ExecutionContext`
    pub context: Context,
    /// Results from DataFusion query
    pub query_results: Option<QueryResults>,
    /// Application logs
    pub logs: Logs,
}

impl<'app> App<'app> {
    pub fn new(args: Args, editor: Editor<'app>) -> Self {
        let execution_config = SessionConfig::new().with_information_schema(true);
        let ctx = Context::new_local(&execution_config);
        // let mut ctx: Context = match (args.host, args.port) {
        //     (Some(ref h), Some(p)) => Context::new_remote(h, p).await.unwrap(),
        //     _ => Context::new_local(&execution_config).await,
        // };

        let files = args.file;

        let rc = App::get_rc_files(args.rc);

        // if !files.is_empty() {
        //     ctx.exec_files(files).await
        // } else if !rc.is_empty() {
        //     info!("Executing '~/.datafusion/.datafusionrc' file");
        //     ctx.exec_files(rc).await
        // }

        let log_state = TuiWidgetState::default();

        let logs = Logs { state: log_state };

        App {
            tab_item: Default::default(),
            input_mode: Default::default(),
            editor,
            // editor: Editor::default(),
            context: ctx,
            query_results: None,
            logs,
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

    pub async fn event_handler(&'app mut self, event: Event) -> Result<AppReturn> {
        match event {
            Event::KeyInput(k) => key_event_handler(self, k).await,
            _ => Ok(AppReturn::Continue),
        }
    }

    pub async fn reload_rc(&mut self) {
        let rc = App::get_rc_files(None);
        self.context.exec_files(rc).await;
        info!("Reloaded .datafusionrc");
    }

    // pub fn write_rc(&self) -> Result<()> {
    //     let text = self.editor.input.combine_lines();
    //     let rc = App::get_rc_files(None);
    //     let file = File::create(rc[0].clone())?;
    //     let mut writer = BufWriter::new(file);
    //     writer.write_all(text.as_bytes())?;
    //     Ok(())
    // }

    pub async fn key_handler(&'app mut self, key: KeyEvent) -> AppReturn {
        key_event_handler(self, key).await.unwrap()
    }

    pub fn update_on_tick(&self) -> AppReturn {
        AppReturn::Continue
    }
}

impl Widget for &App<'_> {
    /// Note: Ratatui uses Immediate Mode rendering (i.e. the entire UI is redrawn)
    /// on every frame based on application state. There is no permanent widget object
    /// in memory.
    fn render(self, area: Rect, buf: &mut Buffer) {
        match self.tab_item {
            TabItem::Editor => draw_sql_editor_tab(self, area, buf),
            TabItem::QueryHistory => draw_query_history_tab(self, area, buf),
            TabItem::Context => draw_context_tab(self, area, buf),
            TabItem::Logs => draw_logs_tab(self, area, buf),
        }
        // let vertical = Layout::vertical([
        //     Constraint::Length(1),
        //     Constraint::Min(0),
        //     Constraint::Length(1),
        // ]);
        // let [header_area, inner_area, footer_area] = vertical.areas(area);
        //
        // let horizontal = Layout::horizontal([Constraint::Min(0)]);
        // let [tabs_area] = horizontal.areas(header_area);
        // self.render_tabs(tabs_area, buf);
        // self.state.tabs.selected.render(inner_area, buf, self);
        // self.render_footer(footer_area, buf);
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

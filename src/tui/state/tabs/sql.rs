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

use core::cell::RefCell;

use color_eyre::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::sql::sqlparser::keywords;
use ratatui::crossterm::event::KeyEvent;
use ratatui::style::palette::tailwind;
use ratatui::style::{Modifier, Style};
use ratatui::widgets::TableState;
use tokio::task::JoinHandle;
use tui_textarea::TextArea;

use crate::tui::ExecutionError;
use crate::config::AppConfig;

pub fn get_keywords() -> Vec<String> {
    keywords::ALL_KEYWORDS
        .iter()
        .map(|k| k.to_string())
        .collect()
}

pub fn keyword_regex() -> String {
    format!(
        "(?i)(^|[^a-zA-Z0-9\'\"`._]*?)({})($|[^a-zA-Z0-9\'\"`._]*)",
        get_keywords().join("|")
    )
}

pub fn keyword_style() -> Style {
    Style::default()
        .bg(tailwind::BLACK)
        .fg(tailwind::YELLOW.c100)
        .add_modifier(Modifier::BOLD)
}

#[derive(Debug, Default, PartialEq)]
pub enum SQLTabMode {
    #[default]
    Normal,
    DDL,
}

#[derive(Debug, Default)]
pub struct SQLTabState<'app> {
    editor: TextArea<'app>,
    editor_editable: bool,
    ddl_editor: TextArea<'app>,
    ddl_editor_editable: bool,
    query_results_state: Option<RefCell<TableState>>,
    result_batches: Option<Vec<RecordBatch>>,
    current_page: Option<usize>,
    execution_error: Option<ExecutionError>,
    execution_task: Option<JoinHandle<Result<()>>>,
    mode: SQLTabMode,
}

impl<'app> SQLTabState<'app> {
    pub fn new(config: &AppConfig) -> Self {
        let empty_text = vec!["Enter a query here.".to_string()];
        // TODO: Enable vim mode from config?
        let mut textarea = TextArea::new(empty_text);
        textarea.set_style(Style::default().fg(tailwind::WHITE));

        let ddl_empty_text = vec!["Write your DDL here.".to_string()];
        let mut ddl_textarea = TextArea::new(ddl_empty_text);
        ddl_textarea.set_style(Style::default().fg(tailwind::WHITE));
        if config.editor.experimental_syntax_highlighting {
            textarea.set_search_pattern(keyword_regex()).unwrap();
            textarea.set_search_style(keyword_style());
            ddl_textarea.set_search_pattern(keyword_regex()).unwrap();
            ddl_textarea.set_search_style(keyword_style());
        };
        Self {
            editor: textarea,
            editor_editable: false,
            ddl_editor: ddl_textarea,
            ddl_editor_editable: false,
            query_results_state: None,
            result_batches: None,
            current_page: None,
            execution_error: None,
            execution_task: None,
            mode: SQLTabMode::default(),
        }
    }

    pub fn query_results_state(&self) -> &Option<RefCell<TableState>> {
        &self.query_results_state
    }

    pub fn refresh_query_results_state(&mut self) {
        self.query_results_state = Some(RefCell::new(TableState::default()));
    }

    pub fn reset_execution_results(&mut self) {
        self.result_batches = None;
        self.current_page = None;
        self.execution_error = None;
        self.refresh_query_results_state();
    }

    pub fn editor(&self) -> TextArea {
        // TODO: Figure out how to do this without clone. Probably need logic in handler to make
        // updates to the Widget and then pass a ref
        self.editor.clone()
    }

    pub fn ddl_editor(&self) -> TextArea {
        self.ddl_editor.clone()
    }

    pub fn active_editor_cloned(&self) -> TextArea {
        match self.mode {
            SQLTabMode::Normal => self.editor.clone(),
            SQLTabMode::DDL => self.ddl_editor.clone(),
        }
    }

    pub fn clear_placeholder(&mut self) {
        let default = "Enter a query here.";
        let lines = self.editor.lines();
        let content = lines.join("");
        if content == default {
            self.editor
                .move_cursor(tui_textarea::CursorMove::Jump(0, 0));
            self.editor.delete_str(default.len());
        }
    }

    pub fn clear_editor(&mut self, config: &AppConfig) {
        let mut textarea = TextArea::new(vec!["".to_string()]);
        textarea.set_style(Style::default().fg(tailwind::WHITE));
        if config.editor.experimental_syntax_highlighting {
            textarea.set_search_pattern(keyword_regex()).unwrap();
            textarea.set_search_style(keyword_style());
        };
        self.editor = textarea;
    }

    pub fn update_editor_content(&mut self, key: KeyEvent) {
        match self.mode {
            SQLTabMode::Normal => self.editor.input(key),
            SQLTabMode::DDL => self.ddl_editor.input(key),
        };
    }

    pub fn add_ddl_to_editor(&mut self, ddl: String) {
        self.ddl_editor.delete_line_by_end();
        self.ddl_editor.set_yank_text(ddl);
        self.ddl_editor.paste();
    }

    pub fn edit(&mut self) {
        match self.mode {
            SQLTabMode::Normal => self.editor_editable = true,
            SQLTabMode::DDL => self.ddl_editor_editable = true,
        };
    }

    pub fn exit_edit(&mut self) {
        match self.mode {
            SQLTabMode::Normal => self.editor_editable = false,
            SQLTabMode::DDL => self.ddl_editor_editable = false,
        };
    }

    pub fn editor_editable(&self) -> bool {
        match self.mode {
            SQLTabMode::Normal => self.editor_editable,
            SQLTabMode::DDL => self.ddl_editor_editable,
        }
    }

    pub fn editable(&self) -> bool {
        self.editor_editable || self.ddl_editor_editable
    }

    // TODO: Create Editor struct and move this there
    pub fn next_word(&mut self) {
        match self.mode {
            SQLTabMode::Normal => self
                .editor
                .move_cursor(tui_textarea::CursorMove::WordForward),
            SQLTabMode::DDL => self
                .ddl_editor
                .move_cursor(tui_textarea::CursorMove::WordForward),
        }
    }

    // TODO: Create Editor struct and move this there
    pub fn previous_word(&mut self) {
        match self.mode {
            SQLTabMode::Normal => self.editor.move_cursor(tui_textarea::CursorMove::WordBack),
            SQLTabMode::DDL => self
                .ddl_editor
                .move_cursor(tui_textarea::CursorMove::WordBack),
        }
    }

    pub fn delete_word(&mut self) {
        match self.mode {
            SQLTabMode::Normal => self.editor.delete_word(),
            SQLTabMode::DDL => self.ddl_editor.delete_word(),
        };
    }

    pub fn add_batch(&mut self, batch: RecordBatch) {
        if let Some(batches) = self.result_batches.as_mut() {
            batches.push(batch);
        } else {
            self.result_batches = Some(vec![batch]);
        }
    }

    pub fn current_batch(&self) -> Option<&RecordBatch> {
        match (self.current_page, self.result_batches.as_ref()) {
            (Some(page), Some(batches)) => batches.get(page),
            _ => None,
        }
    }

    pub fn batches_count(&self) -> usize {
        if let Some(batches) = &self.result_batches {
            batches.len()
        } else {
            0
        }
    }

    pub fn execution_error(&self) -> &Option<ExecutionError> {
        &self.execution_error
    }

    pub fn set_execution_error(&mut self, error: ExecutionError) {
        self.execution_error = Some(error);
    }

    pub fn current_page(&self) -> Option<usize> {
        self.current_page
    }

    pub fn next_page(&mut self) {
        if let Some(page) = self.current_page {
            self.current_page = Some(page + 1);
        } else {
            self.current_page = Some(0);
        }
    }

    pub fn previous_page(&mut self) {
        if let Some(page) = self.current_page {
            if page > 0 {
                self.current_page = Some(page - 1);
            }
        }
    }

    pub fn execution_task(&mut self) -> &mut Option<JoinHandle<Result<()>>> {
        &mut self.execution_task
    }

    pub fn set_execution_task(&mut self, task: JoinHandle<Result<()>>) {
        self.execution_task = Some(task);
    }

    pub fn mode(&self) -> &SQLTabMode {
        &self.mode
    }

    pub fn set_mode(&mut self, mode: SQLTabMode) {
        self.mode = mode
    }

    pub fn sql(&self) -> String {
        match self.mode {
            SQLTabMode::Normal => self.editor.lines().join("\n"),
            SQLTabMode::DDL => self.ddl_editor.lines().join("\n"),
        }
    }
}

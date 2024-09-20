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
use std::time::Duration;

use datafusion::arrow::array::RecordBatch;
use ratatui::crossterm::event::KeyEvent;
use ratatui::style::palette::tailwind;
use ratatui::style::Style;
use ratatui::widgets::TableState;
use tui_textarea::TextArea;

use crate::app::state::tabs::sql;
use crate::execution::ExecutionStats;

#[derive(Clone, Debug)]
pub struct FlightSQLQuery {
    sql: String,
    results: Option<Vec<RecordBatch>>,
    num_rows: Option<usize>,
    error: Option<String>,
    execution_time: Duration,
    execution_stats: Option<ExecutionStats>,
}

impl FlightSQLQuery {
    pub fn new(
        sql: String,
        results: Option<Vec<RecordBatch>>,
        num_rows: Option<usize>,
        error: Option<String>,
        execution_time: Duration,
        execution_stats: Option<ExecutionStats>,
    ) -> Self {
        Self {
            sql,
            results,
            num_rows,
            error,
            execution_time,
            execution_stats,
        }
    }

    pub fn sql(&self) -> &String {
        &self.sql
    }

    pub fn set_results(&mut self, results: Option<Vec<RecordBatch>>) {
        self.results = results;
    }

    pub fn results(&self) -> &Option<Vec<RecordBatch>> {
        &self.results
    }

    pub fn set_num_rows(&mut self, num_rows: Option<usize>) {
        self.num_rows = num_rows;
    }

    pub fn num_rows(&self) -> &Option<usize> {
        &self.num_rows
    }

    pub fn set_error(&mut self, error: Option<String>) {
        self.error = error;
    }

    pub fn error(&self) -> &Option<String> {
        &self.error
    }

    pub fn set_execution_time(&mut self, execution_time: Duration) {
        self.execution_time = execution_time;
    }

    pub fn execution_time(&self) -> &Duration {
        &self.execution_time
    }

    pub fn execution_stats(&self) -> &Option<ExecutionStats> {
        &self.execution_stats
    }

    pub fn set_execution_stats(&mut self, stats: Option<ExecutionStats>) {
        self.execution_stats = stats;
    }
}

#[derive(Debug, Default)]
pub struct FlightSQLTabState<'app> {
    editor: TextArea<'app>,
    editor_editable: bool,
    query: Option<FlightSQLQuery>,
    query_results_state: Option<RefCell<TableState>>,
}

impl<'app> FlightSQLTabState<'app> {
    pub fn new() -> Self {
        let empty_text = vec!["Enter a query here.".to_string()];
        // TODO: Enable vim mode from config?
        let mut textarea = TextArea::new(empty_text);
        textarea.set_style(Style::default().fg(tailwind::WHITE));
        textarea.set_search_pattern(sql::keyword_regex()).unwrap();
        textarea.set_search_style(sql::keyword_style());
        Self {
            editor: textarea,
            editor_editable: false,
            query: None,
            query_results_state: None,
        }
    }

    pub fn query_results_state(&self) -> &Option<RefCell<TableState>> {
        &self.query_results_state
    }

    pub fn refresh_query_results_state(&mut self) {
        self.query_results_state = Some(RefCell::new(TableState::default()));
    }

    pub fn editor(&self) -> TextArea {
        // TODO: Figure out how to do this without clone. Probably need logic in handler to make
        // updates to the Widget and then pass a ref
        self.editor.clone()
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

    pub fn clear_editor(&mut self) {
        let mut textarea = TextArea::new(vec!["".to_string()]);
        textarea.set_style(Style::default().fg(tailwind::WHITE));
        textarea.set_search_pattern(sql::keyword_regex()).unwrap();
        textarea.set_search_style(sql::keyword_style());
        self.editor = textarea;
    }

    pub fn update_editor_content(&mut self, key: KeyEvent) {
        self.editor.input(key);
    }

    pub fn edit(&mut self) {
        self.editor_editable = true;
    }

    pub fn exit_edit(&mut self) {
        self.editor_editable = false;
    }

    pub fn editor_editable(&self) -> bool {
        self.editor_editable
    }

    pub fn set_query(&mut self, query: FlightSQLQuery) {
        self.query = Some(query);
    }

    pub fn query(&self) -> &Option<FlightSQLQuery> {
        &self.query
    }

    // TODO: Create Editor struct and move this there
    pub fn next_word(&mut self) {
        self.editor
            .move_cursor(tui_textarea::CursorMove::WordForward)
    }

    // TODO: Create Editor struct and move this there
    pub fn previous_word(&mut self) {
        self.editor.move_cursor(tui_textarea::CursorMove::WordBack)
    }

    pub fn delete_word(&mut self) {
        self.editor.delete_word();
    }
}

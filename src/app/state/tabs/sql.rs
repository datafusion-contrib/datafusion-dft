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
use log::info;
use ratatui::crossterm::event::KeyEvent;
use ratatui::style::palette::tailwind;
use ratatui::style::Style;
use ratatui::widgets::TableState;
use tui_textarea::TextArea;

use crate::app::app_execution::ExecutionStats;
use crate::app::ExecutionError;

#[derive(Clone, Debug)]
pub struct Query {
    sql: String,
    results: Option<Vec<RecordBatch>>,
    num_rows: Option<usize>,
    error: Option<String>,
    execution_time: Duration,
    execution_stats: Option<ExecutionStats>,
}

impl Query {
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

    pub fn execution_time(&self) -> &Duration {
        &self.execution_time
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

    pub fn set_execution_time(&mut self, elapsed_time: Duration) {
        self.execution_time = elapsed_time;
    }

    pub fn execution_stats(&self) -> &Option<ExecutionStats> {
        &self.execution_stats
    }

    pub fn set_execution_stats(&mut self, stats: Option<ExecutionStats>) {
        self.execution_stats = stats;
    }
}

#[derive(Debug, Default)]
pub struct SQLTabState<'app> {
    editor: TextArea<'app>,
    editor_editable: bool,
    query: Option<Query>,
    query_results_state: Option<RefCell<TableState>>,
    result_batches: Option<Vec<RecordBatch>>,
    results_page: Option<usize>,
    execution_error: Option<ExecutionError>,
}

impl<'app> SQLTabState<'app> {
    pub fn new() -> Self {
        let empty_text = vec!["Enter a query here.".to_string()];
        // TODO: Enable vim mode from config?
        let mut textarea = TextArea::new(empty_text);
        textarea.set_style(Style::default().fg(tailwind::WHITE));
        Self {
            editor: textarea,
            editor_editable: false,
            query: None,
            query_results_state: None,
            result_batches: None,
            results_page: None,
            execution_error: None,
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
        self.results_page = None;
        self.execution_error = None;
        self.refresh_query_results_state();
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

    pub fn set_query(&mut self, query: Query) {
        self.query = Some(query);
    }

    pub fn query(&self) -> &Option<Query> {
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

    pub fn add_batch(&mut self, batch: RecordBatch) {
        if let Some(batches) = self.result_batches.as_mut() {
            batches.push(batch);
        } else {
            self.result_batches = Some(vec![batch]);
        }
    }

    pub fn current_batch(&self) -> Option<&RecordBatch> {
        match (self.results_page, self.result_batches.as_ref()) {
            (Some(page), Some(batches)) => batches.get(page),
            _ => None,
        }
    }

    pub fn execution_error(&self) -> &Option<ExecutionError> {
        &self.execution_error
    }

    pub fn set_execution_error(&mut self, error: ExecutionError) {
        self.execution_error = Some(error);
    }

    pub fn results_page(&self) -> Option<usize> {
        self.results_page
    }

    pub fn next_page(&mut self) {
        if let Some(page) = self.results_page {
            self.results_page = Some(page + 1);
        } else {
            self.results_page = Some(0);
        }
    }
}

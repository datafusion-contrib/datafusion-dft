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

use datafusion::arrow::array::RecordBatch;
use ratatui::crossterm::event::KeyEvent;
use ratatui::style::{palette::tailwind, Style};
use ratatui::widgets::TableState;
use tui_textarea::TextArea;

#[derive(Debug)]
pub struct ExploreTabState<'app> {
    editor: TextArea<'app>,
    editor_editable: bool,
    query_results: Option<Vec<RecordBatch>>,
    query_results_state: Option<TableState>,
    query_error: Option<String>,
}

impl<'app> ExploreTabState<'app> {
    pub fn new() -> Self {
        let empty_text = vec!["Enter a query here.".to_string()];
        // TODO: Enable vim mode from config?
        let mut textarea = TextArea::new(empty_text);
        textarea.set_line_number_style(Style::default().bg(tailwind::GRAY.c400));
        Self {
            editor: textarea,
            editor_editable: false,
            query_results: None,
            query_results_state: None,
            query_error: None,
        }
    }

    pub fn query_results_state_clone(&self) -> Option<TableState> {
        self.query_results_state.clone()
    }

    pub fn query_results_state_mut(&mut self) -> &mut Option<TableState> {
        &mut self.query_results_state
    }

    pub fn refresh_query_results_state(&mut self) {
        self.query_results_state = Some(TableState::default());
    }

    // pub fn query_results_state_mut(&mut self) -> &mut Option<TableState> {
    //     &mut self.query_results_state
    // }

    pub fn query_error(&self) -> &Option<String> {
        &self.query_error
    }

    pub fn set_query_error(&mut self, error: String) {
        self.query_error = Some(error);
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
        self.editor = TextArea::new(vec!["".to_string()]);
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

    pub fn is_editable(&self) -> bool {
        self.editor_editable
    }

    pub fn set_query_results(&mut self, query_results: Vec<RecordBatch>) {
        self.query_results = Some(query_results);
    }

    pub fn query_results(&self) -> &Option<Vec<RecordBatch>> {
        &self.query_results
    }
}

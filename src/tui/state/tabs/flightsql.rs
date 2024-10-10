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
use std::sync::Arc;

use color_eyre::Result;
use datafusion::arrow::{
    array::{Array, RecordBatch, UInt32Array},
    compute::take_record_batch,
    datatypes::Schema,
    error::ArrowError,
};
use log::{error, info};
use ratatui::crossterm::event::KeyEvent;
use ratatui::style::palette::tailwind;
use ratatui::style::Style;
use ratatui::widgets::TableState;
use tokio::task::JoinHandle;
use tui_textarea::TextArea;

use crate::tui::state::tabs::sql;
use crate::tui::ExecutionError;
use crate::config::AppConfig;

const PAGE_SIZE: usize = 100;

#[derive(Debug, Default)]
pub enum FlightSQLConnectionStatus {
    #[default]
    EstablishingConnection,
    Connected,
    FailedToConnect,
    Disconnected,
}

impl FlightSQLConnectionStatus {
    pub fn tab_display(&self) -> String {
        match self {
            FlightSQLConnectionStatus::EstablishingConnection => " [Connecting...]".to_string(),
            FlightSQLConnectionStatus::Connected => "".to_string(),
            FlightSQLConnectionStatus::FailedToConnect => " [Failed to connect]".to_string(),
            FlightSQLConnectionStatus::Disconnected => " [Disconnected]".to_string(),
        }
    }
}

#[derive(Debug, Default)]
pub struct FlightSQLTabState<'app> {
    editor: TextArea<'app>,
    editor_editable: bool,
    query_results_state: Option<RefCell<TableState>>,
    result_batches: Option<Vec<RecordBatch>>,
    current_page: Option<usize>,
    execute_in_progress: bool,
    execution_error: Option<ExecutionError>,
    execution_task: Option<JoinHandle<Result<()>>>,
    connection_status: FlightSQLConnectionStatus,
}

impl<'app> FlightSQLTabState<'app> {
    pub fn new(config: &AppConfig) -> Self {
        let empty_text = vec!["Enter a query here.".to_string()];
        // TODO: Enable vim mode from config?
        let mut textarea = TextArea::new(empty_text);
        textarea.set_style(Style::default().fg(tailwind::WHITE));
        if config.editor.experimental_syntax_highlighting {
            textarea.set_search_pattern(sql::keyword_regex()).unwrap();
            textarea.set_search_style(sql::keyword_style());
        };
        Self {
            editor: textarea,
            editor_editable: false,
            query_results_state: None,
            result_batches: None,
            execution_task: None,
            current_page: None,
            execute_in_progress: false,
            execution_error: None,
            connection_status: FlightSQLConnectionStatus::default(),
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

    pub fn connection_status(&self) -> &FlightSQLConnectionStatus {
        &self.connection_status
    }

    pub fn set_connection_status(&mut self, status: FlightSQLConnectionStatus) {
        self.connection_status = status;
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
            textarea.set_search_pattern(sql::keyword_regex()).unwrap();
            textarea.set_search_style(sql::keyword_style());
        };
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

    pub fn execution_task(&mut self) -> &mut Option<JoinHandle<Result<()>>> {
        &mut self.execution_task
    }

    pub fn set_execution_task(&mut self, task: JoinHandle<Result<()>>) {
        self.execution_task = Some(task);
    }

    pub fn add_batch(&mut self, batch: RecordBatch) {
        if let Some(batches) = self.result_batches.as_mut() {
            batches.push(batch);
        } else {
            self.result_batches = Some(vec![batch]);
        }
    }

    pub fn reset_execution_results(&mut self) {
        info!("Resetting execution results");
        self.result_batches = None;
        self.current_page = None;
        self.execution_error = None;
        self.execute_in_progress = true;
        self.refresh_query_results_state();
    }

    pub fn in_progress(&self) -> bool {
        self.execute_in_progress
    }

    pub fn set_in_progress(&mut self, in_progress: bool) {
        self.execute_in_progress = in_progress;
    }

    pub fn current_page(&self) -> Option<usize> {
        self.current_page
    }

    pub fn execution_error(&self) -> &Option<ExecutionError> {
        &self.execution_error
    }

    pub fn set_execution_error(&mut self, error: ExecutionError) {
        self.execution_error = Some(error);
    }

    pub fn current_page_results(&self) -> Option<RecordBatch> {
        match (self.current_page, self.result_batches.as_ref()) {
            (Some(page), Some(batches)) => {
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                let indices = if total_rows < PAGE_SIZE {
                    UInt32Array::from_iter_values(0_u32..(total_rows as u32))
                } else {
                    let start = page * PAGE_SIZE;
                    let remaining = total_rows - start;
                    // On the last page there could be less than PAGE_SIZE results to view
                    let page_records = remaining.min(PAGE_SIZE);
                    let end = (start as u32) + (page_records as u32);
                    info!("Current page start({start}) end({end})");
                    UInt32Array::from_iter_values((start as u32)..end)
                };
                match take_record_batches(batches, &indices) {
                    Ok(batch) => Some(batch),
                    Err(err) => {
                        error!("Error getting record batch: {}", err);
                        None
                    }
                }
            }
            _ => Some(RecordBatch::new_empty(Arc::new(Schema::empty()))),
        }
    }

    pub fn next_page(&mut self) {
        self.change_page(
            |page, max_pages| {
                if page < max_pages {
                    page + 1
                } else {
                    page
                }
            },
        );
    }

    pub fn previous_page(&mut self) {
        self.change_page(|page, _| if page > 0 { page - 1 } else { 0 });
    }

    fn change_page<F>(&mut self, change_fn: F)
    where
        F: Fn(usize, usize) -> usize,
    {
        match (self.current_page.as_mut(), self.result_batches.as_ref()) {
            (Some(page), Some(batches)) => {
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                let max_pages = total_rows / PAGE_SIZE;
                *page = change_fn(*page, max_pages);
            }
            (None, Some(_)) => self.current_page = Some(0),
            _ => {
                error!("Got change page request with no batches")
            }
        }
    }

    pub fn sql(&self) -> String {
        self.editor.lines().join("\n")
    }
}

fn take_record_batches(
    batches: &[RecordBatch],
    indices: &dyn Array,
) -> Result<RecordBatch, ArrowError> {
    match batches.len() {
        0 => Ok(RecordBatch::new_empty(Arc::new(Schema::empty()))),
        1 => take_record_batch(&batches[0], indices),
        // For now we just get the first batch
        _ => take_record_batch(&batches[0], indices),
    }
}

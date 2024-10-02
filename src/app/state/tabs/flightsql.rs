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
use std::time::Duration;

use color_eyre::Result;
use datafusion::arrow::{
    array::{RecordBatch, UInt32Array},
    compute::{concat_batches, take_record_batch},
    datatypes::{Field, Schema},
};
use ratatui::crossterm::event::KeyEvent;
use ratatui::style::palette::tailwind;
use ratatui::style::Style;
use ratatui::widgets::TableState;
use tokio::task::JoinHandle;
use tui_textarea::TextArea;

use crate::app::state::tabs::sql;
use crate::app::ExecutionError;
use crate::config::AppConfig;
use crate::execution::ExecutionStats;

// #[derive(Clone, Debug)]
// pub struct FlightSQLQuery {
//     sql: String,
//     results: Option<Vec<RecordBatch>>,
//     num_rows: Option<usize>,
//     error: Option<String>,
//     execution_time: Duration,
//     execution_stats: Option<ExecutionStats>,
// }
//
// impl FlightSQLQuery {
//     pub fn new(
//         sql: String,
//         results: Option<Vec<RecordBatch>>,
//         num_rows: Option<usize>,
//         error: Option<String>,
//         execution_time: Duration,
//         execution_stats: Option<ExecutionStats>,
//     ) -> Self {
//         Self {
//             sql,
//             results,
//             num_rows,
//             error,
//             execution_time,
//             execution_stats,
//         }
//     }
//
//     pub fn sql(&self) -> &String {
//         &self.sql
//     }
//
//     pub fn set_results(&mut self, results: Option<Vec<RecordBatch>>) {
//         self.results = results;
//     }
//
//     pub fn results(&self) -> &Option<Vec<RecordBatch>> {
//         &self.results
//     }
//
//     pub fn set_num_rows(&mut self, num_rows: Option<usize>) {
//         self.num_rows = num_rows;
//     }
//
//     pub fn num_rows(&self) -> &Option<usize> {
//         &self.num_rows
//     }
//
//     pub fn set_error(&mut self, error: Option<String>) {
//         self.error = error;
//     }
//
//     pub fn error(&self) -> &Option<String> {
//         &self.error
//     }
//
//     pub fn set_execution_time(&mut self, execution_time: Duration) {
//         self.execution_time = execution_time;
//     }
//
//     pub fn execution_time(&self) -> &Duration {
//         &self.execution_time
//     }
//
//     pub fn execution_stats(&self) -> &Option<ExecutionStats> {
//         &self.execution_stats
//     }
//
//     pub fn set_execution_stats(&mut self, stats: Option<ExecutionStats>) {
//         self.execution_stats = stats;
//     }
// }

#[derive(Debug, Default)]
pub struct FlightSQLTabState<'app> {
    editor: TextArea<'app>,
    editor_editable: bool,
    query_results_state: Option<RefCell<TableState>>,
    result_batches: Option<Vec<RecordBatch>>,
    current_page: Option<usize>,
    current_offset: Option<usize>,
    execution_error: Option<ExecutionError>,
    execution_task: Option<JoinHandle<Result<()>>>,
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
            current_offset: None,
            execution_error: None,
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

    // pub fn set_query(&mut self, query: FlightSQLQuery) {
    //     self.query = Some(query);
    // }

    // pub fn query(&self) -> &Option<FlightSQLQuery> {
    //     &self.query
    // }

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
        self.result_batches = None;
        self.current_page = None;
        self.execution_error = None;
        self.refresh_query_results_state();
    }

    pub fn current_page(&self) -> Option<usize> {
        self.current_page
    }

    pub fn current_page_results(&self) -> Option<RecordBatch> {
        match (self.current_page, self.result_batches.as_ref()) {
            (Some(page), Some(batches)) => get_current_page_result_batch(page, batches),
            _ => Some(RecordBatch::new_empty(Arc::new(Schema::empty()))),
        }
    }

    pub fn execution_error(&self) -> Option<ExecutionError> {
        None
    }

    pub fn next_page(&mut self) {}

    pub fn previous_page(&mut self) {}
}

/// The purpose of this function is to return the start and end batches, as well as the row indices
/// from each to get `num` records starting from `offset` from a slice of batches that have size
/// `batch_sizes`.  The result of this is expected to be passed to
/// `arrow_select::take::take_record_batch` to get the records from each batch and then
/// `arrow_select::concat::concat_batches` to combine them into a single record batch.  This is
/// useful for paginating a slice of record batches.
///
/// The return tuple has the structure:
///
/// ((start_batch, start_index, end_index), (end_batch, start_index, end_index))
///
/// Some examples
/// compute_batch_idxs_and_rows_for_offset(0, 100, &[200]) -> ((0, 0), (0, 100))
/// compute_batch_idxs_and_rows_for_offset(0, 200, &[200]) -> ((0, 0), (0, 200))
/// compute_batch_idxs_and_rows_for_offset(0, 250, &[200, 100]) -> ((0, 0), (0, 200))
fn compute_batch_idxs_and_rows_for_offset(
    offset: usize,
    num: usize,
    batch_sizes: &[usize],
) -> ((usize, usize, usize), (usize, usize, usize)) {
    // let start = 0;
    // for (i, rows) in batch_sizes.iter().enumerate() {
    //     if offset < *rows {
    //         return (start, None);
    //     }
    // }
}

fn get_current_page_result_batch(
    offset: usize,
    num: usize,
    batches: &[RecordBatch],
) -> Option<RecordBatch> {
    let batch_sizes: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();
    match batch_sizes.len() {
        0 => None,
        1 => Some(batches[0].clone()),
        _ => {
            let (
                (start_batch, start_batch_start_idx, start_batch_end_idx),
                (end_batch, end_batch_start_idx, end_batch_end_idx),
            ) = compute_batch_idxs_and_rows_for_offset(offset, num, &batch_sizes);
            let start_indices = UInt32Array::from_iter_values(
                (start_batch_start_idx as u32)..(start_batch_end_idx as u32),
            );
            let end_indices = UInt32Array::from_iter_values(
                (end_batch_start_idx as u32)..(end_batch_end_idx as u32),
            );
            let start_records = take_record_batch(&batches[start_batch], &start_indices).unwrap();
            let end_records = take_record_batch(&batches[end_batch], &end_indices).unwrap();
            let res =
                concat_batches(&start_records.schema(), &[start_records, end_records]).unwrap();
            Some(res)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::app::state::tabs::flightsql::compute_batch_idxs_and_rows_for_offset;

    #[test]
    fn test_compute_batch_idxs_for_offset_no_batches() {
        // let batch_sizes = vec![];

        // let (start, end) = compute_batch_idxs_for_offset(0, &batch_sizes);
        // assert_eq!(start, 4);
        // assert_eq!(end, None);
    }
}

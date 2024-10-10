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

//! Tests for the TUI (e.g. user application with keyboard commands)

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use datafusion::assert_batches_eq;
use dft::tui::{AppEvent, ExecutionResultsBatch};

use crate::tui_cases::TestApp;

fn create_batch(adj: u32) -> RecordBatch {
    let arr1: ArrayRef = Arc::new(UInt32Array::from(vec![1 + adj, 2 + adj]));
    let arr2: ArrayRef = Arc::new(UInt32Array::from(vec![3 + adj, 4 + adj]));

    RecordBatch::try_from_iter(vec![("a", arr1), ("b", arr2)]).unwrap()
}

fn create_execution_results(query: &str, adj: u32) -> ExecutionResultsBatch {
    let duration = Duration::from_secs(1);
    let batch = create_batch(adj);
    ExecutionResultsBatch::new(query.to_string(), batch, duration)
}

// Tests that a single page of results is displayed correctly
#[tokio::test]
async fn single_page() {
    let mut test_app = TestApp::new();
    let res1 = create_execution_results("SELECT 1", 0);
    let event1 = AppEvent::ExecutionResultsNextBatch(res1);

    test_app.handle_app_event(AppEvent::NewExecution).unwrap();
    test_app.handle_app_event(event1).unwrap();

    let state = test_app.state();

    let page = state.sql_tab.current_page().unwrap();
    assert_eq!(page, 0);

    let batch = state.sql_tab.current_batch();
    assert!(batch.is_some());

    let batch = batch.unwrap();
    let batches = vec![batch.clone()];
    let expected = [
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | 3 |",
        "| 2 | 4 |",
        "+---+---+",
    ];
    assert_batches_eq!(expected, &batches);
    let table_state = state.sql_tab.query_results_state();
    assert!(table_state.is_some());
    let table_state = table_state.as_ref().unwrap();
    assert_eq!(table_state.borrow().selected(), None);
}

// Tests that we can paginate through multiple pages and go back to the first page
#[tokio::test]
async fn multiple_pages_forward_and_back() {
    let mut test_app = TestApp::new();
    let res1 = create_execution_results("SELECT 1", 0);
    let event1 = AppEvent::ExecutionResultsNextBatch(res1);

    test_app.handle_app_event(AppEvent::NewExecution).unwrap();
    test_app.handle_app_event(event1).unwrap();

    {
        let state = test_app.state();
        let page = state.sql_tab.current_page().unwrap();
        assert_eq!(page, 0);
    }

    let res2 = create_execution_results("SELECT 1", 1);
    let event2 = AppEvent::ExecutionResultsNextBatch(res2);
    test_app.handle_app_event(event2).unwrap();

    {
        let state = test_app.state();
        let page = state.sql_tab.current_page().unwrap();
        assert_eq!(page, 1);
    }

    {
        let state = test_app.state();
        let batch = state.sql_tab.current_batch();
        assert!(batch.is_some());

        let batch = batch.unwrap();
        let batches = vec![batch.clone()];
        let expected = [
            "+---+---+",
            "| a | b |",
            "+---+---+",
            "| 2 | 4 |",
            "| 3 | 5 |",
            "+---+---+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    let left_key = crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Left,
        crossterm::event::KeyModifiers::NONE,
    );
    let event3 = AppEvent::Key(left_key);
    test_app.handle_app_event(event3).unwrap();

    {
        let state = test_app.state();
        let page = state.sql_tab.current_page().unwrap();
        assert_eq!(page, 0);
    }

    {
        let state = test_app.state();
        let batch = state.sql_tab.current_batch();
        assert!(batch.is_some());

        let batch = batch.unwrap();
        let batches = vec![batch.clone()];
        let expected = [
            "+---+---+",
            "| a | b |",
            "+---+---+",
            "| 1 | 3 |",
            "| 2 | 4 |",
            "+---+---+",
        ];
        assert_batches_eq!(expected, &batches);
    }
}

// Tests that we can still paginate when we already have the batch because we previously viewed the
// page
#[tokio::test]
async fn multiple_pages_forward_and_back_and_forward() {
    let mut test_app = TestApp::new();
    let res1 = create_execution_results("SELECT 1", 0);
    let event1 = AppEvent::ExecutionResultsNextBatch(res1);

    test_app.handle_app_event(AppEvent::NewExecution).unwrap();
    test_app.handle_app_event(event1).unwrap();

    {
        let state = test_app.state();
        let page = state.sql_tab.current_page().unwrap();
        assert_eq!(page, 0);
    }

    let res2 = create_execution_results("SELECT 1", 1);
    let event2 = AppEvent::ExecutionResultsNextBatch(res2);
    test_app.handle_app_event(event2).unwrap();

    let left_key = crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Left,
        crossterm::event::KeyModifiers::NONE,
    );
    let event3 = AppEvent::Key(left_key);
    test_app.handle_app_event(event3).unwrap();

    let right_key = crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Right,
        crossterm::event::KeyModifiers::NONE,
    );
    let event4 = AppEvent::Key(right_key);
    test_app.handle_app_event(event4).unwrap();

    {
        let state = test_app.state();
        let page = state.sql_tab.current_page().unwrap();
        assert_eq!(page, 1);
    }

    {
        let state = test_app.state();
        let batch = state.sql_tab.current_batch();
        assert!(batch.is_some());

        let batch = batch.unwrap();
        let batches = vec![batch.clone()];
        let expected = [
            "+---+---+",
            "| a | b |",
            "+---+---+",
            "| 2 | 4 |",
            "| 3 | 5 |",
            "+---+---+",
        ];
        assert_batches_eq!(expected, &batches);
    }
}

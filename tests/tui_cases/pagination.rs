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
use dft::app::{AppEvent, ExecutionResultsBatch};

use crate::TestApp;

fn create_batch() -> RecordBatch {
    let arr1: ArrayRef = Arc::new(UInt32Array::from(vec![1, 2]));
    let arr2: ArrayRef = Arc::new(UInt32Array::from(vec![4, 5]));

    RecordBatch::try_from_iter(vec![("a", arr1), ("b", arr2)]).unwrap()
}

fn create_execution_results(query: &str) -> ExecutionResultsBatch {
    let duration = Duration::from_secs(1);
    let batch = create_batch();
    ExecutionResultsBatch::new(query.to_string(), batch, duration)
}

#[tokio::test]
async fn single_page() {
    let mut test_app = TestApp::new();
    let res1 = create_execution_results("SELECT 1");
    let event1 = AppEvent::ExecutionResultsNextPage(res1);

    test_app.handle_app_event(AppEvent::NewExecution).unwrap();
    test_app.handle_app_event(event1).unwrap();

    let state = test_app.state();

    let page = state.sql_tab.results_page().unwrap();
    assert_eq!(page, 0);

    let batch = state.sql_tab.current_batch();
    assert!(batch.is_some());

    let batch = batch.unwrap();
    let batches = vec![batch.clone()];
    let expected = [
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | 4 |",
        "| 2 | 5 |",
        "+---+---+",
    ];
    assert_batches_eq!(expected, &batches);
    let table_state = state.sql_tab.query_results_state();
    assert!(table_state.is_some());
    let table_state = table_state.as_ref().unwrap();
    assert_eq!(table_state.borrow().selected(), None);
}

#[tokio::test]
async fn multiple_pages() {
    let mut test_app = TestApp::new();
    let res1 = create_execution_results("SELECT 1");
    let event1 = AppEvent::ExecutionResultsNextPage(res1);

    test_app.handle_app_event(AppEvent::NewExecution).unwrap();
    test_app.handle_app_event(event1).unwrap();

    {
        let state = test_app.state();
        let page = state.sql_tab.results_page().unwrap();
        assert_eq!(page, 0);
    }

    let res2 = create_execution_results("SELECT 1");
    let event1 = AppEvent::ExecutionResultsNextPage(res2);
    test_app.handle_app_event(event1).unwrap();

    {
        let state = test_app.state();
        let page = state.sql_tab.results_page().unwrap();
        assert_eq!(page, 1);
    }
}

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

use std::time::Duration;

use datafusion::arrow::array::RecordBatch;
use datafusion::assert_batches_eq;
use datafusion::execution::context::SessionContext;
use dft::tui::{AppEvent, ExecutionResultsBatch};
use itertools::Itertools;

use crate::tui_cases::TestApp;

async fn create_batch(sql: &str) -> RecordBatch {
    let ctx = SessionContext::new();
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    batches[0].clone()
}

async fn create_execution_results(query: &str) -> ExecutionResultsBatch {
    let duration = Duration::from_secs(1);
    let batch = create_batch(query).await;
    ExecutionResultsBatch::new(query.to_string(), batch, duration)
}

// Tests that a single page of results is displayed correctly
#[tokio::test]
async fn single_page() {
    let mut test_app = TestApp::new();

    test_app
        .handle_app_event(AppEvent::FlightSQLNewExecution)
        .unwrap();
    let res1 = create_execution_results("SELECT 1").await;
    let event1 = AppEvent::FlightSQLExecutionResultsNextBatch(res1);
    test_app.handle_app_event(event1).unwrap();

    let state = test_app.state();

    let page = state.flightsql_tab.current_page().unwrap();
    assert_eq!(page, 0);

    let batch = state.flightsql_tab.current_page_results();
    assert!(batch.is_some());

    let batch = batch.unwrap();
    let batches = vec![batch.clone()];
    let expected = [
        "+----------+",
        "| Int64(1) |",
        "+----------+",
        "| 1        |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &batches);
    let table_state = state.flightsql_tab.query_results_state();
    assert!(table_state.is_some());
    let table_state = table_state.as_ref().unwrap();
    assert_eq!(table_state.borrow().selected(), None);
}

fn create_values_query(num: usize) -> String {
    let base = "SELECT * FROM VALUES";
    let vals = (0..num).map(|i| format!("({i})")).join(",");
    format!("{base} {vals}")
}

// Tests that we can paginate through multiple pages and go back to the first page
#[tokio::test]
async fn multiple_pages_forward_and_back() {
    let mut test_app = TestApp::new();
    let query = create_values_query(101);
    let res1 = create_execution_results(&query).await;
    let event1 = AppEvent::FlightSQLExecutionResultsNextBatch(res1);

    test_app
        .handle_app_event(AppEvent::FlightSQLNewExecution)
        .unwrap();
    test_app.handle_app_event(event1).unwrap();

    {
        let state = test_app.state();
        let page = state.flightsql_tab.current_page().unwrap();
        assert_eq!(page, 0);
    }

    let event2 = AppEvent::FlightSQLExecutionResultsNextPage;
    test_app.handle_app_event(event2).unwrap();

    {
        let state = test_app.state();
        let page = state.flightsql_tab.current_page().unwrap();
        assert_eq!(page, 1);
    }

    {
        let state = test_app.state();
        let batch = state.flightsql_tab.current_page_results();
        assert!(batch.is_some());

        let batch = batch.unwrap();
        let batches = vec![batch.clone()];
        let expected = [
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 100     |",
            "+---------+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    let event3 = AppEvent::FlightSQLExecutionResultsPreviousPage;
    test_app.handle_app_event(event3).unwrap();

    {
        let state = test_app.state();
        let page = state.flightsql_tab.current_page().unwrap();
        assert_eq!(page, 0);
    }

    {
        let state = test_app.state();
        let batch = state.flightsql_tab.current_page_results();
        assert!(batch.is_some());

        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 100);
    }
}

// Tests that we can still paginate when we already have the batch because we previously viewed the
// page
#[tokio::test]
async fn multiple_pages_forward_and_back_and_forward() {
    let mut test_app = TestApp::new();
    let query = create_values_query(101);
    let res1 = create_execution_results(&query).await;
    let event1 = AppEvent::FlightSQLExecutionResultsNextBatch(res1);

    test_app
        .handle_app_event(AppEvent::FlightSQLNewExecution)
        .unwrap();
    test_app.handle_app_event(event1).unwrap();

    {
        let state = test_app.state();
        let page = state.flightsql_tab.current_page().unwrap();
        assert_eq!(page, 0);
    }

    let event2 = AppEvent::FlightSQLExecutionResultsNextPage;
    test_app.handle_app_event(event2).unwrap();

    let event3 = AppEvent::FlightSQLExecutionResultsPreviousPage;
    test_app.handle_app_event(event3).unwrap();

    let event4 = AppEvent::FlightSQLExecutionResultsNextPage;
    test_app.handle_app_event(event4).unwrap();

    {
        let state = test_app.state();
        let page = state.flightsql_tab.current_page().unwrap();
        assert_eq!(page, 1);
    }

    {
        let state = test_app.state();
        let batch = state.flightsql_tab.current_page_results();
        assert!(batch.is_some());

        let batch = batch.unwrap();
        let batches = vec![batch.clone()];
        let expected = [
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 100     |",
            "+---------+",
        ];
        assert_batches_eq!(expected, &batches);
    }
}

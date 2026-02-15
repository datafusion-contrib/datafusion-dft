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
use datafusion_dft::tui::execution::ExecutionResultsBatch;
use datafusion_dft::tui::AppEvent;
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
    let mut test_app = TestApp::new().await;

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

fn create_values_query_offset(num: usize, offset: usize) -> String {
    let base = "SELECT * FROM VALUES";
    let vals = (offset..offset + num).map(|i| format!("({i})")).join(",");
    format!("{base} {vals}")
}

// Tests that we can paginate through multiple pages and go back to the first page
#[tokio::test]
async fn multiple_pages_forward_and_back() {
    let mut test_app = TestApp::new().await;
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
    let mut test_app = TestApp::new().await;
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

// Tests lazy loading: only load batches as needed for pagination
// Simulates 3 batches: 60 rows, 60 rows, 20 rows (140 total)
#[tokio::test]
async fn multiple_batches_lazy_loading() {
    let mut test_app = TestApp::new().await;

    test_app
        .handle_app_event(AppEvent::FlightSQLNewExecution)
        .unwrap();

    // Send only first batch initially (lazy loading)
    let batch1 = create_execution_results(&create_values_query(60)).await;
    test_app
        .handle_app_event(AppEvent::FlightSQLExecutionResultsNextBatch(batch1))
        .unwrap();

    // Verify page 0 shows 60 rows (only first batch loaded)
    {
        let state = test_app.state();
        assert_eq!(state.flightsql_tab.current_page().unwrap(), 0);
        let page_results = state.flightsql_tab.current_page_results().unwrap();
        assert_eq!(page_results.num_rows(), 60);
    }

    // Send second batch (simulating lazy load)
    let batch2 = create_execution_results(&create_values_query_offset(60, 60)).await;
    test_app
        .handle_app_event(AppEvent::FlightSQLExecutionResultsNextBatch(batch2))
        .unwrap();

    // Now page 0 should show 100 rows (spanning both batches)
    {
        let state = test_app.state();
        let page_results = state.flightsql_tab.current_page_results().unwrap();
        assert_eq!(page_results.num_rows(), 100);
    }

    // Go to page 1
    test_app
        .handle_app_event(AppEvent::FlightSQLExecutionResultsNextPage)
        .unwrap();

    // Send third batch
    let batch3 = create_execution_results(&create_values_query_offset(20, 120)).await;
    test_app
        .handle_app_event(AppEvent::FlightSQLExecutionResultsNextBatch(batch3))
        .unwrap();

    // Verify page 1 shows remaining rows
    {
        let state = test_app.state();
        assert_eq!(state.flightsql_tab.current_page().unwrap(), 1);
        let page_results = state.flightsql_tab.current_page_results().unwrap();
        assert_eq!(page_results.num_rows(), 40);
    }
}

// Tests that multiple small batches are automatically loaded to fill a page
// This verifies the fix for the issue where user would have to press Right multiple times
// Scenario: Page needs 100 rows, but each batch only has 30 rows
#[tokio::test]
async fn multiple_small_batches_auto_load() {
    let mut test_app = TestApp::new().await;

    test_app
        .handle_app_event(AppEvent::FlightSQLNewExecution)
        .unwrap();

    // Send first batch: 100 rows (fills page 0)
    let batch1 = create_execution_results(&create_values_query(100)).await;
    test_app
        .handle_app_event(AppEvent::FlightSQLExecutionResultsNextBatch(batch1))
        .unwrap();

    // Verify we're on page 0 with 100 rows
    {
        let state = test_app.state();
        assert_eq!(state.flightsql_tab.current_page().unwrap(), 0);
        assert_eq!(state.flightsql_tab.total_loaded_rows(), 100);
    }

    // Send second batch: only 30 rows (NOT enough for page 1 which needs rows 100-199)
    // The system should NOT advance the page yet
    let batch2 = create_execution_results(&create_values_query_offset(30, 100)).await;
    test_app
        .handle_app_event(AppEvent::FlightSQLExecutionResultsNextBatch(batch2))
        .unwrap();

    // Verify we're still on page 0, but now have 130 rows total
    {
        let state = test_app.state();
        assert_eq!(state.flightsql_tab.current_page().unwrap(), 0);
        assert_eq!(state.flightsql_tab.total_loaded_rows(), 130);
        // Verify we still need more batches for page 1
        assert!(state.flightsql_tab.needs_more_batches_for_page(1));
    }

    // Send third batch: another 30 rows (total 160, still NOT enough)
    let batch3 = create_execution_results(&create_values_query_offset(30, 130)).await;
    test_app
        .handle_app_event(AppEvent::FlightSQLExecutionResultsNextBatch(batch3))
        .unwrap();

    // Verify we're still on page 0, but now have 160 rows total
    {
        let state = test_app.state();
        assert_eq!(state.flightsql_tab.current_page().unwrap(), 0);
        assert_eq!(state.flightsql_tab.total_loaded_rows(), 160);
        // Verify we still need more batches for page 1
        assert!(state.flightsql_tab.needs_more_batches_for_page(1));
    }

    // Send fourth batch: another 40 rows (total 200, NOW enough for page 1!)
    // The system should automatically advance to page 1
    let batch4 = create_execution_results(&create_values_query_offset(40, 160)).await;
    test_app
        .handle_app_event(AppEvent::FlightSQLExecutionResultsNextBatch(batch4))
        .unwrap();

    // Process the automatic NextPage event that was queued
    // In the real app, this happens automatically in the event loop
    // In tests, we need to simulate it by checking for and processing the event
    // Since we can't easily intercept the event queue in tests, we verify the state is ready
    {
        let state = test_app.state();
        assert_eq!(state.flightsql_tab.total_loaded_rows(), 200);
        // Verify we now have enough data for page 1
        assert!(!state.flightsql_tab.needs_more_batches_for_page(1));
    }

    // Now when user manually advances (or automatic event processes), we can show page 1
    test_app
        .handle_app_event(AppEvent::FlightSQLExecutionResultsNextPage)
        .unwrap();

    {
        let state = test_app.state();
        assert_eq!(state.flightsql_tab.current_page().unwrap(), 1);
        let page_results = state.flightsql_tab.current_page_results().unwrap();
        assert_eq!(page_results.num_rows(), 100); // Page 1 shows rows 100-199
    }
}

// Tests that the system correctly handles the case where exactly enough batches
// are loaded to fill a page (boundary condition)
#[tokio::test]
async fn exact_batches_for_page() {
    let mut test_app = TestApp::new().await;

    test_app
        .handle_app_event(AppEvent::FlightSQLNewExecution)
        .unwrap();

    // Send first batch: 50 rows
    let batch1 = create_execution_results(&create_values_query(50)).await;
    test_app
        .handle_app_event(AppEvent::FlightSQLExecutionResultsNextBatch(batch1))
        .unwrap();

    // Verify page 0 shows 50 rows
    {
        let state = test_app.state();
        assert_eq!(state.flightsql_tab.current_page().unwrap(), 0);
        assert_eq!(state.flightsql_tab.total_loaded_rows(), 50);
    }

    // Send second batch: another 50 rows (total 100, exactly fills page 0)
    let batch2 = create_execution_results(&create_values_query_offset(50, 50)).await;
    test_app
        .handle_app_event(AppEvent::FlightSQLExecutionResultsNextBatch(batch2))
        .unwrap();

    // Verify page 0 now shows 100 rows
    {
        let state = test_app.state();
        assert_eq!(state.flightsql_tab.current_page().unwrap(), 0);
        let page_results = state.flightsql_tab.current_page_results().unwrap();
        assert_eq!(page_results.num_rows(), 100);
        assert_eq!(state.flightsql_tab.total_loaded_rows(), 100);
    }

    // Send third batch: another 50 rows (total 150, NOT enough for full page 1)
    let batch3 = create_execution_results(&create_values_query_offset(50, 100)).await;
    test_app
        .handle_app_event(AppEvent::FlightSQLExecutionResultsNextBatch(batch3))
        .unwrap();

    {
        let state = test_app.state();
        assert_eq!(state.flightsql_tab.total_loaded_rows(), 150);
        assert!(state.flightsql_tab.needs_more_batches_for_page(1));
    }

    // Send fourth batch: exactly 50 more rows (total 200, exactly fills page 1)
    let batch4 = create_execution_results(&create_values_query_offset(50, 150)).await;
    test_app
        .handle_app_event(AppEvent::FlightSQLExecutionResultsNextBatch(batch4))
        .unwrap();

    {
        let state = test_app.state();
        assert_eq!(state.flightsql_tab.total_loaded_rows(), 200);
        assert!(!state.flightsql_tab.needs_more_batches_for_page(1));
    }

    // Advance to page 1
    test_app
        .handle_app_event(AppEvent::FlightSQLExecutionResultsNextPage)
        .unwrap();

    {
        let state = test_app.state();
        assert_eq!(state.flightsql_tab.current_page().unwrap(), 1);
        let page_results = state.flightsql_tab.current_page_results().unwrap();
        assert_eq!(page_results.num_rows(), 100);
    }
}

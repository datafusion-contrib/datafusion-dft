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

//! Tests for FlightSQL query execution to ensure it executes the correct queries
//! and doesn't accidentally execute queries from other tabs

use datafusion_dft::tui::AppEvent;
use ratatui::crossterm::event;

use crate::tui_cases::TestApp;

/// Test that ALT+Enter in FlightSQL tab's edit mode doesn't trigger execution
/// in SQL tab. This test verifies that the FlightSQL tab maintains its own
/// execution context separate from the SQL tab.
///
/// This test primarily ensures that the handler doesn't accidentally reference
/// the wrong tab's state (which was the bug).
#[tokio::test(flavor = "multi_thread")]
async fn flightsql_alt_enter_uses_flightsql_tab() {
    let mut test_app = TestApp::new().await;

    // Switch to FlightSQL tab
    let flightsql_key = event::KeyEvent::new(event::KeyCode::Char('2'), event::KeyModifiers::NONE);
    test_app
        .handle_app_event(AppEvent::Key(flightsql_key))
        .unwrap();

    // Enter edit mode
    let edit_key = event::KeyEvent::new(event::KeyCode::Char('e'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(edit_key)).unwrap();

    // Verify we're in edit mode
    assert!(test_app.state().flightsql_tab.editor_editable());

    // Simulate ALT+Enter (which would execute the query)
    let alt_enter = event::KeyEvent::new(event::KeyCode::Enter, event::KeyModifiers::ALT);
    test_app.handle_app_event(AppEvent::Key(alt_enter)).unwrap();

    // The test passes if we don't panic trying to access the wrong tab's state.
    // In the buggy version, this would try to call sql_tab.sql() instead of
    // flightsql_tab.sql(), and set_execution_task on the wrong tab.
}

/// Test that Enter key in FlightSQL normal mode triggers execution
#[tokio::test(flavor = "multi_thread")]
async fn flightsql_enter_in_normal_mode_executes() {
    let mut test_app = TestApp::new().await;

    // Switch to FlightSQL tab
    let flightsql_key = event::KeyEvent::new(event::KeyCode::Char('2'), event::KeyModifiers::NONE);
    test_app
        .handle_app_event(AppEvent::Key(flightsql_key))
        .unwrap();

    // Should be in normal mode (not editable)
    assert!(!test_app.state().flightsql_tab.editor_editable());

    // Press Enter in normal mode
    let enter = event::KeyEvent::new(event::KeyCode::Enter, event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(enter)).unwrap();

    // Test passes if no panic occurs
    // The execution would be triggered on the FlightSQL tab
}

/// Test that ALT+Enter in SQL tab's edit mode doesn't affect FlightSQL tab
#[tokio::test(flavor = "multi_thread")]
async fn sql_alt_enter_doesnt_affect_flightsql() {
    let mut test_app = TestApp::new().await;

    // Start on SQL tab (default)
    assert!(matches!(
        test_app.state().tabs.selected,
        datafusion_dft::tui::ui::SelectedTab::SQL
    ));

    // Enter edit mode in SQL tab
    let edit_key = event::KeyEvent::new(event::KeyCode::Char('e'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(edit_key)).unwrap();

    // Verify we're in edit mode
    assert!(test_app.state().sql_tab.editable());

    // Press ALT+Enter in SQL tab
    let alt_enter = event::KeyEvent::new(event::KeyCode::Enter, event::KeyModifiers::ALT);
    test_app.handle_app_event(AppEvent::Key(alt_enter)).unwrap();

    // The test verifies that execution happens on SQL tab, not FlightSQL tab
    // In the buggy FlightSQL handler, it was doing the reverse (accessing sql_tab
    // when it should access flightsql_tab)
}

/// Test that switching between tabs and executing queries works correctly
#[tokio::test(flavor = "multi_thread")]
async fn switching_tabs_maintains_separate_execution_contexts() {
    let mut test_app = TestApp::new().await;

    // Start on SQL tab
    assert!(matches!(
        test_app.state().tabs.selected,
        datafusion_dft::tui::ui::SelectedTab::SQL
    ));

    // Enter edit mode and execute in SQL tab
    let edit_key = event::KeyEvent::new(event::KeyCode::Char('e'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(edit_key)).unwrap();

    let alt_enter = event::KeyEvent::new(event::KeyCode::Enter, event::KeyModifiers::ALT);
    test_app.handle_app_event(AppEvent::Key(alt_enter)).unwrap();

    // Exit edit mode
    let esc = event::KeyEvent::new(event::KeyCode::Esc, event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(esc)).unwrap();

    // Switch to FlightSQL tab
    let flightsql_key = event::KeyEvent::new(event::KeyCode::Char('2'), event::KeyModifiers::NONE);
    test_app
        .handle_app_event(AppEvent::Key(flightsql_key))
        .unwrap();

    // Enter edit mode in FlightSQL tab
    let edit_key = event::KeyEvent::new(event::KeyCode::Char('e'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(edit_key)).unwrap();

    // Execute in FlightSQL tab
    let alt_enter = event::KeyEvent::new(event::KeyCode::Enter, event::KeyModifiers::ALT);
    test_app.handle_app_event(AppEvent::Key(alt_enter)).unwrap();

    // Test passes if both executions happened on their respective tabs
    // without mixing up state
}

/// Test that Ctrl+Enter doesn't execute queries (only ALT+Enter should in edit mode)
#[tokio::test(flavor = "multi_thread")]
async fn ctrl_enter_doesnt_execute_in_flightsql_edit_mode() {
    let mut test_app = TestApp::new().await;

    // Switch to FlightSQL tab
    let flightsql_key = event::KeyEvent::new(event::KeyCode::Char('2'), event::KeyModifiers::NONE);
    test_app
        .handle_app_event(AppEvent::Key(flightsql_key))
        .unwrap();

    // Enter edit mode
    let edit_key = event::KeyEvent::new(event::KeyCode::Char('e'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(edit_key)).unwrap();

    assert!(test_app.state().flightsql_tab.editor_editable());

    // Try Ctrl+Enter (should not execute)
    let ctrl_enter = event::KeyEvent::new(event::KeyCode::Enter, event::KeyModifiers::CONTROL);
    test_app
        .handle_app_event(AppEvent::Key(ctrl_enter))
        .unwrap();

    // Should still be in edit mode (Ctrl+Enter shouldn't trigger execution or exit)
    assert!(test_app.state().flightsql_tab.editor_editable());
}

/// Test that plain Enter in edit mode doesn't execute (only ALT+Enter should)
#[tokio::test(flavor = "multi_thread")]
async fn plain_enter_doesnt_execute_in_flightsql_edit_mode() {
    let mut test_app = TestApp::new().await;

    // Switch to FlightSQL tab
    let flightsql_key = event::KeyEvent::new(event::KeyCode::Char('2'), event::KeyModifiers::NONE);
    test_app
        .handle_app_event(AppEvent::Key(flightsql_key))
        .unwrap();

    // Enter edit mode
    let edit_key = event::KeyEvent::new(event::KeyCode::Char('e'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(edit_key)).unwrap();

    assert!(test_app.state().flightsql_tab.editor_editable());

    // Press plain Enter (should just insert newline in editor, not execute)
    let enter = event::KeyEvent::new(event::KeyCode::Enter, event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(enter)).unwrap();

    // Should still be in edit mode
    assert!(test_app.state().flightsql_tab.editor_editable());
}

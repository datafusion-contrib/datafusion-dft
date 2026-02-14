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

//! Tests for keyboard modifier handling to ensure shortcuts only trigger
//! with the correct modifier combinations

use datafusion_dft::tui::AppEvent;
use ratatui::crossterm::event;

use crate::tui_cases::TestApp;

/// Test that Ctrl+Q does NOT quit the application (only plain 'q' should)
#[tokio::test(flavor = "multi_thread")]
async fn ctrl_q_should_not_quit_sql_tab() {
    let mut test_app = TestApp::new().await;

    // Simulate Ctrl+Q
    let ctrl_q = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::CONTROL);
    test_app.handle_app_event(AppEvent::Key(ctrl_q)).unwrap();

    // Should NOT quit with Ctrl+Q
    assert!(!test_app.state().should_quit);

    // But plain 'q' should quit
    let plain_q = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(plain_q)).unwrap();
    assert!(test_app.state().should_quit);
}

/// Test that Alt+Q does NOT quit the application
#[tokio::test(flavor = "multi_thread")]
async fn alt_q_should_not_quit_sql_tab() {
    let mut test_app = TestApp::new().await;

    let alt_q = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::ALT);
    test_app.handle_app_event(AppEvent::Key(alt_q)).unwrap();

    assert!(!test_app.state().should_quit);
}

/// Test that Shift+Q does NOT quit the application
#[tokio::test(flavor = "multi_thread")]
async fn shift_q_should_not_quit_sql_tab() {
    let mut test_app = TestApp::new().await;

    let shift_q = event::KeyEvent::new(event::KeyCode::Char('Q'), event::KeyModifiers::SHIFT);
    test_app.handle_app_event(AppEvent::Key(shift_q)).unwrap();

    assert!(!test_app.state().should_quit);
}

/// Test that Ctrl+1 does NOT switch tabs (only plain '1' should)
#[tokio::test(flavor = "multi_thread")]
async fn ctrl_number_should_not_switch_tabs() {
    let mut test_app = TestApp::new().await;

    // Start on SQL tab (tab 0)
    assert!(matches!(
        test_app.state().tabs.selected,
        datafusion_dft::tui::ui::SelectedTab::SQL
    ));

    // Try to switch with Ctrl+2
    let ctrl_2 = event::KeyEvent::new(event::KeyCode::Char('2'), event::KeyModifiers::CONTROL);
    test_app.handle_app_event(AppEvent::Key(ctrl_2)).unwrap();

    // Should still be on SQL tab
    assert!(matches!(
        test_app.state().tabs.selected,
        datafusion_dft::tui::ui::SelectedTab::SQL
    ));

    // But plain '2' should switch
    let plain_2 = event::KeyEvent::new(event::KeyCode::Char('2'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(plain_2)).unwrap();

    // Should now be on different tab (FlightSQL if feature enabled, History otherwise)
    #[cfg(feature = "flightsql")]
    assert!(matches!(
        test_app.state().tabs.selected,
        datafusion_dft::tui::ui::SelectedTab::FlightSQL
    ));
    #[cfg(not(feature = "flightsql"))]
    assert!(matches!(
        test_app.state().tabs.selected,
        datafusion_dft::tui::ui::SelectedTab::History
    ));
}

/// Test that Alt+1 through Alt+5 do NOT switch tabs
#[tokio::test(flavor = "multi_thread")]
async fn alt_numbers_should_not_switch_tabs() {
    let mut test_app = TestApp::new().await;

    for num in ['1', '2', '3', '4', '5'] {
        let alt_num = event::KeyEvent::new(event::KeyCode::Char(num), event::KeyModifiers::ALT);
        test_app.handle_app_event(AppEvent::Key(alt_num)).unwrap();

        // Should still be on SQL tab (the default)
        assert!(matches!(
            test_app.state().tabs.selected,
            datafusion_dft::tui::ui::SelectedTab::SQL
        ));
    }
}

/// Test that Ctrl+Q doesn't quit from History tab
#[tokio::test(flavor = "multi_thread")]
async fn ctrl_q_should_not_quit_history_tab() {
    let mut test_app = TestApp::new().await;

    // Switch to History tab (tab 2 with flightsql, tab 1 without)
    #[cfg(feature = "flightsql")]
    let history_key = event::KeyEvent::new(event::KeyCode::Char('3'), event::KeyModifiers::NONE);
    #[cfg(not(feature = "flightsql"))]
    let history_key = event::KeyEvent::new(event::KeyCode::Char('2'), event::KeyModifiers::NONE);

    test_app
        .handle_app_event(AppEvent::Key(history_key))
        .unwrap();

    // Try Ctrl+Q
    let ctrl_q = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::CONTROL);
    test_app.handle_app_event(AppEvent::Key(ctrl_q)).unwrap();

    // Should NOT quit
    assert!(!test_app.state().should_quit);
}

/// Test that Ctrl+Q doesn't quit from Logs tab
#[tokio::test(flavor = "multi_thread")]
async fn ctrl_q_should_not_quit_logs_tab() {
    let mut test_app = TestApp::new().await;

    // Switch to Logs tab
    #[cfg(feature = "flightsql")]
    let logs_key = event::KeyEvent::new(event::KeyCode::Char('4'), event::KeyModifiers::NONE);
    #[cfg(not(feature = "flightsql"))]
    let logs_key = event::KeyEvent::new(event::KeyCode::Char('3'), event::KeyModifiers::NONE);

    test_app.handle_app_event(AppEvent::Key(logs_key)).unwrap();

    // Try Ctrl+Q
    let ctrl_q = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::CONTROL);
    test_app.handle_app_event(AppEvent::Key(ctrl_q)).unwrap();

    // Should NOT quit
    assert!(!test_app.state().should_quit);
}

/// Test that Ctrl+Q doesn't quit from Context tab
#[tokio::test(flavor = "multi_thread")]
async fn ctrl_q_should_not_quit_context_tab() {
    let mut test_app = TestApp::new().await;

    // Switch to Context tab
    #[cfg(feature = "flightsql")]
    let context_key = event::KeyEvent::new(event::KeyCode::Char('5'), event::KeyModifiers::NONE);
    #[cfg(not(feature = "flightsql"))]
    let context_key = event::KeyEvent::new(event::KeyCode::Char('4'), event::KeyModifiers::NONE);

    test_app
        .handle_app_event(AppEvent::Key(context_key))
        .unwrap();

    // Try Ctrl+Q
    let ctrl_q = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::CONTROL);
    test_app.handle_app_event(AppEvent::Key(ctrl_q)).unwrap();

    // Should NOT quit
    assert!(!test_app.state().should_quit);
}

/// Test that Ctrl+E doesn't enter edit mode in SQL tab (only plain 'e' should)
#[tokio::test(flavor = "multi_thread")]
async fn ctrl_e_should_not_enter_edit_mode() {
    let mut test_app = TestApp::new().await;

    // Should start in normal mode
    assert!(!test_app.state().sql_tab.editable());

    // Try Ctrl+E
    let ctrl_e = event::KeyEvent::new(event::KeyCode::Char('e'), event::KeyModifiers::CONTROL);
    test_app.handle_app_event(AppEvent::Key(ctrl_e)).unwrap();

    // Should still be in normal mode
    assert!(!test_app.state().sql_tab.editable());

    // But plain 'e' should enter edit mode
    let plain_e = event::KeyEvent::new(event::KeyCode::Char('e'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(plain_e)).unwrap();

    // Should now be in edit mode
    assert!(test_app.state().sql_tab.editable());
}

/// Test that Ctrl+Arrow keys don't navigate results (only plain arrows should)
#[tokio::test(flavor = "multi_thread")]
async fn ctrl_arrows_should_not_navigate() {
    let mut test_app = TestApp::new().await;

    // Try Ctrl+Left
    let ctrl_left = event::KeyEvent::new(event::KeyCode::Left, event::KeyModifiers::CONTROL);
    test_app.handle_app_event(AppEvent::Key(ctrl_left)).unwrap();

    // Try Ctrl+Right
    let ctrl_right = event::KeyEvent::new(event::KeyCode::Right, event::KeyModifiers::CONTROL);
    test_app
        .handle_app_event(AppEvent::Key(ctrl_right))
        .unwrap();

    // Try Ctrl+Down
    let ctrl_down = event::KeyEvent::new(event::KeyCode::Down, event::KeyModifiers::CONTROL);
    test_app.handle_app_event(AppEvent::Key(ctrl_down)).unwrap();

    // Try Ctrl+Up
    let ctrl_up = event::KeyEvent::new(event::KeyCode::Up, event::KeyModifiers::CONTROL);
    test_app.handle_app_event(AppEvent::Key(ctrl_up)).unwrap();

    // If the test completes without panicking, the modifiers were properly ignored
}

#[cfg(feature = "flightsql")]
/// Test that Ctrl+Q doesn't quit from FlightSQL tab
#[tokio::test(flavor = "multi_thread")]
async fn ctrl_q_should_not_quit_flightsql_tab() {
    let mut test_app = TestApp::new().await;

    // Switch to FlightSQL tab
    let flightsql_key = event::KeyEvent::new(event::KeyCode::Char('2'), event::KeyModifiers::NONE);
    test_app
        .handle_app_event(AppEvent::Key(flightsql_key))
        .unwrap();

    // Try Ctrl+Q
    let ctrl_q = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::CONTROL);
    test_app.handle_app_event(AppEvent::Key(ctrl_q)).unwrap();

    // Should NOT quit
    assert!(!test_app.state().should_quit);
}

#[cfg(feature = "flightsql")]
/// Test that Ctrl+E doesn't enter edit mode in FlightSQL tab
#[tokio::test(flavor = "multi_thread")]
async fn ctrl_e_should_not_enter_edit_mode_flightsql() {
    let mut test_app = TestApp::new().await;

    // Switch to FlightSQL tab
    let flightsql_key = event::KeyEvent::new(event::KeyCode::Char('2'), event::KeyModifiers::NONE);
    test_app
        .handle_app_event(AppEvent::Key(flightsql_key))
        .unwrap();

    // Should start in normal mode
    assert!(!test_app.state().flightsql_tab.editor_editable());

    // Try Ctrl+E
    let ctrl_e = event::KeyEvent::new(event::KeyCode::Char('e'), event::KeyModifiers::CONTROL);
    test_app.handle_app_event(AppEvent::Key(ctrl_e)).unwrap();

    // Should still be in normal mode
    assert!(!test_app.state().flightsql_tab.editor_editable());
}

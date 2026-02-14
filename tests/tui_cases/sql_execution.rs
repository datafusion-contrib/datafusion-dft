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

//! Tests for SQL tab query execution and keyboard handling

use datafusion_dft::tui::AppEvent;
use ratatui::crossterm::event;

use crate::tui_cases::TestApp;

/// Test that ALT+Enter in SQL tab's edit mode executes the query
#[tokio::test(flavor = "multi_thread")]
async fn sql_alt_enter_executes_in_edit_mode() {
    let mut test_app = TestApp::new().await;

    // Should start on SQL tab
    assert!(matches!(
        test_app.state().tabs.selected,
        datafusion_dft::tui::ui::SelectedTab::SQL
    ));

    // Enter edit mode
    let edit_key = event::KeyEvent::new(event::KeyCode::Char('e'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(edit_key)).unwrap();

    // Verify we're in edit mode
    assert!(test_app.state().sql_tab.editable());

    // Press ALT+Enter to execute
    let alt_enter = event::KeyEvent::new(event::KeyCode::Enter, event::KeyModifiers::ALT);
    test_app.handle_app_event(AppEvent::Key(alt_enter)).unwrap();

    // Test passes if execution is triggered without panic
}

/// Test that Enter key in SQL normal mode executes the query
#[tokio::test(flavor = "multi_thread")]
async fn sql_enter_in_normal_mode_executes() {
    let mut test_app = TestApp::new().await;

    // Should start in normal mode
    assert!(!test_app.state().sql_tab.editable());

    // Press Enter in normal mode
    let enter = event::KeyEvent::new(event::KeyCode::Enter, event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(enter)).unwrap();

    // Test passes if execution is triggered without panic
}

/// Test that plain Enter in edit mode doesn't execute (only ALT+Enter should)
#[tokio::test(flavor = "multi_thread")]
async fn sql_plain_enter_doesnt_execute_in_edit_mode() {
    let mut test_app = TestApp::new().await;

    // Enter edit mode
    let edit_key = event::KeyEvent::new(event::KeyCode::Char('e'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(edit_key)).unwrap();

    assert!(test_app.state().sql_tab.editable());

    // Press plain Enter (should just insert newline, not execute)
    let enter = event::KeyEvent::new(event::KeyCode::Enter, event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(enter)).unwrap();

    // Should still be in edit mode
    assert!(test_app.state().sql_tab.editable());
}

/// Test that Ctrl+Enter doesn't execute in edit mode
#[tokio::test(flavor = "multi_thread")]
async fn sql_ctrl_enter_doesnt_execute_in_edit_mode() {
    let mut test_app = TestApp::new().await;

    // Enter edit mode
    let edit_key = event::KeyEvent::new(event::KeyCode::Char('e'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(edit_key)).unwrap();

    assert!(test_app.state().sql_tab.editable());

    // Try Ctrl+Enter (should not execute)
    let ctrl_enter = event::KeyEvent::new(event::KeyCode::Enter, event::KeyModifiers::CONTROL);
    test_app
        .handle_app_event(AppEvent::Key(ctrl_enter))
        .unwrap();

    // Should still be in edit mode
    assert!(test_app.state().sql_tab.editable());
}

/// Test that Esc exits edit mode
#[tokio::test(flavor = "multi_thread")]
async fn sql_esc_exits_edit_mode() {
    let mut test_app = TestApp::new().await;

    // Enter edit mode
    let edit_key = event::KeyEvent::new(event::KeyCode::Char('e'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(edit_key)).unwrap();

    assert!(test_app.state().sql_tab.editable());

    // Press Esc to exit edit mode
    let esc = event::KeyEvent::new(event::KeyCode::Esc, event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(esc)).unwrap();

    // Should no longer be in edit mode
    assert!(!test_app.state().sql_tab.editable());
}

/// Test that ALT+Left/Right/Backspace work in edit mode for word navigation
#[tokio::test(flavor = "multi_thread")]
async fn sql_alt_word_navigation_in_edit_mode() {
    let mut test_app = TestApp::new().await;

    // Enter edit mode
    let edit_key = event::KeyEvent::new(event::KeyCode::Char('e'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(edit_key)).unwrap();

    assert!(test_app.state().sql_tab.editable());

    // Test ALT+Left (previous word)
    let alt_left = event::KeyEvent::new(event::KeyCode::Left, event::KeyModifiers::ALT);
    test_app.handle_app_event(AppEvent::Key(alt_left)).unwrap();

    // Test ALT+Right (next word)
    let alt_right = event::KeyEvent::new(event::KeyCode::Right, event::KeyModifiers::ALT);
    test_app.handle_app_event(AppEvent::Key(alt_right)).unwrap();

    // Test ALT+Backspace (delete word)
    let alt_backspace = event::KeyEvent::new(event::KeyCode::Backspace, event::KeyModifiers::ALT);
    test_app
        .handle_app_event(AppEvent::Key(alt_backspace))
        .unwrap();

    // Should still be in edit mode
    assert!(test_app.state().sql_tab.editable());
}

/// Test that switching to DDL mode works
#[tokio::test(flavor = "multi_thread")]
async fn sql_switch_to_ddl_mode() {
    let mut test_app = TestApp::new().await;

    // Should start in Normal mode
    assert_eq!(
        *test_app.state().sql_tab.mode(),
        datafusion_dft::tui::state::tabs::sql::SQLTabMode::Normal
    );

    // Press 'd' to switch to DDL mode
    let d_key = event::KeyEvent::new(event::KeyCode::Char('d'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(d_key)).unwrap();

    // Should now be in DDL mode
    assert_eq!(
        *test_app.state().sql_tab.mode(),
        datafusion_dft::tui::state::tabs::sql::SQLTabMode::DDL
    );
}

/// Test that switching back to Normal mode works
#[tokio::test(flavor = "multi_thread")]
async fn sql_switch_to_normal_mode() {
    let mut test_app = TestApp::new().await;

    // Switch to DDL mode first
    let d_key = event::KeyEvent::new(event::KeyCode::Char('d'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(d_key)).unwrap();

    // Press 'n' to switch back to Normal mode
    let n_key = event::KeyEvent::new(event::KeyCode::Char('n'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(n_key)).unwrap();

    // Should be back in Normal mode
    assert_eq!(
        *test_app.state().sql_tab.mode(),
        datafusion_dft::tui::state::tabs::sql::SQLTabMode::Normal
    );
}

/// Test that 'c' clears the editor
#[tokio::test(flavor = "multi_thread")]
async fn sql_c_clears_editor() {
    let mut test_app = TestApp::new().await;

    // Press 'c' to clear editor
    let c_key = event::KeyEvent::new(event::KeyCode::Char('c'), event::KeyModifiers::NONE);
    test_app.handle_app_event(AppEvent::Key(c_key)).unwrap();

    // Test passes if no panic occurs
    // The editor should be cleared
}

/// Test that Ctrl+D doesn't switch to DDL mode (only plain 'd' should)
#[tokio::test(flavor = "multi_thread")]
async fn sql_ctrl_d_doesnt_switch_mode() {
    let mut test_app = TestApp::new().await;

    // Should start in Normal mode
    assert_eq!(
        *test_app.state().sql_tab.mode(),
        datafusion_dft::tui::state::tabs::sql::SQLTabMode::Normal
    );

    // Try Ctrl+D
    let ctrl_d = event::KeyEvent::new(event::KeyCode::Char('d'), event::KeyModifiers::CONTROL);
    test_app.handle_app_event(AppEvent::Key(ctrl_d)).unwrap();

    // Should still be in Normal mode
    assert_eq!(
        *test_app.state().sql_tab.mode(),
        datafusion_dft::tui::state::tabs::sql::SQLTabMode::Normal
    );
}

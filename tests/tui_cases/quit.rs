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

use dft::execution::ExecutionContext;
use dft::tui::state::initialize;
use dft::tui::{App, AppEvent};
use ratatui::crossterm::event;
use tempfile::{tempdir, TempDir};

#[tokio::test]
async fn construct_with_no_args() {
    let _test_app = TestApp::new();
}

#[tokio::test]
async fn quit_app_from_sql_tab() {
    let mut test_app = TestApp::new();
    // SQL Tab
    let key = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(key);
    test_app.handle_app_event(app_event).unwrap();
    // Ideally, we figure out a way to check that the app actually quits
    assert!(test_app.state().should_quit);
}

#[tokio::test]
async fn quit_app_from_flightsql_tab() {
    let mut test_app = TestApp::new();
    let flightsql_key = event::KeyEvent::new(event::KeyCode::Char('2'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(flightsql_key);
    test_app.handle_app_event(app_event).unwrap();
    let key = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(key);
    test_app.handle_app_event(app_event).unwrap();
    assert!(test_app.state().should_quit);
}

#[tokio::test]
async fn quit_app_from_history_tab() {
    let mut test_app = TestApp::new();
    let history_key = event::KeyEvent::new(event::KeyCode::Char('3'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(history_key);
    test_app.handle_app_event(app_event).unwrap();
    let key = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(key);
    test_app.handle_app_event(app_event).unwrap();
    assert!(test_app.state().should_quit);
}

#[tokio::test]
async fn quit_app_from_logs_tab() {
    let mut test_app = TestApp::new();
    let logs_key = event::KeyEvent::new(event::KeyCode::Char('4'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(logs_key);
    test_app.handle_app_event(app_event).unwrap();
    let key = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(key);
    test_app.handle_app_event(app_event).unwrap();
    assert!(test_app.state().should_quit);
}

#[tokio::test]
async fn quit_app_from_context_tab() {
    let mut test_app = TestApp::new();
    let context_key = event::KeyEvent::new(event::KeyCode::Char('5'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(context_key);
    test_app.handle_app_event(app_event).unwrap();
    let key = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(key);
    test_app.handle_app_event(app_event).unwrap();
    assert!(test_app.state().should_quit);
}

/// Fixture with an [`App`] instance and other temporary state
struct TestApp<'app> {
    /// Temporary directory for configuration files
    ///
    /// The directory is removed when the object is dropped so this
    /// field must remain alive while the app is running
    #[allow(dead_code)]
    config_path: TempDir,
    /// The [`App`] instance
    app: App<'app>,
}

impl<'app> TestApp<'app> {
    /// Create a new [`TestApp`] instance configured with a temporary directory
    fn new() -> Self {
        let config_path = tempdir().unwrap();
        let state = initialize(config_path.path().to_path_buf());
        let execution = ExecutionContext::try_new(&state.config.execution).unwrap();
        let app = App::new(state, execution);
        Self { config_path, app }
    }

    /// Call app.event_handler with the given event
    pub fn handle_app_event(&mut self, event: AppEvent) -> color_eyre::Result<()> {
        self.app.handle_app_event(event)
    }

    /// Return the app state
    pub fn state(&self) -> &dft::tui::state::AppState {
        self.app.state()
    }
}

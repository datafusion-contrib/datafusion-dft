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

use dft::app::state::initialize;
use dft::app::{App, AppEvent};
use ratatui::crossterm::event;
use tempfile::tempdir;

#[tokio::test]
async fn run_app_with_no_args() {
    let config_path = tempdir().unwrap();
    let state = initialize(config_path.path().to_path_buf());
    let _app = App::new(state);
}

#[tokio::test]
async fn quit_app_from_all_tabs() {
    let config_path = tempdir().unwrap();
    let state = initialize(config_path.path().to_path_buf());

    // SQL Tab
    let mut app = App::new(state);
    let key = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(key);
    app.handle_app_event(app_event).unwrap();
    // Ideally, we figure out a way to check that the app actually quits
    assert!(app.state().should_quit);

    // FlightSQL Tab
    let state = initialize(config_path.path().to_path_buf());
    let mut app = App::new(state);
    let flightsql_key = event::KeyEvent::new(event::KeyCode::Char('2'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(flightsql_key);
    app.handle_app_event(app_event).unwrap();
    let key = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(key);
    app.handle_app_event(app_event).unwrap();
    assert!(app.state().should_quit);

    // History Tab
    let state = initialize(config_path.path().to_path_buf());
    let mut app = App::new(state);
    let history_key = event::KeyEvent::new(event::KeyCode::Char('3'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(history_key);
    app.handle_app_event(app_event).unwrap();
    let key = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(key);
    app.handle_app_event(app_event).unwrap();
    assert!(app.state().should_quit);

    // Logs Tab
    let state = initialize(config_path.path().to_path_buf());
    let mut app = App::new(state);
    let logs_key = event::KeyEvent::new(event::KeyCode::Char('4'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(logs_key);
    app.handle_app_event(app_event).unwrap();
    let key = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(key);
    app.handle_app_event(app_event).unwrap();
    assert!(app.state().should_quit);

    // Context Tab
    let state = initialize(config_path.path().to_path_buf());
    let mut app = App::new(state);
    let context_key = event::KeyEvent::new(event::KeyCode::Char('5'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(context_key);
    app.handle_app_event(app_event).unwrap();
    let key = event::KeyEvent::new(event::KeyCode::Char('q'), event::KeyModifiers::NONE);
    let app_event = AppEvent::Key(key);
    app.handle_app_event(app_event).unwrap();
    assert!(app.state().should_quit);
}

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

use std::sync::Arc;

use log::{error, info};
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::tui::{handlers::tab_navigation_handler, AppEvent};

use super::App;

pub fn normal_mode_handler(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => app.state.should_quit = true,
        tab @ (KeyCode::Char('1')
        | KeyCode::Char('2')
        | KeyCode::Char('3')
        | KeyCode::Char('4')
        | KeyCode::Char('5')) => tab_navigation_handler(app, tab),
        KeyCode::Char('c') => app.state.flightsql_tab.clear_editor(&app.state.config),
        KeyCode::Char('e') => {
            info!("Handling");
            let editor = app.state.flightsql_tab.editor();
            let lines = editor.lines();
            let content = lines.join("");
            info!("Conent: {}", content);
            let default = "Enter a query here.";
            if content == default {
                info!("Clearing default content");
                app.state.flightsql_tab.clear_placeholder();
            }
            app.state.flightsql_tab.edit();
        }
        KeyCode::Down => {
            if let Some(s) = app.state.flightsql_tab.query_results_state() {
                info!("Select next");
                let mut s = s.borrow_mut();
                s.select_next();
            }
        }
        KeyCode::Up => {
            if let Some(s) = app.state.flightsql_tab.query_results_state() {
                info!("Select previous");
                let mut s = s.borrow_mut();
                s.select_previous();
            }
        }

        KeyCode::Enter => {
            info!("Executing FlightSQL query");
            let sql = app.state.flightsql_tab.sql();
            info!("SQL: {}", sql);
            let sqls: Vec<String> = sql.split(';').map(|s| s.to_string()).collect();
            let execution = Arc::clone(&app.execution);
            let _event_tx = app.event_tx();
            let handle = tokio::spawn(execution.run_flightsqls(sqls, _event_tx));
            app.state.flightsql_tab.set_execution_task(handle);
        }
        KeyCode::Right => {
            let _event_tx = app.event_tx();
            if let Err(e) = _event_tx.send(AppEvent::FlightSQLExecutionResultsNextPage) {
                error!("Error going to next FlightSQL results page: {e}");
            }
        }
        KeyCode::Left => {
            let _event_tx = app.event_tx();
            if let Err(e) = _event_tx.send(AppEvent::FlightSQLExecutionResultsPreviousPage) {
                error!("Error going to previous FlightSQL results page: {e}");
            }
        }
        _ => {}
    }
}

pub fn editable_handler(app: &mut App, key: KeyEvent) {
    match (key.code, key.modifiers) {
        (KeyCode::Left, KeyModifiers::ALT) => app.state.flightsql_tab.previous_word(),
        (KeyCode::Right, KeyModifiers::ALT) => app.state.flightsql_tab.next_word(),
        (KeyCode::Backspace, KeyModifiers::ALT) => app.state.flightsql_tab.delete_word(),
        (KeyCode::Esc, _) => app.state.flightsql_tab.exit_edit(),
        _ => app.state.flightsql_tab.update_editor_content(key),
    }
}

pub fn app_event_handler(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::Key(key) => match app.state.flightsql_tab.editor_editable() {
            true => editable_handler(app, key),
            false => normal_mode_handler(app, key),
        },
        AppEvent::Error => {}
        _ => {}
    };
}

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

use log::info;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::App;
use crate::app::{handlers::tab_navigation_handler, AppEvent};

pub fn normal_mode_handler(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => app.state.should_quit = true,
        tab @ (KeyCode::Char('1')
        | KeyCode::Char('2')
        | KeyCode::Char('3')
        | KeyCode::Char('4')
        | KeyCode::Char('5')) => tab_navigation_handler(app, tab),
        KeyCode::Char('c') => app.state.sql_tab.clear_editor(),
        KeyCode::Char('e') => {
            let editor = app.state.sql_tab.editor();
            let lines = editor.lines();
            let content = lines.join("");
            let default = "Enter a query here.";
            if content == default {
                app.state.sql_tab.clear_placeholder();
            }
            app.state.sql_tab.edit();
        }
        KeyCode::Down => {
            if let Some(s) = app.state.sql_tab.query_results_state() {
                info!("Select next");
                let mut s = s.borrow_mut();
                s.select_next();
            }
        }
        KeyCode::Up => {
            if let Some(s) = app.state.sql_tab.query_results_state() {
                info!("Select previous");
                let mut s = s.borrow_mut();
                s.select_previous();
            }
        }

        KeyCode::Enter => {
            let sql = app.state.sql_tab.editor().lines().join("");
            info!("Running query: {}", sql);
            let _event_tx = app.event_tx().clone();
            let execution = Arc::clone(&app.execution);
            // TODO: Extract this into function to be used in both normal and editable handler.
            // Only useful if we get Ctrl / Cmd + Enter to work in editable mode though.
            tokio::spawn(async move {
                let sqls: Vec<&str> = sql.split(';').collect();
                let _ = execution.run_sqls(sqls, _event_tx).await;
            });
        }
        KeyCode::Right => {
            let _event_tx = app.event_tx().clone();
            // This won't work if you paginate the results, switch to FlightSQL tab, and then
            // switch back to SQL tab, and paginate again.
            //
            // Need to decide if switching tabs should reset pagination.
            if let Some(p) = app.state.history_tab.history().last() {
                let execution = Arc::clone(&app.execution);
                let sql = p.sql().clone();
                tokio::spawn(async move {
                    // TODO: Should be a call to `next_page` and `next_batch` is implementation
                    // detail.
                    execution.next_batch(sql, _event_tx).await;
                });
            }
        }
        KeyCode::Left => {
            app.state.sql_tab.previous_page();
            app.state.sql_tab.refresh_query_results_state();
        }
        _ => {}
    }
}

pub fn editable_handler(app: &mut App, key: KeyEvent) {
    match (key.code, key.modifiers) {
        (KeyCode::Left, KeyModifiers::ALT) => app.state.sql_tab.previous_word(),
        (KeyCode::Right, KeyModifiers::ALT) => app.state.sql_tab.next_word(),
        (KeyCode::Backspace, KeyModifiers::ALT) => app.state.sql_tab.delete_word(),
        (KeyCode::Esc, _) => app.state.sql_tab.exit_edit(),
        _ => app.state.sql_tab.update_editor_content(key),
    }
}

pub fn app_event_handler(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::Key(key) => match app.state.sql_tab.editor_editable() {
            true => editable_handler(app, key),
            false => normal_mode_handler(app, key),
        },
        AppEvent::Error => {}
        _ => {}
    };
}

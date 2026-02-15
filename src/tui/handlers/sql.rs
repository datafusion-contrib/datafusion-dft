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

use super::App;
use crate::tui::{handlers::tab_navigation_handler, state::tabs::sql::SQLTabMode, AppEvent};

pub fn normal_mode_handler(app: &mut App, key: KeyEvent) {
    match (key.code, key.modifiers) {
        (KeyCode::Char('q'), KeyModifiers::NONE) => app.state.should_quit = true,
        (
            tab @ (KeyCode::Char('1')
            | KeyCode::Char('2')
            | KeyCode::Char('3')
            | KeyCode::Char('4')
            | KeyCode::Char('5')),
            KeyModifiers::NONE,
        ) => tab_navigation_handler(app, tab),
        (KeyCode::Char('c'), KeyModifiers::NONE) => {
            app.state.sql_tab.clear_editor(&app.state.config)
        }
        (KeyCode::Char('e'), KeyModifiers::NONE) => {
            let editor = app.state.sql_tab.editor();
            let lines = editor.lines();
            let content = lines.join("");
            let default = "Enter a query here.";
            if content == default {
                app.state.sql_tab.clear_placeholder();
            }
            app.state.sql_tab.edit();
        }
        (KeyCode::Char('d'), KeyModifiers::NONE) => app.state.sql_tab.set_mode(SQLTabMode::DDL),
        (KeyCode::Char('n'), KeyModifiers::NONE) => app.state.sql_tab.set_mode(SQLTabMode::Normal),
        (KeyCode::Char('s'), KeyModifiers::NONE) => {
            if *app.state.sql_tab.mode() == SQLTabMode::DDL {
                let textarea = app.state.sql_tab.active_editor_cloned();
                let ddl = textarea.lines().join("\n");
                app.execution.save_ddl(ddl)
            }
        }
        (KeyCode::Down, KeyModifiers::NONE) => {
            if let Some(s) = app.state.sql_tab.query_results_state() {
                info!("Select next");
                let mut s = s.borrow_mut();
                s.select_next();
            }
        }
        (KeyCode::Up, KeyModifiers::NONE) => {
            if let Some(s) = app.state.sql_tab.query_results_state() {
                info!("Select previous");
                let mut s = s.borrow_mut();
                s.select_previous();
            }
        }

        (KeyCode::Enter, KeyModifiers::NONE) => match app.state.sql_tab.mode() {
            SQLTabMode::Normal => {
                let sql = app.state.sql_tab.sql();
                info!("Running query: {}", sql);
                let _event_tx = app.event_tx().clone();
                let execution = Arc::clone(&app.execution);
                let sqls: Vec<String> = sql.split(';').map(|s| s.to_string()).collect();
                let handle = tokio::spawn(execution.run_sqls(sqls, _event_tx));
                app.state.sql_tab.set_execution_task(handle);
            }
            SQLTabMode::DDL => {
                let _event_tx = app.event_tx().clone();
                // TODO: Probably want this to load from Editor instead of the file so that
                // it is the latest content.
                let ddl = app.execution.load_ddl().unwrap_or_default();
                if let Err(e) = _event_tx.send(AppEvent::ExecuteDDL(ddl)) {
                    error!("Error sending ExecuteDDL event: {:?}", e);
                }
            }
        },
        (KeyCode::Right, KeyModifiers::NONE) => {
            let _event_tx = app.event_tx();

            if let Some(current_page) = app.state.sql_tab.current_page() {
                let next_page = current_page + 1;

                // Check if we need more batches for the next page
                if app.state.sql_tab.needs_more_batches_for_page(next_page) {
                    info!("Fetching more batches for page {}", next_page);

                    if let Some(last_query) = app.state.history_tab.history().last() {
                        let execution = Arc::clone(&app.execution);
                        let sql = last_query.sql().clone();
                        tokio::spawn(async move {
                            execution.next_batch(sql, _event_tx).await;
                        });
                    }
                    return; // Wait for batch to load before advancing page
                }
            }

            // Sufficient data available, advance page
            if let Err(e) = app.event_tx().send(AppEvent::ExecutionResultsNextPage) {
                error!("Error going to next SQL results page: {e}");
            }
        }
        (KeyCode::Left, KeyModifiers::NONE) => {
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
        (KeyCode::Enter, KeyModifiers::ALT) => {
            match app.state.sql_tab.mode() {
                // TODO: Encapsulate this logic
                SQLTabMode::Normal => {
                    let sql = app.state.sql_tab.sql();
                    info!("Running query: {}", sql);
                    let _event_tx = app.event_tx().clone();
                    let execution = Arc::clone(&app.execution);
                    let sqls: Vec<String> = sql.split(';').map(|s| s.to_string()).collect();
                    let handle = tokio::spawn(execution.run_sqls(sqls, _event_tx));
                    app.state.sql_tab.set_execution_task(handle);
                }
                SQLTabMode::DDL => {
                    let _event_tx = app.event_tx().clone();
                    // TODO: Probably want this to load from Editor instead of the file so that
                    // it is the latest content.
                    let ddl = app.execution.load_ddl().unwrap_or_default();
                    if let Err(e) = _event_tx.send(AppEvent::ExecuteDDL(ddl)) {
                        error!("Error sending ExecuteDDL event: {:?}", e);
                    }
                }
            }
            // Exit edit mode after executing so user can scroll results
            app.state.sql_tab.exit_edit();
        }
        _ => app.state.sql_tab.update_editor_content(key),
    }
}

pub fn app_event_handler(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::Key(key) => match app.state.sql_tab.editable() {
            true => editable_handler(app, key),
            false => normal_mode_handler(app, key),
        },
        AppEvent::Error => {}
        _ => {}
    };
}

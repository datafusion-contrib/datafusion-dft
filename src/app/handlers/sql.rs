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

use std::{sync::Arc, time::Instant};

use datafusion::{arrow::array::RecordBatch, physical_plan::execute_stream};
use log::{error, info};
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use tokio_stream::StreamExt;

use crate::app::{
    execution::collect_plan_stats, handlers::tab_navigation_handler, state::tabs::sql::Query,
    AppEvent,
};

use super::App;

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
            info!("Run query");
            let sql = app.state.sql_tab.editor().lines().join("");
            info!("SQL: {}", sql);
            let execution = Arc::clone(&app.execution);
            let _event_tx = app.app_event_tx.clone();
            // TODO: Maybe this should be on a separate runtime to prevent blocking main thread /
            // runtime
            // TODO: Extract this into function to be used in both normal and editable handler
            tokio::spawn(async move {
                let sqls: Vec<&str> = sql.split(';').collect();
                let _ = execution.run_sqls(sqls, _event_tx).await;
            });
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
        (KeyCode::Enter, KeyModifiers::CONTROL) => {
            let query = app.state.sql_tab.editor().lines().join("");
            let ctx = app.execution.session_ctx.clone();
            let _event_tx = app.app_event_tx.clone();
            // TODO: Maybe this should be on a separate runtime to prevent blocking main thread /
            // runtime
            tokio::spawn(async move {
                // TODO: Turn this into a match and return the error somehow
                let start = Instant::now();
                if let Ok(df) = ctx.sql(&query).await {
                    let plan = df.create_physical_plan().await;
                    match plan {
                        Ok(p) => {
                            let task_ctx = ctx.task_ctx();
                            let stream = execute_stream(Arc::clone(&p), task_ctx);
                            let mut batches: Vec<RecordBatch> = Vec::new();
                            match stream {
                                Ok(mut s) => {
                                    while let Some(b) = s.next().await {
                                        match b {
                                            Ok(b) => batches.push(b),
                                            Err(e) => {
                                                error!("Error getting RecordBatch: {:?}", e)
                                            }
                                        }
                                    }

                                    let elapsed = start.elapsed();
                                    let stats = collect_plan_stats(p);
                                    info!("Got stats: {:?}", stats);
                                    let query =
                                        Query::new(query, Some(batches), None, None, elapsed, None);
                                    let _ = _event_tx.send(AppEvent::QueryResult(query));
                                }
                                Err(e) => {
                                    error!("Error creating RecordBatchStream: {:?}", e)
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error creating physical plan: {:?}", e)
                        }
                    }
                } else {
                    error!("Error creating dataframe")
                }
            });
        }
        _ => app.state.sql_tab.update_editor_content(key),
    }
}

pub fn app_event_handler(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::Key(key) => match app.state.sql_tab.editor_editable() {
            true => editable_handler(app, key),
            false => normal_mode_handler(app, key),
        },
        AppEvent::Tick => {}
        AppEvent::Error => {}
        _ => {}
    };
}

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
use std::time::{Duration, Instant};

use color_eyre::eyre::eyre;
use log::{error, info};
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::app::{state::tabs::sql::Query, AppEvent};

use super::App;

pub fn normal_mode_handler(app: &mut App, key: KeyEvent) {
    info!("FSNormal mode handler");
    match key.code {
        KeyCode::Char('c') => app.state.flightsql_tab.clear_editor(),
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
            info!("Run FS query");
            let sql = app.state.flightsql_tab.editor().lines().join("");
            info!("SQL: {}", sql);
            let mut query = Query::new(sql.clone(), None, None, None, Duration::default());
            let client = Arc::clone(&app.execution.flightsql_client);
            let _event_tx = app.app_event_tx.clone();
            // TODO: Maybe this should be on a separate runtime to prevent blocking main thread /
            // runtime
            tokio::spawn(async move {
                let start = std::time::Instant::now();
                if let Some(ref mut c) = *client.lock().unwrap() {
                    info!("Sending query");
                    let r = c.execute(sql, None);
                }

                // match ctx.sql(&sql).await {
                //     Ok(df) => match df.collect().await {
                //         Ok(res) => {
                //             let elapsed = start.elapsed();
                //             let rows: usize = res.iter().map(|r| r.num_rows()).sum();
                //             query.set_results(Some(res));
                //             query.set_num_rows(Some(rows));
                //             query.set_elapsed_time(elapsed);
                //         }
                //         Err(e) => {
                //             error!("Error collecting results: {:?}", e);
                //             let elapsed = start.elapsed();
                //             query.set_error(Some(e.to_string()));
                //             query.set_elapsed_time(elapsed);
                //         }
                //     },
                //     Err(e) => {
                //         error!("Error creating dataframe: {:?}", e);
                //         let elapsed = start.elapsed();
                //         query.set_error(Some(e.to_string()));
                //         query.set_elapsed_time(elapsed);
                //     }
                // }
                // let _ = _event_tx.send(AppEvent::QueryResult(query));
            });
        }
        _ => {}
    }
}

pub fn editable_handler(app: &mut App, key: KeyEvent) {
    info!("FSEditable handler: {:?}", key);
    match (key.code, key.modifiers) {
        (KeyCode::Esc, _) => app.state.flightsql_tab.exit_edit(),
        _ => app.state.flightsql_tab.update_editor_content(key),
    }
}

pub fn app_event_handler(app: &mut App, event: AppEvent) {
    info!("FSSQL Tab handling: {:?}", event);
    match event {
        AppEvent::Key(key) => match app.state.flightsql_tab.editor_editable() {
            true => editable_handler(app, key),
            false => normal_mode_handler(app, key),
        },
        AppEvent::FlightSQLQueryResult(r) => {
            info!("Query results: {:?}", r);
            app.state.flightsql_tab.set_query(r);
            app.state.flightsql_tab.refresh_query_results_state();
        }
        AppEvent::Tick => {}
        AppEvent::Error => {}
        _ => {}
    };
}

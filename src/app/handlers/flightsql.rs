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

use datafusion::arrow::array::RecordBatch;
use log::{error, info};
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use tokio_stream::StreamExt;
use tonic::IntoRequest;

use crate::app::state::tabs::flightsql::FlightSQLQuery;
use crate::app::{handlers::tab_navigation_handler, AppEvent};

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

        // KeyCode::Enter => {
        //     info!("Run FS query");
        //     let sql = app.state.flightsql_tab.editor().lines().join("");
        //     info!("SQL: {}", sql);
        //     let execution = Arc::clone(&app.execution);
        //     let _event_tx = app.event_tx();
        //     tokio::spawn(async move {
        //         let client = execution.flightsql_client();
        //         let mut query =
        //             FlightSQLQuery::new(sql.clone(), None, None, None, Duration::default(), None);
        //         let start = Instant::now();
        //         if let Some(ref mut c) = *client.lock().await {
        //             info!("Sending query");
        //             match c.execute(sql, None).await {
        //                 Ok(flight_info) => {
        //                     for endpoint in flight_info.endpoint {
        //                         if let Some(ticket) = endpoint.ticket {
        //                             match c.do_get(ticket.into_request()).await {
        //                                 Ok(mut stream) => {
        //                                     let mut batches: Vec<RecordBatch> = Vec::new();
        //                                     // temporarily only show the first batch to avoid
        //                                     // buffering massive result sets. Eventually there should
        //                                     // be some sort of paging logic
        //                                     // see https://github.com/datafusion-contrib/datafusion-tui/pull/133#discussion_r1756680874
        //                                     // while let Some(maybe_batch) = stream.next().await {
        //                                     if let Some(maybe_batch) = stream.next().await {
        //                                         match maybe_batch {
        //                                             Ok(batch) => {
        //                                                 info!("Batch rows: {}", batch.num_rows());
        //                                                 batches.push(batch);
        //                                             }
        //                                             Err(e) => {
        //                                                 error!("Error getting batch: {:?}", e);
        //                                                 let elapsed = start.elapsed();
        //                                                 query.set_error(Some(e.to_string()));
        //                                                 query.set_execution_time(elapsed);
        //                                             }
        //                                         }
        //                                     }
        //                                     let elapsed = start.elapsed();
        //                                     let rows: usize =
        //                                         batches.iter().map(|r| r.num_rows()).sum();
        //                                     query.set_results(Some(batches));
        //                                     query.set_num_rows(Some(rows));
        //                                     query.set_execution_time(elapsed);
        //                                 }
        //                                 Err(e) => {
        //                                     error!("Error getting response: {:?}", e);
        //                                     let elapsed = start.elapsed();
        //                                     query.set_error(Some(e.to_string()));
        //                                     query.set_execution_time(elapsed);
        //                                 }
        //                             }
        //                         }
        //                     }
        //                 }
        //                 Err(e) => {
        //                     error!("Error getting response: {:?}", e);
        //                     let elapsed = start.elapsed();
        //                     query.set_error(Some(e.to_string()));
        //                     query.set_execution_time(elapsed);
        //                 }
        //             }
        //         }
        //
        //         let _ = _event_tx.send(AppEvent::FlightSQLQueryResult(query));
        //     });
        // }
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
        AppEvent::FlightSQLQueryResult(r) => {
            info!("Query results: {:?}", r);
            app.state.flightsql_tab.set_query(r);
            app.state.flightsql_tab.refresh_query_results_state();
        }
        AppEvent::Error => {}
        _ => {}
    };
}

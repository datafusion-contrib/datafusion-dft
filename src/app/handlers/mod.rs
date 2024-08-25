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

use std::time::{Duration, Instant};

use color_eyre::{eyre::eyre, Result};
use log::{debug, error, info, trace};
use ratatui::crossterm::event::{self, KeyCode, KeyEvent, KeyModifiers};
use tui_logger::TuiWidgetEvent;

use crate::{
    app::{state::tabs::explore::Query, AppEvent},
    ui::SelectedTab,
};

use super::App;

pub fn crossterm_event_handler(event: event::Event) -> Option<AppEvent> {
    debug!("crossterm::event: {:?}", event);
    match event {
        event::Event::Key(key) => {
            if key.kind == event::KeyEventKind::Press {
                Some(AppEvent::Key(key))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn tab_navigation_handler(app: &mut App, key: KeyCode) {
    match key {
        KeyCode::Char('s') => app.state.tabs.selected = SelectedTab::Queries,
        KeyCode::Char('l') => app.state.tabs.selected = SelectedTab::Logs,
        _ => {}
    };
}

fn explore_tab_normal_mode_handler(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => app.state.should_quit = true,
        KeyCode::Char('c') => app.state.explore_tab.clear_editor(),
        KeyCode::Char('e') => {
            let editor = app.state.explore_tab.editor();
            let lines = editor.lines();
            let content = lines.join("");
            info!("Conent: {}", content);
            let default = "Enter a query here.";
            if content == default {
                info!("Clearing default content");
                app.state.explore_tab.clear_placeholder();
            }
            app.state.explore_tab.edit();
        }
        KeyCode::Down => {
            if let Some(s) = app.state.explore_tab.query_results_state() {
                info!("Select next");
                let mut s = s.borrow_mut();
                s.select_next();
            }
        }
        KeyCode::Up => {
            if let Some(s) = app.state.explore_tab.query_results_state() {
                info!("Select previous");
                let mut s = s.borrow_mut();
                s.select_previous();
            }
        }

        tab @ (KeyCode::Char('s') | KeyCode::Char('l')) => tab_navigation_handler(app, tab),
        KeyCode::Enter => {
            info!("Run query");
            let sql = app.state.explore_tab.editor().lines().join("");
            info!("SQL: {}", sql);
            let mut query = Query::new(sql.clone(), None, None, None, Duration::default());
            let ctx = app.execution.session_ctx.clone();
            let _event_tx = app.app_event_tx.clone();
            // TODO: Maybe this should be on a separate runtime to prevent blocking main thread /
            // runtime
            tokio::spawn(async move {
                let start = std::time::Instant::now();
                match ctx.sql(&sql).await {
                    Ok(df) => match df.collect().await {
                        Ok(res) => {
                            let elapsed = start.elapsed();
                            let rows: usize = res.iter().map(|r| r.num_rows()).sum();
                            query.set_results(Some(res));
                            query.set_num_rows(Some(rows));
                            query.set_elapsed_time(elapsed);
                            // let query = Query::new(sql, Some(res), Some(rows), None, elapsed);
                            // let _ = _event_tx.send(AppEvent::ExploreQueryResult(res));
                        }
                        Err(e) => {
                            error!("Error collecting results: {:?}", e);
                            let elapsed = start.elapsed();
                            query.set_error(Some(e.to_string()));
                            query.set_elapsed_time(elapsed);
                            // let _ = _event_tx.send(AppEvent::ExploreQueryError(e.to_string()));
                        }
                    },
                    Err(e) => {
                        error!("Error creating dataframe: {:?}", e);
                        let elapsed = start.elapsed();
                        query.set_error(Some(e.to_string()));
                        query.set_elapsed_time(elapsed);
                        // let _ = _event_tx.send(AppEvent::ExploreQueryError(e.to_string()));
                    }
                }
                let _ = _event_tx.send(AppEvent::ExploreQueryResult(query));
                // if let Ok(df) = ctx.sql(&query).await {
                //     if let Ok(res) = df.collect().await.map_err(|e| eyre!(e)) {
                //         info!("Results: {:?}", res);
                //         let _ = _event_tx.send(AppEvent::ExploreQueryResult(res));
                //     }
                // } else {
                //     error!("Error creating dataframe")
                // }
            });
        }
        _ => {}
    }
}

fn explore_tab_editable_handler(app: &mut App, key: KeyEvent) {
    info!("KeyEvent: {:?}", key);
    match (key.code, key.modifiers) {
        (KeyCode::Esc, _) => app.state.explore_tab.exit_edit(),
        (KeyCode::Enter, KeyModifiers::CONTROL) => {
            let query = app.state.explore_tab.editor().lines().join("");
            let ctx = app.execution.session_ctx.clone();
            let _event_tx = app.app_event_tx.clone();
            // TODO: Maybe this should be on a separate runtime to prevent blocking main thread /
            // runtime
            tokio::spawn(async move {
                // TODO: Turn this into a match and return the error somehow
                let start = Instant::now();
                if let Ok(df) = ctx.sql(&query).await {
                    if let Ok(res) = df.collect().await.map_err(|e| eyre!(e)) {
                        info!("Results: {:?}", res);
                        let elapsed = start.elapsed();
                        let query = Query::new(query, Some(res), None, None, elapsed);
                        let _ = _event_tx.send(AppEvent::ExploreQueryResult(query));
                    }
                } else {
                    error!("Error creating dataframe")
                }
            });
        }
        _ => app.state.explore_tab.update_editor_content(key),
    }
}

fn explore_tab_app_event_handler(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::Key(key) => match app.state.explore_tab.editor_editable() {
            true => explore_tab_editable_handler(app, key),
            false => explore_tab_normal_mode_handler(app, key),
        },
        AppEvent::ExploreQueryResult(r) => {
            info!("Query results: {:?}", r);
            app.state.explore_tab.set_query(r);
            app.state.explore_tab.refresh_query_results_state();
        }
        AppEvent::Tick => {}
        AppEvent::Error => {}
        _ => {}
    };
}

fn logs_tab_key_event_handler(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('h') => {
            app.state.logs_tab.transition(TuiWidgetEvent::HideKey);
        }
        KeyCode::Char('f') => {
            app.state.logs_tab.transition(TuiWidgetEvent::FocusKey);
        }
        KeyCode::Char('+') => {
            app.state.logs_tab.transition(TuiWidgetEvent::PlusKey);
        }
        KeyCode::Char('-') => {
            app.state.logs_tab.transition(TuiWidgetEvent::MinusKey);
        }
        KeyCode::Char('q') => app.state.should_quit = true,
        KeyCode::Char(' ') => {
            app.state.logs_tab.transition(TuiWidgetEvent::SpaceKey);
        }
        KeyCode::Esc => {
            app.state.logs_tab.transition(TuiWidgetEvent::EscapeKey);
        }
        KeyCode::Down => {
            app.state.logs_tab.transition(TuiWidgetEvent::DownKey);
        }
        KeyCode::Up => {
            app.state.logs_tab.transition(TuiWidgetEvent::UpKey);
        }
        KeyCode::Right => {
            app.state.logs_tab.transition(TuiWidgetEvent::RightKey);
        }
        KeyCode::Left => {
            app.state.logs_tab.transition(TuiWidgetEvent::LeftKey);
        }
        KeyCode::PageDown => {
            app.state.logs_tab.transition(TuiWidgetEvent::NextPageKey);
        }

        KeyCode::PageUp => {
            app.state.logs_tab.transition(TuiWidgetEvent::PrevPageKey);
        }
        KeyCode::Char('e') => {
            app.state.tabs.selected = SelectedTab::Queries;
        }
        _ => {}
    }
}

fn logs_tab_app_event_handler(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::Key(key) => logs_tab_key_event_handler(app, key),
        AppEvent::ExploreQueryResult(r) => {
            app.state.explore_tab.set_query(r);
            app.state.explore_tab.refresh_query_results_state();
        }
        AppEvent::Tick => {}
        AppEvent::Error => {}
        _ => {}
    };
}

pub fn app_event_handler(app: &mut App, event: AppEvent) -> Result<()> {
    let now = std::time::Instant::now();
    trace!("Tui::Event: {:?}", event);
    if let AppEvent::ExecuteDDL(ddl) = event {
        let queries: Vec<String> = ddl.split(";").map(|s| s.to_string()).collect();
        queries.into_iter().for_each(|q| {
            let ctx = app.execution.session_ctx.clone();
            tokio::spawn(async move {
                if let Ok(df) = ctx.sql(&q).await {
                    if df.collect().await.is_ok() {
                        info!("Successful DDL");
                    }
                }
            });
        })
    } else {
        match app.state.tabs.selected {
            SelectedTab::Queries => explore_tab_app_event_handler(app, event),
            SelectedTab::Logs => logs_tab_app_event_handler(app, event),
        };
    }
    trace!("Event handling took: {:?}", now.elapsed());
    Ok(())
}

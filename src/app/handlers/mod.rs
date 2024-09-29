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
#[cfg(feature = "flightsql")]
pub mod flightsql;
pub mod history;
pub mod sql;

use color_eyre::Result;
use log::{error, info, trace};
use ratatui::crossterm::event::{self, KeyCode, KeyEvent};
use tui_logger::TuiWidgetEvent;

use crate::app::state::tabs::history::Context;
use crate::app::ExecutionResultsBatch;

#[cfg(feature = "flightsql")]
use std::sync::Arc;

use super::App;
use crate::app::ui::SelectedTab;
use crate::app::{state::tabs::history::HistoryQuery, AppEvent};

pub fn crossterm_event_handler(event: event::Event) -> Option<AppEvent> {
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
    #[cfg(feature = "flightsql")]
    match key {
        KeyCode::Char('1') => app.state.tabs.selected = SelectedTab::SQL,
        KeyCode::Char('2') => app.state.tabs.selected = SelectedTab::FlightSQL,
        KeyCode::Char('3') => app.state.tabs.selected = SelectedTab::History,
        KeyCode::Char('4') => app.state.tabs.selected = SelectedTab::Logs,
        KeyCode::Char('5') => app.state.tabs.selected = SelectedTab::Context,
        _ => {}
    };
    #[cfg(not(feature = "flightsql"))]
    match key {
        KeyCode::Char('1') => app.state.tabs.selected = SelectedTab::SQL,
        KeyCode::Char('2') => app.state.tabs.selected = SelectedTab::History,
        KeyCode::Char('3') => app.state.tabs.selected = SelectedTab::Logs,
        KeyCode::Char('4') => app.state.tabs.selected = SelectedTab::Context,
        _ => {}
    };
}

fn logs_tab_key_event_handler(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => app.state.should_quit = true,
        tab @ (KeyCode::Char('1')
        | KeyCode::Char('2')
        | KeyCode::Char('3')
        | KeyCode::Char('4')
        | KeyCode::Char('5')) => tab_navigation_handler(app, tab),
        KeyCode::Char('f') => {
            app.state.logs_tab.transition(TuiWidgetEvent::FocusKey);
        }
        KeyCode::Char('h') => {
            app.state.logs_tab.transition(TuiWidgetEvent::HideKey);
        }
        KeyCode::Char('+') => {
            app.state.logs_tab.transition(TuiWidgetEvent::PlusKey);
        }
        KeyCode::Char('-') => {
            app.state.logs_tab.transition(TuiWidgetEvent::MinusKey);
        }
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
        _ => {}
    }
}

fn context_tab_key_event_handler(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => app.state.should_quit = true,
        tab @ (KeyCode::Char('1')
        | KeyCode::Char('2')
        | KeyCode::Char('3')
        | KeyCode::Char('4')
        | KeyCode::Char('5')) => tab_navigation_handler(app, tab),
        _ => {}
    }
}

fn logs_tab_app_event_handler(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::Key(key) => logs_tab_key_event_handler(app, key),
        AppEvent::Error => {}
        _ => {}
    };
}

fn context_tab_app_event_handler(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::Key(key) => context_tab_key_event_handler(app, key),
        AppEvent::Error => {}
        _ => {}
    };
}

pub fn app_event_handler(app: &mut App, event: AppEvent) -> Result<()> {
    trace!("Tui::Event: {:?}", event);
    let now = std::time::Instant::now();
    match event {
        AppEvent::ExecuteDDL(ddl) => {
            let queries: Vec<String> = ddl
                .split(';')
                .filter_map(|s| {
                    if s.is_empty() || s == "\n" {
                        return None;
                    }
                    Some(s.to_string())
                })
                .collect();
            let ctx = app.execution.session_ctx().clone();
            let handle = tokio::spawn(async move {
                for q in queries {
                    info!("Executing DDL: {:?}", q);
                    match ctx.sql(&q).await {
                        Ok(df) => {
                            if df.collect().await.is_ok() {
                                info!("Successful DDL");
                            }
                        }
                        Err(e) => {
                            error!("Error executing DDL {:?}: {:?}", q, e);
                        }
                    }
                }
            });
            app.ddl_task = Some(handle);
        }
        AppEvent::NewExecution => {
            app.state.sql_tab.reset_execution_results();
        }
        AppEvent::ExecutionResultsError(e) => {
            app.state.sql_tab.set_execution_error(e.clone());
            let history_query = HistoryQuery::new(
                Context::Local,
                e.query().to_string(),
                *e.duration(),
                None,
                Some(e.error().to_string()),
            );
            info!("Adding to history: {:?}", history_query);
            app.state.history_tab.add_to_history(history_query);
            app.state.history_tab.refresh_history_table_state();
        }
        AppEvent::ExecutionResultsNextPage(r) => {
            let ExecutionResultsBatch {
                query,
                duration,
                batch,
            } = r;
            app.state.sql_tab.add_batch(batch);
            app.state.sql_tab.next_page();
            app.state.sql_tab.refresh_query_results_state();
            let history_query =
                HistoryQuery::new(Context::Local, query.to_string(), duration, None, None);
            app.state.history_tab.add_to_history(history_query);
            app.state.history_tab.refresh_history_table_state();
        }
        #[cfg(feature = "flightsql")]
        AppEvent::FlightSQLQueryResult(r) => {
            app.state.flightsql_tab.set_query(r.clone());
            app.state.flightsql_tab.refresh_query_results_state();
            let history_query = HistoryQuery::new(
                Context::FlightSQL,
                r.sql().clone(),
                *r.execution_time(),
                r.execution_stats().clone(),
                None,
            );
            app.state.history_tab.add_to_history(history_query);
            app.state.history_tab.refresh_history_table_state()
        }
        #[cfg(feature = "flightsql")]
        AppEvent::EstablishFlightSQLConnection => {
            let execution = Arc::clone(&app.execution);
            let flightsql_config = app.state.config.flightsql.clone();
            tokio::spawn(async move {
                if let Err(e) = execution.create_flightsql_client(flightsql_config).await {
                    error!("Error creating FlightSQL client: {:?}", e);
                } else {
                    info!("Created FlightSQL client");
                }
            });
        }
        _ => {
            match app.state.tabs.selected {
                SelectedTab::SQL => sql::app_event_handler(app, event),
                SelectedTab::Logs => logs_tab_app_event_handler(app, event),
                SelectedTab::Context => context_tab_app_event_handler(app, event),
                SelectedTab::History => history::app_event_handler(app, event),
                #[cfg(feature = "flightsql")]
                SelectedTab::FlightSQL => flightsql::app_event_handler(app, event),
            };
        }
    }
    trace!("Event handling took: {:?}", now.elapsed());
    Ok(())
}

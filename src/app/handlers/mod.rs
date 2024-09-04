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
use log::{debug, error, info, trace};
use ratatui::crossterm::event::{self, KeyCode, KeyEvent};
use tui_logger::TuiWidgetEvent;

use crate::state::tabs::history::Context;

#[cfg(feature = "flightsql")]
use arrow_flight::sql::client::FlightSqlServiceClient;
#[cfg(feature = "flightsql")]
use std::sync::Arc;
#[cfg(feature = "flightsql")]
use tonic::transport::Channel;

use crate::{
    app::{state::tabs::history::HistoryQuery, AppEvent},
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
        AppEvent::Tick => {}
        AppEvent::Error => {}
        _ => {}
    };
}

fn context_tab_app_event_handler(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::Key(key) => context_tab_key_event_handler(app, key),
        AppEvent::Tick => {}
        AppEvent::Error => {}
        _ => {}
    };
}

pub fn app_event_handler(app: &mut App, event: AppEvent) -> Result<()> {
    // TODO: AppEvent::QueryResult can probably be handled here rather than duplicating in
    // each tab
    trace!("Tui::Event: {:?}", event);
    let now = std::time::Instant::now();
    match event {
        AppEvent::ExecuteDDL(ddl) => {
            let queries: Vec<String> = ddl.split(';').map(|s| s.to_string()).collect();
            queries.into_iter().for_each(|q| {
                let ctx = app.execution.session_ctx.clone();
                tokio::spawn(async move {
                    match ctx.sql(&q).await {
                        Ok(df) => {
                            if df.collect().await.is_ok() {
                                info!("Successful DDL");
                            }
                        }
                        Err(e) => {
                            error!("Error executing DDL: {:?}", e);
                        }
                    }
                });
            })
        }
        AppEvent::QueryResult(r) => {
            app.state.sql_tab.set_query(r.clone());
            app.state.sql_tab.refresh_query_results_state();
            let history_query = HistoryQuery::new(
                Context::Local,
                r.sql().clone(),
                *r.execution_time(),
                r.execution_stats().clone(),
            );
            app.state.history_tab.add_to_history(history_query);
            app.state.history_tab.refresh_history_table_state()
        }
        #[cfg(feature = "flightsql")]
        AppEvent::FlightSQLQueryResult(r) => {
            app.state.flightsql_tab.set_query(r.clone());
            app.state.flightsql_tab.refresh_query_results_state();
            let history_query =
                HistoryQuery::new(Context::FlightSQL, r.sql().clone(), *r.execution_time());
            app.state.history_tab.add_to_history(history_query);
            app.state.history_tab.refresh_history_table_state()
        }
        #[cfg(feature = "flightsql")]
        AppEvent::EstablishFlightSQLConnection => {
            let url = app.state.config.flightsql.connection_url.clone();
            info!("Connection to FlightSQL host: {}", url);
            let url: &'static str = Box::leak(url.into_boxed_str());
            let client = Arc::clone(&app.execution.flightsql_client);
            tokio::spawn(async move {
                let maybe_channel = Channel::from_static(&url).connect().await;
                info!("Created channel");
                match maybe_channel {
                    Ok(channel) => {
                        let flightsql_client = FlightSqlServiceClient::new(channel);
                        let mut locked_client = client.lock().await;
                        *locked_client = Some(flightsql_client);
                    }
                    Err(e) => {
                        info!("Error creating channel for FlightSQL: {:?}", e);
                    }
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

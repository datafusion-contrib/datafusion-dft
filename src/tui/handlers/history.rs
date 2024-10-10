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

use ratatui::crossterm::event::KeyCode;

use crate::tui::{handlers::tab_navigation_handler, AppEvent};

use super::App;

pub fn app_event_handler(app: &mut App, event: AppEvent) {
    match event {
        AppEvent::Key(key) => match key.code {
            KeyCode::Char('q') => app.state.should_quit = true,
            tab @ (KeyCode::Char('1')
            | KeyCode::Char('2')
            | KeyCode::Char('3')
            | KeyCode::Char('4')
            | KeyCode::Char('5')) => tab_navigation_handler(app, tab),

            KeyCode::Down => {
                if let Some(s) = app.state.history_tab.history_table_state() {
                    let mut s = s.borrow_mut();
                    s.select_next();
                }
            }
            KeyCode::Up => {
                if let Some(s) = app.state.history_tab.history_table_state() {
                    let mut s = s.borrow_mut();
                    s.select_previous();
                }
            }
            _ => {}
        },
        AppEvent::Error => {}
        _ => {}
    };
}

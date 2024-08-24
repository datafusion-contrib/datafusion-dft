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

use crate::app::core::{App, AppReturn, TabItem};
use crate::app::error::Result;
use log::debug;
use ratatui::crossterm::event::{KeyCode, KeyEvent};
use tui_logger::TuiWidgetEvent;

pub async fn logging_handler<'app>(app: &'app mut App<'app>, key: KeyEvent) -> Result<AppReturn> {
    match app.tab_item {
        TabItem::Logs => match key.code {
            KeyCode::Char('h') => {
                app.logs.state.transition(TuiWidgetEvent::HideKey);
                Ok(AppReturn::Continue)
            }
            KeyCode::Char('f') => {
                app.logs.state.transition(TuiWidgetEvent::FocusKey);
                Ok(AppReturn::Continue)
            }
            KeyCode::Char('+') => {
                app.logs.state.transition(TuiWidgetEvent::PlusKey);
                Ok(AppReturn::Continue)
            }
            KeyCode::Char('-') => {
                app.logs.state.transition(TuiWidgetEvent::MinusKey);
                Ok(AppReturn::Continue)
            }
            KeyCode::Char('q') => Ok(AppReturn::Exit),
            KeyCode::Char(' ') => {
                app.logs.state.transition(TuiWidgetEvent::SpaceKey);
                Ok(AppReturn::Continue)
            }
            KeyCode::Esc => {
                app.logs.state.transition(TuiWidgetEvent::EscapeKey);
                Ok(AppReturn::Continue)
            }
            KeyCode::Down => {
                app.logs.state.transition(TuiWidgetEvent::DownKey);
                Ok(AppReturn::Continue)
            }
            KeyCode::Up => {
                app.logs.state.transition(TuiWidgetEvent::UpKey);
                Ok(AppReturn::Continue)
            }
            KeyCode::Right => {
                app.logs.state.transition(TuiWidgetEvent::RightKey);
                Ok(AppReturn::Continue)
            }
            KeyCode::Left => {
                app.logs.state.transition(TuiWidgetEvent::LeftKey);
                Ok(AppReturn::Continue)
            }
            KeyCode::PageDown => {
                app.logs.state.transition(TuiWidgetEvent::NextPageKey);
                Ok(AppReturn::Continue)
            }

            KeyCode::PageUp => {
                app.logs.state.transition(TuiWidgetEvent::PrevPageKey);
                Ok(AppReturn::Continue)
            }
            KeyCode::Char(c) if c.is_ascii_digit() => {
                change_tab(c, app)?;
                Ok(AppReturn::Continue)
            }
            _ => Ok(AppReturn::Continue),
        },
        _ => Ok(AppReturn::Continue),
    }
}

fn change_tab(c: char, app: &mut App) -> Result<AppReturn> {
    match TabItem::try_from(c) {
        Ok(tab_item) => {
            app.tab_item = tab_item;
        }
        Err(e) => {
            debug!("{}", e);
        }
    };
    Ok(AppReturn::Continue)
}

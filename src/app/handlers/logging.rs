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
use crate::events::Key;
use log::debug;
use tui_logger::TuiWidgetEvent;

pub async fn logging_handler(app: &mut App, key: Key) -> Result<AppReturn> {
    match app.tab_item {
        TabItem::Logs => match key {
            Key::Char('h') => {
                app.logs.state.transition(&TuiWidgetEvent::HideKey);
                Ok(AppReturn::Continue)
            }
            Key::Char('f') => {
                app.logs.state.transition(&TuiWidgetEvent::FocusKey);
                Ok(AppReturn::Continue)
            }
            Key::Char('+') => {
                app.logs.state.transition(&TuiWidgetEvent::PlusKey);
                Ok(AppReturn::Continue)
            }
            Key::Char('-') => {
                app.logs.state.transition(&TuiWidgetEvent::MinusKey);
                Ok(AppReturn::Continue)
            }
            Key::Char('q') => Ok(AppReturn::Exit),
            Key::Char(' ') => {
                app.logs.state.transition(&TuiWidgetEvent::SpaceKey);
                Ok(AppReturn::Continue)
            }
            Key::Esc => {
                app.logs.state.transition(&TuiWidgetEvent::EscapeKey);
                Ok(AppReturn::Continue)
            }
            Key::Down => {
                app.logs.state.transition(&TuiWidgetEvent::DownKey);
                Ok(AppReturn::Continue)
            }
            Key::Up => {
                app.logs.state.transition(&TuiWidgetEvent::UpKey);
                Ok(AppReturn::Continue)
            }
            Key::Right => {
                app.logs.state.transition(&TuiWidgetEvent::RightKey);
                Ok(AppReturn::Continue)
            }
            Key::Left => {
                app.logs.state.transition(&TuiWidgetEvent::LeftKey);
                Ok(AppReturn::Continue)
            }
            Key::PageDown => {
                app.logs.state.transition(&TuiWidgetEvent::NextPageKey);
                Ok(AppReturn::Continue)
            }

            Key::PageUp => {
                app.logs.state.transition(&TuiWidgetEvent::PrevPageKey);
                Ok(AppReturn::Continue)
            }
            Key::Char(c) if c.is_ascii_digit() => {
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

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

use crate::app::core::{App, AppReturn, InputMode, TabItem};
use crate::app::error::Result;
use crate::app::handlers::execute_query;
use crate::app::handlers::logging;
use log::debug;
use ratatui::crossterm::event::{KeyCode, KeyEvent};

pub async fn normal_mode_handler<'app>(
    app: &'app mut App<'app>,
    key: KeyEvent,
) -> Result<AppReturn> {
    match app.tab_item {
        TabItem::Editor => match key.code {
            KeyCode::Enter => execute_query(app).await,
            KeyCode::Char('c') => {
                // app.editor.input.clear()?;
                app.input_mode = InputMode::Editing;
                Ok(AppReturn::Continue)
            }
            KeyCode::Char('e') => {
                app.input_mode = InputMode::Editing;
                Ok(AppReturn::Continue)
            }
            KeyCode::Char('q') => Ok(AppReturn::Exit),
            KeyCode::Char('r') => {
                app.input_mode = InputMode::Rc;
                Ok(AppReturn::Continue)
            }
            KeyCode::Char(c) => {
                if c.is_ascii_digit() {
                    change_tab(c, app)
                } else {
                    Ok(AppReturn::Continue)
                }
            }
            KeyCode::Down => {
                if let Some(results) = &mut app.query_results {
                    results.scroll.x += 1
                }
                Ok(AppReturn::Continue)
            }
            KeyCode::PageDown => {
                if let Some(results) = &mut app.query_results {
                    results.scroll.x += 10
                }
                Ok(AppReturn::Continue)
            }
            KeyCode::Up => {
                if let Some(results) = &mut app.query_results {
                    let new_x = match results.scroll.x {
                        0 => 0,
                        n => n - 1,
                    };
                    results.scroll.x = new_x
                }
                Ok(AppReturn::Continue)
            }
            KeyCode::PageUp => {
                if let Some(results) = &mut app.query_results {
                    let new_x = match results.scroll.x {
                        0 => 0,
                        n => n - 10,
                    };
                    results.scroll.x = new_x
                }
                Ok(AppReturn::Continue)
            }

            KeyCode::Right => {
                if let Some(results) = &mut app.query_results {
                    results.scroll.y += 3
                }
                Ok(AppReturn::Continue)
            }
            KeyCode::Left => {
                if let Some(results) = &mut app.query_results {
                    let new_y = match results.scroll.y {
                        0..=2 => 0,
                        n => n - 3,
                    };
                    results.scroll.y = new_y
                }
                Ok(AppReturn::Continue)
            }
            _ => Ok(AppReturn::Continue),
        },
        TabItem::Logs => logging::logging_handler(app, key).await,
        _ => match key.code {
            KeyCode::Char('q') => Ok(AppReturn::Exit),
            KeyCode::Char(c) if c.is_ascii_digit() => change_tab(c, app),
            _ => Ok(AppReturn::Continue),
        },
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

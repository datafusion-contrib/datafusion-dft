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

use crate::app::core::{App, AppReturn, InputMode};
use crate::app::error::Result;
use crate::events::Key;
use log::debug;

pub enum NormalModeAction {
    Continue,
    Exit,
}

pub async fn normal_mode_handler(app: &mut App, key: Key) -> Result<AppReturn> {
    if app.tabs.index == 0 {
        match key {
            Key::Char('c') => {
                app.editor.input.clear()?;
                app.input_mode = InputMode::Editing;
                Ok(AppReturn::Continue)
            }
            Key::Char('e') => {
                app.input_mode = InputMode::Editing;
                Ok(AppReturn::Continue)
            }
            Key::Char('r') => {
                app.reload_rc().await;
                Ok(AppReturn::Continue)
            }
            Key::Char('q') => Ok(AppReturn::Exit),
            Key::Char('r') => {
                app.input_mode = InputMode::Rc;
                Ok(AppReturn::Continue)
            }
            Key::Char(c) => {
                if c.is_ascii_digit() {
                    change_tab(c, app)
                } else {
                    Ok(AppReturn::Continue)
                }
            }
            Key::Down => {
                if let Some(results) = &mut app.query_results {
                    results.scroll.x += 1
                }
                Ok(AppReturn::Continue)
            }
            Key::Up => {
                if let Some(results) = &mut app.query_results {
                    let new_x = match results.scroll.x {
                        0 => 0,
                        n => n - 1,
                    };
                    results.scroll.x = new_x
                }
                Ok(AppReturn::Continue)
            }
            Key::Right => {
                if let Some(results) = &mut app.query_results {
                    results.scroll.y += 3
                }
                Ok(AppReturn::Continue)
            }
            Key::Left => {
                if let Some(results) = &mut app.query_results {
                    let new_y = match results.scroll.y {
                        0 | 1 | 2 => 0,
                        n => n - 3,
                    };
                    results.scroll.y = new_y
                }
                Ok(AppReturn::Continue)
            }
            _ => Ok(AppReturn::Continue),
        }
    } else {
        match key {
            Key::Char('q') => Ok(AppReturn::Exit),
            Key::Char(c @ ('0'..='9')) => change_tab(c, app),
            _ => Ok(AppReturn::Continue),
        }
    }
}

fn change_tab(c: char, app: &mut App) -> Result<AppReturn> {
    // Already checked that this is a digit, safe to unwrap
    let input_idx = c.to_digit(10).unwrap() as usize;
    if 0 < input_idx && input_idx <= app.tabs.titles.len() {
        app.tabs.index = input_idx - 1
    } else {
        debug!(
            "Invalid tab index: {}, valid range is [1..={}]",
            input_idx,
            app.tabs.titles.len()
        );
    };
    Ok(AppReturn::Continue)
}

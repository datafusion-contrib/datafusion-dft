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

use std::io;

use crate::app::{App, AppReturn, InputMode};
use crate::events::Key;

pub enum NormalModeAction {
    Continue,
    Exit,
}

pub fn normal_mode_handler(app: &mut App, key: Key) -> io::Result<AppReturn> {
    let result = match key {
        Key::Char('c') => {
            app.editor.input.clear();
            app.input_mode = InputMode::Editing;
            Ok(AppReturn::Continue)
        }
        Key::Char('e') => {
            app.input_mode = InputMode::Editing;
            Ok(AppReturn::Continue)
        }
        Key::Char('q') => Ok(AppReturn::Exit),
        Key::Char(c) => {
            if c.is_ascii_digit() {
                let input_idx = c.to_digit(10).unwrap() as usize;
                if input_idx < app.tabs.titles.len() {
                    app.tabs.index = input_idx
                } else {
                };
                Ok(AppReturn::Continue)
            } else {
                Ok(AppReturn::Continue)
            }
        }
        Key::Down => {
            match app.query_results {
                Some(ref mut results) => results.scroll.x += 1,
                None => {}
            };
            Ok(AppReturn::Continue)
        }
        Key::Up => {
            match app.query_results {
                Some(ref mut results) => {
                    let new_x = match results.scroll.x {
                        0 => 0,
                        n => n - 1,
                    };
                    results.scroll.x = new_x
                }
                None => {}
            };
            Ok(AppReturn::Continue)
        }
        Key::Right => {
            match app.query_results {
                Some(ref mut results) => results.scroll.y += 3,
                None => {}
            };
            Ok(AppReturn::Continue)
        }
        Key::Left => {
            match app.query_results {
                Some(ref mut results) => {
                    let new_y = match results.scroll.y {
                        0 | 1 | 2 => 0,
                        n => n - 3,
                    };
                    results.scroll.y = new_y
                }
                None => {}
            };
            Ok(AppReturn::Continue)
        }
        _ => Ok(AppReturn::Continue),
    };
    result
}

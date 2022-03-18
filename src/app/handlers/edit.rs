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

use log::debug;
use std::io;
use std::time::Instant;

use crate::app::datafusion::context::{QueryResults, QueryResultsMeta};
use crate::app::ui::Scroll;
use crate::app::{App, AppReturn, InputMode};
use crate::events::Key;

pub async fn edit_mode_handler(app: &mut App, key: Key) -> io::Result<AppReturn> {
    debug!(
        "{} Entered, current row / col: {} / {}",
        key, app.editor.input.cursor_row, app.editor.input.cursor_column
    );
    match key {
        Key::Enter => enter_handler(app).await,
        Key::Char(c) => match c {
            ';' => {
                app.editor.input.append_char(c);
                app.editor.sql_terminated = true;
            }
            _ => {
                app.editor.input.append_char(c);
            }
        },
        Key::Left => app.editor.input.previous_char(),
        Key::Right => app.editor.input.next_char(),
        Key::Up => app.editor.input.up_row(),
        Key::Down => app.editor.input.down_row(),
        Key::Tab => app.editor.input.tab(),
        Key::Backspace => {
            app.editor.input.backspace();
        }
        Key::Esc => {
            app.input_mode = InputMode::Normal;
        }
        _ => {}
    };
    Ok(AppReturn::Continue)
}

async fn enter_handler(app: &mut App) {
    match app.editor.sql_terminated {
        false => {
            app.editor.input.append_char('\n');
        }
        true => {
            let sql: String = app.editor.input.combine_lines();
            app.editor.sql_terminated = false;

            let now = Instant::now();
            let df = app.context.sql(&sql).await;
            match df {
                Ok(df) => {
                    let batches = df.collect().await.unwrap();
                    let query_duration = now.elapsed().as_secs_f64();
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    let query_meta = QueryResultsMeta {
                        query: sql,
                        succeeded: true,
                        error: None,
                        rows,
                        query_duration,
                    };
                    app.editor.history.push(query_meta.clone());
                    app.query_results = Some(QueryResults {
                        batches,
                        meta: query_meta,
                        scroll: Scroll { x: 0, y: 0 },
                    });
                }
                Err(e) => {
                    let err_msg = format!("{}", e);
                    let query_meta = QueryResultsMeta {
                        query: sql,
                        succeeded: false,
                        error: Some(err_msg),
                        rows: 0,
                        query_duration: 0.0,
                    };
                    app.editor.history.push(query_meta)
                }
            }
        }
    }
}

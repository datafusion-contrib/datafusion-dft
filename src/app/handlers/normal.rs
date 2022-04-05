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
use crate::app::datafusion::context::{QueryResults, QueryResultsMeta};
use crate::app::error::{DftError, Result};
use crate::app::ui::Scroll;
use crate::events::Key;
use arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use datafusion::prelude::DataFrame;
use log::{debug, error};
use std::sync::Arc;
use std::time::Instant;

pub enum NormalModeAction {
    Continue,
    Exit,
}

pub async fn normal_mode_handler(app: &mut App, key: Key) -> Result<AppReturn> {
    if app.tab_item == TabItem::Editor {
        match key {
            Key::Enter => execute(app).await,
            Key::Char('c') => {
                app.editor.input.clear()?;
                app.input_mode = InputMode::Editing;
                Ok(AppReturn::Continue)
            }
            Key::Char('e') => {
                app.input_mode = InputMode::Editing;
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
            Key::PageDown => {
                if let Some(results) = &mut app.query_results {
                    results.scroll.x += 10
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
            Key::PageUp => {
                if let Some(results) = &mut app.query_results {
                    let new_x = match results.scroll.x {
                        0 => 0,
                        n => n - 10,
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
            Key::Char(c) if c.is_ascii_digit() => change_tab(c, app),
            _ => Ok(AppReturn::Continue),
        }
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

pub async fn execute(app: &mut App) -> Result<AppReturn> {
    let sql: String = app.editor.input.combine_lines();
    handle_queries(app, sql).await?;
    Ok(AppReturn::Continue)
}

async fn handle_queries(app: &mut App, sql: String) -> Result<()> {
    let start = Instant::now();
    let queries = sql.split(';');
    for query in queries {
        if !query.is_empty() {
            let df = app.context.sql(query).await;
            match df {
                Ok(df) => handle_successful_query(app, start, query.to_string(), df).await?,
                Err(err) => {
                    handle_failed_query(app, query.to_string(), err)?;
                    break;
                }
            };
        }
    }
    Ok(())
}

async fn handle_successful_query(
    app: &mut App,
    start: Instant,
    sql: String,
    df: Arc<dyn DataFrame>,
) -> Result<()> {
    debug!("Successfully executed query");
    let batches = df.collect().await.map_err(DftError::DataFusionError)?;
    let query_duration = start.elapsed().as_secs_f64();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let query_meta = QueryResultsMeta {
        query: sql,
        succeeded: true,
        error: None,
        rows,
        query_duration,
    };
    app.editor.history.push(query_meta.clone());
    let pretty_batches = pretty_format_batches(&batches).unwrap().to_string();
    app.query_results = Some(QueryResults {
        batches,
        pretty_batches,
        meta: query_meta,
        scroll: Scroll { x: 0, y: 0 },
    });
    Ok(())
}

fn handle_failed_query(app: &mut App, sql: String, error: DataFusionError) -> Result<()> {
    error!("{}", error);
    let err_msg = format!("{}", error);
    app.query_results = None;
    let query_meta = QueryResultsMeta {
        query: sql,
        succeeded: false,
        error: Some(err_msg),
        rows: 0,
        query_duration: 0.0,
    };
    app.editor.history.push(query_meta);
    Ok(())
}

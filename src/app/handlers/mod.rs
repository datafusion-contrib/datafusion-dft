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

pub mod edit;
pub mod normal;
pub mod rc;

use arrow::util::pretty::pretty_format_batches;
use datafusion::error::DataFusionError;
use datafusion::prelude::DataFrame;
use log::{debug, error};
use std::sync::Arc;
use std::time::Instant;

use crate::app::core::{App, AppReturn, InputMode};
use crate::app::datafusion::context::{QueryResults, QueryResultsMeta};
use crate::app::error::{DftError, Result};
use crate::app::ui::Scroll;
use crate::events::Key;

pub async fn key_event_handler<'logs>(app: &mut App<'logs>, key: Key) -> Result<AppReturn> {
    match app.input_mode {
        InputMode::Normal => normal::normal_mode_handler(app, key).await,
        InputMode::Editing => edit::edit_mode_handler(app, key).await,
        InputMode::Rc => rc::rc_mode_handler(app, key).await,
    }
}

pub async fn execute_query<'logs>(app: &mut App<'logs>) -> Result<AppReturn> {
    let sql: String = app.editor.input.combine_lines();
    handle_queries(app, sql).await?;
    Ok(AppReturn::Continue)
}

async fn handle_queries<'logs>(app: &mut App<'logs>, sql: String) -> Result<()> {
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

async fn handle_successful_query<'logs>(
    app: &mut App<'logs>,
    start: Instant,
    sql: String,
    df: Arc<DataFrame>,
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

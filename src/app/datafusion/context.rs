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

//! Context (remote or local)

use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};

use log::{debug, error, info};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;

use crate::app::ui::Scroll;

#[derive(Clone, Debug, PartialEq)]
pub struct QueryResultsMeta {
    pub query: String,
    pub succeeded: bool,
    pub error: Option<String>,
    pub rows: usize,
    pub query_duration: f64,
}

pub struct QueryResults {
    pub batches: Vec<RecordBatch>,
    pub pretty_batches: String,
    pub meta: QueryResultsMeta,
    pub scroll: Scroll,
}

impl QueryResults {
    pub fn format_timing_info(&self) -> String {
        format!(
            "[ {} {} in set. Query took {:.3} seconds ] ",
            self.meta.rows,
            if self.meta.rows == 1 { "row" } else { "rows" },
            self.meta.query_duration
        )
    }
}

/// The CLI supports using a local DataFusion context or a distributed BallistaContext
pub enum Context {
    /// In-process execution with DataFusion
    Local(ExecutionContext),
    /// Distributed execution with Ballista (if available)
    Remote(BallistaContext),
}

impl Context {
    /// create a new remote context with given host and port
    pub fn new_remote(host: &str, port: u16) -> Result<Context> {
        debug!("Created BallistaContext @ {:?}:{:?}", host, port);
        Ok(Context::Remote(BallistaContext::try_new(host, port)?))
    }

    /// create a local context using the given config
    pub async fn new_local(config: &ExecutionConfig) -> Context {
        debug!("Created ExecutionContext");
        let ctx = ExecutionContext::with_config(config.clone());

        #[cfg(feature = "s3")]
        use crate::app::datafusion::object_stores::register_s3;
        #[cfg(feature = "s3")]
        let ctx = register_s3(ctx).await;

        #[cfg(feature = "bigtable")]
        use crate::app::datafusion::table_providers::register_bigtable;
        #[cfg(feature = "bigtable")]
        let ctx = register_bigtable(ctx).await;

        Context::Local(ctx)
    }

    /// execute an SQL statement against the context
    pub async fn sql(&mut self, sql: &str) -> Result<Arc<dyn DataFrame>> {
        info!("Executing SQL: {:?}", sql);
        match self {
            Context::Local(datafusion) => datafusion.sql(sql).await,
            Context::Remote(ballista) => ballista.sql(sql).await,
        }
    }

    pub async fn exec_files(&mut self, files: Vec<String>) {
        let files = files
            .into_iter()
            .map(|file_path| File::open(file_path).unwrap())
            .collect::<Vec<_>>();
        for file in files {
            let mut reader = BufReader::new(file);
            exec_from_lines(self, &mut reader).await;
        }
    }

    pub fn format_execution_config(&self) -> Option<Vec<String>> {
        match self {
            Context::Local(ctx) => {
                let mut config = Vec::new();
                let cfg = ctx.state.lock().config.clone();
                debug!("Extracting ExecutionConfig attributes");
                config.push(format!("Target Partitions: {}", cfg.target_partitions));
                config.push(format!("Repartition Joins: {}", cfg.repartition_joins));
                config.push(format!(
                    "Repartition Aggregations: {}",
                    cfg.repartition_aggregations
                ));
                config.push(format!("Repartition Windows: {}", cfg.repartition_windows));
                Some(config)
            }
            Context::Remote(_) => None,
        }
    }

    pub fn format_physical_optimizers(&self) -> Option<Vec<String>> {
        match self {
            Context::Local(ctx) => {
                let physical_opts = ctx.state.lock().config.physical_optimizers.clone();
                debug!("Extracting physical optimizer rules");
                let opts = physical_opts
                    .iter()
                    .map(|opt| opt.name().to_string())
                    .collect();
                Some(opts)
            }
            Context::Remote(_) => None,
        }
    }
}

async fn exec_from_lines(ctx: &mut Context, reader: &mut BufReader<File>) {
    let mut query = "".to_owned();

    for line in reader.lines() {
        match line {
            Ok(line) if line.starts_with("--") => {
                continue;
            }
            Ok(line) => {
                let line = line.trim_end();
                query.push_str(line);
                if line.ends_with(';') {
                    match exec_and_print(ctx, query).await {
                        Ok(_) => {}
                        Err(err) => error!("{:?}", err),
                    }
                    query = "".to_owned();
                } else {
                    query.push('\n');
                }
            }
            _ => {
                break;
            }
        }
    }

    // run the left over query if the last statement doesn't contain ‘;’
    if !query.is_empty() {
        match exec_and_print(ctx, query).await {
            Ok(_) => {}
            Err(err) => error!("{:?}", err),
        }
    }
}

async fn exec_and_print(ctx: &mut Context, sql: String) -> Result<()> {
    let _df = ctx.sql(&sql).await?;
    Ok(())
}

// implement wrappers around the BallistaContext to support running without ballista

// Feature added but not tested as cant install from crates
#[cfg(feature = "ballista")]
use ballista;
#[cfg(feature = "ballista")]
pub struct BallistaContext(ballista::context::BallistaContext);
#[cfg(feature = "ballista")]
impl BallistaContext {
    pub fn try_new(host: &str, port: u16) -> Result<Self> {
        use ballista::context::BallistaContext;
        use ballista::prelude::BallistaConfig;
        let config: BallistaConfig =
            BallistaConfig::new().map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        Ok(Self(BallistaContext::remote(host, port, &config)))
    }
    pub async fn sql(&mut self, sql: &str) -> Result<Arc<dyn DataFrame>> {
        self.0.sql(sql).await
    }
}

// Feature added but not tested as cant install from crates
#[cfg(not(feature = "ballista"))]
pub struct BallistaContext();
#[cfg(not(feature = "ballista"))]
impl BallistaContext {
    pub fn try_new(_host: &str, _port: u16) -> Result<Self> {
        Err(DataFusionError::NotImplemented(
            "Remote execution not supported. Compile with feature 'ballista' to enable".to_string(),
        ))
    }
    pub async fn sql(&mut self, _sql: &str) -> Result<Arc<dyn DataFrame>> {
        unreachable!()
    }
}

#[cfg(test)]
mod test {
    use datafusion::error::DataFusionError;
    use sqlparser::parser::ParserError;

    use crate::app::core::{App, TabItem};
    use crate::app::datafusion::context::{QueryResults, QueryResultsMeta};
    use crate::app::handlers::execute_query;
    use crate::app::ui::Scroll;
    use crate::cli::args::mock_standard_args;
    use crate::utils::test_util::assert_results_eq;

    #[test]
    fn test_tab_item_from_char() {
        assert!(TabItem::try_from('0').is_err());
        assert_eq!(TabItem::Editor, TabItem::try_from('1').unwrap());
        assert_eq!(TabItem::QueryHistory, TabItem::try_from('2').unwrap());
        assert_eq!(TabItem::Context, TabItem::try_from('3').unwrap());
        assert_eq!(TabItem::Logs, TabItem::try_from('4').unwrap());
        assert!(TabItem::try_from('5').is_err());
    }

    #[test]
    fn test_tab_item_to_usize() {
        (0_usize..TabItem::all_values().len()).for_each(|i| {
            assert_eq!(
                TabItem::all_values()[i],
                TabItem::try_from(format!("{}", i + 1).chars().next().unwrap()).unwrap()
            );
            assert_eq!(TabItem::all_values()[i].list_index(), i);
        });
    }

    #[tokio::test]
    async fn test_select() {
        let args = mock_standard_args();
        let mut app = App::new(args).await;

        let query = "SELECT 1";
        for char in query.chars() {
            app.editor.input.append_char(char).unwrap();
        }

        execute_query(&mut app).await.unwrap();

        let results = app.query_results.unwrap();

        let expected_meta = QueryResultsMeta {
            query: query.to_string(),
            succeeded: true,
            error: None,
            rows: 1,
            query_duration: 0f64,
        };

        let expected_results = QueryResults {
            batches: Vec::new(),
            pretty_batches: String::new(),
            meta: expected_meta,
            scroll: Scroll { x: 0, y: 0 },
        };

        assert_results_eq(Some(results), Some(expected_results));
    }

    #[tokio::test]
    async fn test_select_with_typo() {
        let args = mock_standard_args();
        let mut app = App::new(args).await;

        let query = "SELE 1";
        for char in query.chars() {
            app.editor.input.append_char(char).unwrap();
        }

        execute_query(&mut app).await.unwrap();

        let actual = app.editor.history.pop();

        let expected_meta = QueryResultsMeta {
            query: query.to_string(),
            succeeded: false,
            error: Some(
                "SQL error: ParserError(\"Expected an SQL statement, found: SELE\")".to_string(),
            ),
            rows: 0,
            query_duration: 0f64,
        };

        assert_eq!(actual, Some(expected_meta));
    }

    #[tokio::test]
    async fn test_create_table() {
        let args = mock_standard_args();
        let mut app = App::new(args).await;

        let query = "CREATE TABLE abc AS VALUES (1,2,3)";

        for char in query.chars() {
            app.editor.input.append_char(char).unwrap();
        }

        execute_query(&mut app).await.unwrap();

        let results = app.query_results;

        let expected_meta = QueryResultsMeta {
            query: query.to_string(),
            succeeded: true,
            error: None,
            rows: 0,
            query_duration: 0f64,
        };

        let expected_results = QueryResults {
            batches: Vec::new(),
            pretty_batches: String::new(),
            meta: expected_meta,
            scroll: Scroll { x: 0, y: 0 },
        };

        assert_results_eq(results, Some(expected_results));
    }
}

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

use arrow::json;

use arrow::datatypes::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::{
    expressions::PhysicalSortExpr, DisplayFormatType, ExecutionPlan, Partitioning, Statistics,
};
use futures::{future, stream, Future, Stream};
use http::Uri;
use hyper::body::{Buf, HttpBody};
use hyper::{Client, StatusCode};
use hyper_tls::HttpsConnector;
use serde_json;
use std::{any::Any, sync::Arc};

use crate::app::error::Result;

#[derive(Debug)]
pub enum ApiFormat {
    Json,
}

#[derive(Debug)]
pub struct ApiConfig {
    uri: Uri,
    format: ApiFormat,
}

/// An implementation of `TableProvider` that calls an API.
pub struct ApiTable {
    config: ApiConfig,
    table_schema: SchemaRef,
}

impl ApiTable {
    async fn get_page(self, uri: Uri) -> Result<()> {
        let https = HttpsConnector::new();
        let client = Client::builder().build::<_, hyper::Body>(https);
        let mut response = client.get(uri).await?;

        while let Some(chunk) = response.body_mut().data().await {
            let chunk_reader = chunk?.reader();
            let builder = json::ReaderBuilder::new().infer_schema(Some(100));
            let reader = builder.build(chunk_reader)?;

            println!("Chunk: {:?}", chunk);
        }

        match response.status() {
            StatusCode::OK => println!("Successful API call"),
            _ => println!("Unsuccesful API call"),
        };
        Ok(())
    }
}

#[async_trait]
impl TableProvider for ApiTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let uri = self.config.uri;
    }
}

#[derive(Debug)]
struct JsonApiExec {
    config: ApiConfig,
    schema_infer_max_rec: Option<usize>,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
}

impl ExecutionPlan for JsonApiExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        // TODO: Come back to this when implementing paginated APIs
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let proj = self.base_config.projected_file_column_names();

        let batch_size = context.session_config().batch_size;
        let file_schema = Arc::clone(&self.base_config.file_schema);

        // The json reader cannot limit the number of records, so `remaining` is ignored.
        let fun = move |file, _remaining: &Option<usize>| {
            // TODO: make DecoderOptions implement Clone so we can
            // clone here rather than recreating the options each time
            // https://github.com/apache/arrow-rs/issues/1580
            let options = DecoderOptions::new().with_batch_size(batch_size);

            let options = if let Some(proj) = proj.clone() {
                options.with_projection(proj)
            } else {
                options
            };

            Box::new(json::Reader::new(file, Arc::clone(&file_schema), options)) as BatchIter
        };

        Ok(Box::pin(FileStream::new(
            Arc::clone(&self.base_config.object_store),
            self.base_config.file_groups[partition].clone(),
            fun,
            Arc::clone(&self.projected_schema),
            self.base_config.limit,
            self.base_config.table_partition_cols.clone(),
        )))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "JsonApiExec: limit={:?}, endpoint={}",
                    self.config.limit,
                    super::FileGroupsDisplay(&self.base_config.file_groups),
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}

#[cfg(test)]
mod test {
    use crate::app::datafusion::table_providers::api::ApiTable;
    use crate::app::error::Result;

    #[tokio::test]
    async fn test_get_api_results() -> Result<()> {
        let uri = "https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@1/latest/currencies.json"
            .parse()
            .unwrap();

        ApiTable::get_page(uri).await
    }
}

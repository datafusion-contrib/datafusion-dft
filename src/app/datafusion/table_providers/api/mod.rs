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

pub mod json;

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::{ColumnStatistics, ExecutionPlan, Statistics};
use futures::{future, stream, AsyncRead, Future, Stream};
use http::Uri;
use hyper::body::{Buf, HttpBody};
use hyper::{Client, StatusCode};
use hyper_tls::HttpsConnector;
use std::{any::Any, fmt, io::Read, pin::Pin, sync::Arc};

use crate::app::error::Result;

/// Stream readers opened on a given API
pub type ApiPageReaderStream =
    Pin<Box<dyn Stream<Item = Result<Arc<dyn ApiPageReader>>> + Send + Sync>>;

/// API Reader for one page from an object store.
///
/// Note that the dynamic dispatch on the reader might
/// have some performance impacts.
#[async_trait]
pub trait ApiPageReader: Send + Sync {
    /// Get reader for a part [start, start + length] in the file asynchronously
    async fn chunk_reader(&self, start: u64, length: usize) -> Result<Box<dyn AsyncRead>>;

    /// Get reader for a part [start, start + length] in the file
    fn sync_chunk_reader(&self, start: u64, length: usize) -> Result<Box<dyn Read + Send + Sync>>;

    /// Get reader for the entire file
    fn sync_reader(&self) -> Result<Box<dyn Read + Send + Sync>> {
        self.sync_chunk_reader(0, self.length() as usize)
    }

    async fn sync_get_page(uri: Uri) -> Result<Box<dyn Read + Send + Sync>> {
        // let https = HttpsConnector::new();
        // let client = Client::builder().build::<_, hyper::Body>(https);
        // let mut response = client.get(uri).await?;

        // while let Some(chunk) = response.body_mut().data().await {
        //     let chunk_reader = chunk?.reader();
        //     // let builder = json::ReaderBuilder::new().infer_schema(Some(100));
        //     // let reader = builder.build(chunk_reader)?;

        //     println!("Chunk: {:?}", chunk);
        // }

        // match response.status() {
        //     StatusCode::OK => println!("Successful API call"),
        //     _ => println!("Unsuccesful API call"),
        // };
        // Ok(())
    }

    /// Get the size of the file
    fn length(&self) -> u64;
}

#[derive(Debug, Clone)]
/// A single page from an API
pub struct ApiPage {
    /// URI for the API
    pub uri: Uri,
}

/// The base configurations to provide when creating a physical plan for
/// any given API.
#[derive(Debug, Clone)]
pub struct ApiScanConfig {
    /// Schema before projection. It contains the columns that are expected
    /// to be in the API results without the table partition columns.
    pub api_schema: SchemaRef,
    /// List of files to be processed, grouped into partitions
    pub api_pages: Vec<Vec<ApiPage>>,
    /// Estimated overall statistics of the files, taking `filters` into account.
    pub statistics: Statistics,
    /// Columns on which to project the data. Indexes that are higher than the
    /// number of columns of `api_schema` refer to `table_partition_cols`.
    pub projection: Option<Vec<usize>>,
    /// The minimum number of records required from this source plan
    pub limit: Option<usize>,
}

impl ApiScanConfig {
    /// Project the schema and the statistics on the given column indices
    fn project(&self) -> (SchemaRef, Statistics) {
        if self.projection.is_none() {
            return (Arc::clone(&self.api_schema), self.statistics.clone());
        }

        let proj_iter: Box<dyn Iterator<Item = usize>> = match &self.projection {
            Some(proj) => Box::new(proj.iter().copied()),
            None => Box::new(0..(self.api_schema.fields().len())),
        };

        let mut table_fields = vec![];
        let mut table_cols_stats = vec![];
        for idx in proj_iter {
            if idx < self.api_schema.fields().len() {
                table_fields.push(self.api_schema.field(idx).clone());
                if let Some(file_cols_stats) = &self.statistics.column_statistics {
                    table_cols_stats.push(file_cols_stats[idx].clone())
                } else {
                    table_cols_stats.push(ColumnStatistics::default())
                }
            } else {
                // There shouldnt be an partitioning columns for APIs
                panic!("Found partitioning columns when there should be none")
            }
        }

        let table_stats = Statistics {
            num_rows: self.statistics.num_rows,
            is_exact: self.statistics.is_exact,
            // TODO correct byte size?
            total_byte_size: None,
            column_statistics: Some(table_cols_stats),
        };

        let table_schema = Arc::new(Schema::new(table_fields));

        (table_schema, table_stats)
    }

    fn projected_file_column_names(&self) -> Option<Vec<String>> {
        self.projection.as_ref().map(|p| {
            p.iter()
                .filter(|col_idx| **col_idx < self.api_schema.fields().len())
                .map(|col_idx| self.api_schema.field(*col_idx).name())
                .cloned()
                .collect()
        })
    }

    fn file_column_projection_indices(&self) -> Option<Vec<usize>> {
        self.projection.as_ref().map(|p| {
            p.iter()
                .filter(|col_idx| **col_idx < self.api_schema.fields().len())
                .copied()
                .collect()
        })
    }
}

#[async_trait]
pub trait ApiFormat: Send + Sync + fmt::Debug {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Infer the common schema of the return API responses. The objects will usually
    /// be analysed up to a given number of records or pages (as specified in the
    /// format config) then give the estimated common schema. This might fail if
    /// the files have schemas that cannot be merged.
    // async fn infer_schema(&self, readers: PageReaderStream) -> Result<SchemaRef>;
    async fn infer_schema(&self, readers: Arc<dyn ApiPageReader>) -> Result<SchemaRef>;

    /// Infer the statistics for the provided object. The cost and accuracy of the
    /// estimated statistics might vary greatly between file formats.
    ///
    /// `table_schema` is the (combined) schema of the overall table
    /// and may be a superset of the schema contained in this file.
    ///
    /// TODO: should the file source return statistics for only columns referred to in the table schema?
    async fn infer_stats(
        &self,
        reader: Arc<dyn ApiPageReader>,
        table_schema: SchemaRef,
    ) -> Result<Statistics>;

    /// Take a list of files and convert it to the appropriate executor
    /// according to this file format.
    async fn create_physical_plan(&self, conf: ApiScanConfig) -> Result<Arc<dyn ExecutionPlan>>;
}

#[derive(Debug)]
pub struct ApiConfig {
    uri: Uri,
    format: Arc<dyn ApiFormat>,
}

/// An implementation of `TableProvider` that calls an API.
pub struct ApiTable {
    uri: Uri,
    format: Arc<dyn ApiFormat>,
    table_schema: SchemaRef,
}

impl ApiTable {
    pub async fn try_new(config: ApiConfig) -> Result<Self> {
        let page = ApiTable::get_page(config.uri).await?;
        let api_schema = config.format.infer_schema(page)?;
        Ok(Self {
            uri: config.uri,
            format: config.format,
            table_schema: api_schema,
        })
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

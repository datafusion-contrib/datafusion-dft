// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

// use arrow::json;

// use arrow::datatypes::{Field, Schema, SchemaRef};
// use async_trait::async_trait;
// use datafusion::datasource::{TableProvider, TableType};
// use datafusion::error::Result as DFResult;
// use datafusion::logical_plan::Expr;
// use datafusion::physical_plan::{
//     expressions::PhysicalSortExpr, ColumnStatistics, DisplayFormatType, ExecutionPlan,
//     Partitioning, Statistics,
// };
// use futures::{future, stream, AsyncRead, Future, Stream};
// use http::Uri;
// use hyper::body::{Buf, HttpBody};
// use hyper::{Client, StatusCode};
// use hyper_tls::HttpsConnector;
// use serde_json;
// use std::{any::Any, fmt, io::Read, pin::Pin, sync::Arc};

// use crate::app::error::Result;

// /// Stream readers opened on a given object store
// pub type PageReaderStream = Pin<Box<dyn Stream<Item = Result<Arc<dyn PageReader>>> + Send + Sync>>;

// /// Object Reader for one file in an object store.
// ///
// /// Note that the dynamic dispatch on the reader might
// /// have some performance impacts.
// #[async_trait]
// pub trait PageReader: Send + Sync {
//     /// Get reader for a part [start, start + length] in the file asynchronously
//     async fn chunk_reader(&self, start: u64, length: usize) -> Result<Box<dyn AsyncRead>>;

//     /// Get reader for a part [start, start + length] in the file
//     fn sync_chunk_reader(&self, start: u64, length: usize) -> Result<Box<dyn Read + Send + Sync>>;

//     /// Get reader for the entire file
//     fn sync_reader(&self) -> Result<Box<dyn Read + Send + Sync>> {
//         self.sync_chunk_reader(0, self.length() as usize)
//     }

//     /// Get the size of the file
//     fn length(&self) -> u64;
// }

// #[derive(Debug, Clone)]
// /// A single page from an API
// pub struct ApiPage {
//     /// URI for the API
//     pub uri: Uri,
// }

// /// The base configurations to provide when creating a physical plan for
// /// any given API.
// #[derive(Debug, Clone)]
// pub struct ApiScanConfig {
//     /// Schema before projection. It contains the columns that are expected
//     /// to be in the API results without the table partition columns.
//     pub api_schema: SchemaRef,
//     /// List of files to be processed, grouped into partitions
//     pub api_pages: Vec<Vec<ApiPage>>,
//     /// Estimated overall statistics of the files, taking `filters` into account.
//     pub statistics: Statistics,
//     /// Columns on which to project the data. Indexes that are higher than the
//     /// number of columns of `api_schema` refer to `table_partition_cols`.
//     pub projection: Option<Vec<usize>>,
//     /// The minimum number of records required from this source plan
//     pub limit: Option<usize>,
// }

// impl ApiScanConfig {
//     /// Project the schema and the statistics on the given column indices
//     fn project(&self) -> (SchemaRef, Statistics) {
//         if self.projection.is_none() {
//             return (Arc::clone(&self.api_schema), self.statistics.clone());
//         }

//         let proj_iter: Box<dyn Iterator<Item = usize>> = match &self.projection {
//             Some(proj) => Box::new(proj.iter().copied()),
//             None => Box::new(0..(self.api_schema.fields().len())),
//         };

//         let mut table_fields = vec![];
//         let mut table_cols_stats = vec![];
//         for idx in proj_iter {
//             if idx < self.api_schema.fields().len() {
//                 table_fields.push(self.api_schema.field(idx).clone());
//                 if let Some(file_cols_stats) = &self.statistics.column_statistics {
//                     table_cols_stats.push(file_cols_stats[idx].clone())
//                 } else {
//                     table_cols_stats.push(ColumnStatistics::default())
//                 }
//             } else {
//                 // There shouldnt be an partitioning columns for APIs
//                 panic!("Found partitioning columns when there should be none")
//             }
//         }

//         let table_stats = Statistics {
//             num_rows: self.statistics.num_rows,
//             is_exact: self.statistics.is_exact,
//             // TODO correct byte size?
//             total_byte_size: None,
//             column_statistics: Some(table_cols_stats),
//         };

//         let table_schema = Arc::new(Schema::new(table_fields));

//         (table_schema, table_stats)
//     }

//     fn projected_file_column_names(&self) -> Option<Vec<String>> {
//         self.projection.as_ref().map(|p| {
//             p.iter()
//                 .filter(|col_idx| **col_idx < self.api_schema.fields().len())
//                 .map(|col_idx| self.api_schema.field(*col_idx).name())
//                 .cloned()
//                 .collect()
//         })
//     }

//     fn file_column_projection_indices(&self) -> Option<Vec<usize>> {
//         self.projection.as_ref().map(|p| {
//             p.iter()
//                 .filter(|col_idx| **col_idx < self.api_schema.fields().len())
//                 .copied()
//                 .collect()
//         })
//     }
// }

// #[async_trait]
// pub trait ApiFormat: Send + Sync + fmt::Debug {
//     /// Returns the table provider as [`Any`](std::any::Any) so that it can be
//     /// downcast to a specific implementation.
//     fn as_any(&self) -> &dyn Any;

//     /// Infer the common schema of the return API responses. The objects will usually
//     /// be analysed up to a given number of records or pages (as specified in the
//     /// format config) then give the estimated common schema. This might fail if
//     /// the files have schemas that cannot be merged.
//     // async fn infer_schema(&self, readers: PageReaderStream) -> Result<SchemaRef>;
//     async fn infer_schema(&self, readers: Arc<dyn PageReader>) -> Result<SchemaRef>;

//     /// Infer the statistics for the provided object. The cost and accuracy of the
//     /// estimated statistics might vary greatly between file formats.
//     ///
//     /// `table_schema` is the (combined) schema of the overall table
//     /// and may be a superset of the schema contained in this file.
//     ///
//     /// TODO: should the file source return statistics for only columns referred to in the table schema?
//     async fn infer_stats(
//         &self,
//         reader: Arc<dyn PageReader>,
//         table_schema: SchemaRef,
//     ) -> Result<Statistics>;

//     /// Take a list of files and convert it to the appropriate executor
//     /// according to this file format.
//     async fn create_physical_plan(&self, conf: ApiScanConfig) -> Result<Arc<dyn ExecutionPlan>>;
// }

// #[derive(Debug)]
// pub struct ApiConfig {
//     uri: Uri,
//     format: Arc<dyn ApiFormat>,
// }

// /// An implementation of `TableProvider` that calls an API.
// pub struct ApiTable {
//     uri: ApiConfig,
//     format: Arc<dyn ApiFormat>,
//     table_schema: SchemaRef,
// }

// impl ApiTable {
//     pub fn new(config: ApiConfig) -> Self {
//         let api_schema = format.infer_schema(uri)
//         Self {
//             uri: config.uri,
//             format: config.format,
//         }
//     }

//     async fn get_page(self, uri: Uri) -> Result<()> {
//         let https = HttpsConnector::new();
//         let client = Client::builder().build::<_, hyper::Body>(https);
//         let mut response = client.get(uri).await?;

//         while let Some(chunk) = response.body_mut().data().await {
//             let chunk_reader = chunk?.reader();
//             let builder = json::ReaderBuilder::new().infer_schema(Some(100));
//             let reader = builder.build(chunk_reader)?;

//             println!("Chunk: {:?}", chunk);
//         }

//         match response.status() {
//             StatusCode::OK => println!("Successful API call"),
//             _ => println!("Unsuccesful API call"),
//         };
//         Ok(())
//     }
// }

// #[async_trait]
// impl TableProvider for ApiTable {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }

//     fn schema(&self) -> SchemaRef {
//         Arc::clone(&self.table_schema)
//     }

//     fn table_type(&self) -> TableType {
//         TableType::Base
//     }

//     async fn scan(
//         &self,
//         projection: &Option<Vec<usize>>,
//         filters: &[Expr],
//         limit: Option<usize>,
//     ) -> Result<Arc<dyn ExecutionPlan>> {
//         let uri = self.config.uri;
//     }
// }

// #[derive(Debug)]
// struct JsonApiExec {
//     config: ApiScanConfig,
//     projected_statistics: Statistics,
//     projected_schema: SchemaRef,
// }

// impl JsonApiExec {
//     pub fn new(base_config: ApiScanConfig) -> Self {
//         let (projected_schema, projected_statistics) = base_config.project();

//         Self {
//             config: base_config,
//             projected_statistics,
//             projected_schema,
//         }
//     }
// }

// impl ExecutionPlan for JsonApiExec {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }

//     fn schema(&self) -> SchemaRef {
//         self.projected_schema.clone()
//     }

//     fn output_partitioning(&self) -> Partitioning {
//         // TODO: Come back to this when implementing paginated APIs
//         Partitioning::UnknownPartitioning(1)
//     }

//     fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
//         None
//     }

//     fn relies_on_input_order(&self) -> bool {
//         false
//     }

//     fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
//         Vec::new()
//     }

//     fn with_new_children(
//         self: Arc<Self>,
//         _: Vec<Arc<dyn ExecutionPlan>>,
//     ) -> DFResult<Arc<dyn ExecutionPlan>> {
//         Ok(self)
//     }

//     fn execute(
//         &self,
//         partition: usize,
//         context: Arc<TaskContext>,
//     ) -> Result<SendableRecordBatchStream> {
//         let proj = self.base_config.projected_file_column_names();

//         let batch_size = context.session_config().batch_size;
//         let file_schema = Arc::clone(&self.base_config.file_schema);

//         // The json reader cannot limit the number of records, so `remaining` is ignored.
//         let fun = move |file, _remaining: &Option<usize>| {
//             // TODO: make DecoderOptions implement Clone so we can
//             // clone here rather than recreating the options each time
//             // https://github.com/apache/arrow-rs/issues/1580
//             let options = DecoderOptions::new().with_batch_size(batch_size);

//             let options = if let Some(proj) = proj.clone() {
//                 options.with_projection(proj)
//             } else {
//                 options
//             };

//             Box::new(json::Reader::new(file, Arc::clone(&file_schema), options)) as BatchIter
//         };

//         Ok(Box::pin(FileStream::new(
//             Arc::clone(&self.base_config.object_store),
//             self.base_config.file_groups[partition].clone(),
//             fun,
//             Arc::clone(&self.projected_schema),
//             self.base_config.limit,
//             self.base_config.table_partition_cols.clone(),
//         )))
//     }

//     fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//         match t {
//             DisplayFormatType::Default => {
//                 write!(
//                     f,
//                     "JsonApiExec: limit={:?}, endpoint={}",
//                     self.config.limit,
//                     super::FileGroupsDisplay(&self.base_config.file_groups),
//                 )
//             }
//         }
//     }

//     fn statistics(&self) -> Statistics {
//         self.projected_statistics.clone()
//     }
// }

// #[cfg(test)]
// mod test {
//     use crate::app::datafusion::table_providers::api::ApiTable;
//     use crate::app::error::Result;

//     #[tokio::test]
//     async fn test_get_api_results() -> Result<()> {
//         let uri = "https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@1/latest/currencies.json"
//             .parse()
//             .unwrap();

//         ApiTable::get_page(uri).await
//     }
// }

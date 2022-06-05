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

use arrow::datatypes::SchemaRef;
use arrow::json;
use arrow::json::reader::infer_json_schema_from_iterator;
use arrow::json::reader::ValueIter;
use async_trait::async_trait;
use bytes::Buf;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::TaskContext;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::{
    expressions::PhysicalSortExpr, DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use std::io::BufReader;
use std::{any::Any, sync::Arc};

use crate::app::datafusion::table_providers::api::{ApiFormat, ApiScanConfig, ApiTable};
use crate::app::error::Result;

/// New line delimited JSON `FileFormat` implementation.
#[derive(Debug)]
pub struct JsonFormat {
    schema_infer_max_rec: Option<usize>,
}

impl Default for JsonFormat {
    fn default() -> Self {
        Self {
            schema_infer_max_rec: Some(100),
        }
    }
}

impl JsonFormat {
    /// Set a limit in terms of records to scan to infer the schema
    /// - defaults to `DEFAULT_SCHEMA_INFER_MAX_RECORD`
    pub fn with_schema_infer_max_rec(mut self, max_rec: Option<usize>) -> Self {
        self.schema_infer_max_rec = max_rec;
        self
    }
}

#[async_trait]
impl ApiFormat for JsonFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(&self, uri: Uri) -> Result<SchemaRef> {
        let mut schemas = Vec::new();
        let mut records_to_read = self.schema_infer_max_rec.unwrap_or(usize::MAX);
        let reader = ApiTable::try_get_page(uri).await?.bytes.reader();
        let mut reader = BufReader::new(reader);
        let iter = ValueIter::new(&mut reader, None);
        let schema = infer_json_schema_from_iterator(iter.take_while(|_| {
            let should_take = records_to_read > 0;
            if should_take {
                records_to_read -= 1;
            }
            should_take
        }))?;
        // for file in files {
        //     let reader = store.file_reader(file.sized_file.clone())?.sync_reader()?;
        //     let mut reader = BufReader::new(reader);
        //     let iter = ValueIter::new(&mut reader, None);
        //     let schema = infer_json_schema_from_iterator(iter.take_while(|_| {
        //         let should_take = records_to_read > 0;
        //         if should_take {
        //             records_to_read -= 1;
        //         }
        //         should_take
        //     }))?;
        //     schemas.push(schema);
        //     if records_to_read == 0 {
        //         break;
        //     }
        // }

        Ok(Arc::new(schema))
    }

    // async fn infer_stats(
    //     &self,
    //     _store: &Arc<dyn ObjectStore>,
    //     _table_schema: SchemaRef,
    //     _file: &FileMeta,
    // ) -> Result<Statistics> {
    //     Ok(Statistics::default())
    // }

    async fn create_physical_plan(
        &self,
        conf: ApiScanConfig,
        _filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = JsonApiExec::new(conf);
        Ok(Arc::new(exec))
    }
}

/// Execution plan for scanning NdJson data source
#[derive(Debug, Clone)]
pub struct JsonApiExec {
    base_config: ApiScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
}

impl JsonApiExec {
    /// Create a new JSON reader execution plan provided base configurations
    pub fn new(base_config: ApiScanConfig) -> Self {
        let (projected_schema, projected_statistics) = base_config.project();

        Self {
            base_config,
            projected_schema,
            projected_statistics,
        }
    }
}

impl ExecutionPlan for JsonApiExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
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

        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url)?;

        Ok(Box::pin(FileStream::new(
            object_store,
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
                    "JsonExec: limit={:?}, files={}",
                    self.base_config.limit,
                    super::FileGroupsDisplay(&self.base_config.file_groups),
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}

pub async fn plan_to_json(
    state: &SessionState,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
) -> Result<()> {
    let path = path.as_ref();
    // create directory to contain the CSV files (one per partition)
    let fs_path = Path::new(path);
    match fs::create_dir(fs_path) {
        Ok(()) => {
            let mut tasks = vec![];
            for i in 0..plan.output_partitioning().partition_count() {
                let plan = plan.clone();
                let filename = format!("part-{}.json", i);
                let path = fs_path.join(&filename);
                let file = fs::File::create(path)?;
                let mut writer = json::LineDelimitedWriter::new(file);
                let task_ctx = Arc::new(TaskContext::from(state));
                let stream = plan.execute(i, task_ctx)?;
                let handle: JoinHandle<Result<()>> = task::spawn(async move {
                    stream
                        .map(|batch| writer.write(batch?))
                        .try_collect()
                        .await
                        .map_err(DataFusionError::from)
                });
                tasks.push(handle);
            }
            futures::future::join_all(tasks).await;
            Ok(())
        }
        Err(e) => Err(DataFusionError::Execution(format!(
            "Could not create directory {}: {:?}",
            path, e
        ))),
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

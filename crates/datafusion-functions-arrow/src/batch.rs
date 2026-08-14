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

use crate::filename_arg;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::plan_err;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use std::fs::File;
use std::sync::Arc;

/// The record batches read from the requested blocks of an Arrow IPC file.
#[derive(Debug)]
struct ArrowBatchTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[async_trait]
impl TableProvider for ArrowBatchTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(MemorySourceConfig::try_new_exec(
            std::slice::from_ref(&self.batches),
            TableProvider::schema(self),
            projection.cloned(),
        )?)
    }
}

fn expr_to_index(expr: &Expr, arg: &str) -> Result<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(i)), _) if *i >= 0 => Ok(*i as usize),
        _ => plan_err!("arrow_batch requires a non-negative integer {arg} argument"),
    }
}

/// `arrow_batch` table-valued function
///
/// Reads the data from specific record batches of an Arrow IPC file. Takes a
/// filename and either a single batch index or an inclusive start and end
/// index. The indexes match the `batch_index` column of `arrow_batches`.
///
/// Examples:
/// ```sql
/// -- Read batch 1
/// SELECT * FROM arrow_batch('file.arrow', 1);
/// -- Read batches 1 through 3 (inclusive)
/// SELECT * FROM arrow_batch('file.arrow', 1, 3);
/// ```
#[derive(Debug)]
pub struct ArrowBatchFunc {}

impl TableFunctionImpl for ArrowBatchFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = filename_arg("arrow_batch", exprs)?.to_string();
        if exprs.len() < 2 || exprs.len() > 3 {
            return plan_err!(
                "arrow_batch requires a filename and either a single batch index or a start and end index"
            );
        }
        let start = expr_to_index(&exprs[1], "batch index")?;
        let end = match exprs.get(2) {
            Some(expr) => expr_to_index(expr, "end index")?,
            None => start,
        };
        if end < start {
            return plan_err!(
                "arrow_batch end index ({end}) must be greater than or equal to start index ({start})"
            );
        }

        let file = File::open(&filename)?;
        let mut reader = FileReader::try_new(file, None)?;
        let num_batches = reader.num_batches();
        if end >= num_batches {
            return plan_err!(
                "batch index {end} is out of bounds for {filename} which has {num_batches} batches"
            );
        }

        let schema = reader.schema();
        reader.set_index(start)?;
        let mut batches = Vec::with_capacity(end - start + 1);
        for _ in start..=end {
            match reader.next() {
                Some(batch) => batches.push(batch?),
                None => break,
            }
        }

        Ok(Arc::new(ArrowBatchTable { schema, batches }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use datafusion::prelude::SessionContext;
    use std::path::Path;

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("arrow_batch", Arc::new(ArrowBatchFunc {}));
        ctx
    }

    /// Write an IPC file with 3 batches of 2 rows each (values 0..6).
    fn write_ipc(path: &Path) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let file = std::fs::File::create(path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        for chunk in (0..6).collect::<Vec<i64>>().chunks(2) {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(chunk.to_vec()))],
            )
            .unwrap();
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
    }

    async fn query_values(ctx: &SessionContext, sql: &str) -> Vec<i64> {
        let result = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        result
            .iter()
            .flat_map(|batch| {
                batch
                    .column(batch.schema().index_of("v").unwrap())
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    /// A single index reads only that batch.
    #[tokio::test]
    async fn test_single_batch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        write_ipc(&path);

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM arrow_batch('{}', 1) ORDER BY v",
            path.display()
        );
        assert_eq!(query_values(&ctx, &sql).await, vec![2, 3]);
    }

    /// A start and end index reads the inclusive range of batches.
    #[tokio::test]
    async fn test_batch_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        write_ipc(&path);

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM arrow_batch('{}', 1, 2) ORDER BY v",
            path.display()
        );
        assert_eq!(query_values(&ctx, &sql).await, vec![2, 3, 4, 5]);

        let sql = format!(
            "SELECT * FROM arrow_batch('{}', 0, 2) ORDER BY v",
            path.display()
        );
        assert_eq!(query_values(&ctx, &sql).await, vec![0, 1, 2, 3, 4, 5]);
    }

    /// An out of bounds index produces an error naming the file's batch count.
    #[tokio::test]
    async fn test_out_of_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        write_ipc(&path);

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM arrow_batch('{}', 3)", path.display());
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(
            err.contains("batch index 3 is out of bounds") && err.contains("3 batches"),
            "{err}"
        );
    }

    /// An end index before the start index produces an error.
    #[tokio::test]
    async fn test_end_before_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        write_ipc(&path);

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM arrow_batch('{}', 2, 1)", path.display());
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(
            err.contains("end index (1) must be greater than or equal to start index (2)"),
            "{err}"
        );
    }

    /// Missing or invalid index arguments produce a plan error.
    #[tokio::test]
    async fn test_invalid_arguments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        write_ipc(&path);

        let ctx = make_ctx();
        for sql in [
            format!("SELECT * FROM arrow_batch('{}')", path.display()),
            format!("SELECT * FROM arrow_batch('{}', 'a')", path.display()),
            format!("SELECT * FROM arrow_batch('{}', -1)", path.display()),
            format!("SELECT * FROM arrow_batch('{}', 0, 1, 2)", path.display()),
        ] {
            assert!(ctx.sql(&sql).await.is_err(), "expected error for: {sql}");
        }
    }
}

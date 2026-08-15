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
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{plan_err, Column};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::sync::Arc;

#[derive(Debug)]
struct ParquetRowGroupsTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[async_trait]
impl TableProvider for ParquetRowGroupsTable {
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
        _ => plan_err!("parquet_row_groups requires a non-negative integer {arg} argument"),
    }
}

/// `parquet_row_groups` table-valued function
///
/// Reads the data from specific row groups of a Parquet file. Takes a filename
/// and either a single row group index or an inclusive start and end index.
///
/// Examples:
/// ```sql
/// -- Read row group 1
/// SELECT * FROM parquet_row_groups('file.parquet', 1);
/// -- Read row groups 1 through 3 (inclusive)
/// SELECT * FROM parquet_row_groups('file.parquet', 1, 3);
/// ```
#[derive(Debug)]
pub struct ParquetRowGroupsFunc {}

impl TableFunctionImpl for ParquetRowGroupsFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = match exprs.first() {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => s,
            Some(Expr::Column(Column { name, .. })) => name,
            _ => {
                return plan_err!(
                    "parquet_row_groups requires a string filename as its first argument"
                );
            }
        };
        if exprs.len() < 2 || exprs.len() > 3 {
            return plan_err!(
                "parquet_row_groups requires a filename and either a single row group index or a start and end index"
            );
        }
        let start = expr_to_index(&exprs[1], "row group index")?;
        let end = match exprs.get(2) {
            Some(expr) => expr_to_index(expr, "end index")?,
            None => start,
        };
        if end < start {
            return plan_err!(
                "parquet_row_groups end index ({end}) must be greater than or equal to start index ({start})"
            );
        }

        let file = File::open(filename)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let num_row_groups = builder.metadata().num_row_groups();
        if end >= num_row_groups {
            return plan_err!(
                "row group index {end} is out of bounds for {filename} which has {num_row_groups} row groups"
            );
        }

        let schema = builder.schema().clone();
        let reader = builder.with_row_groups((start..=end).collect()).build()?;
        let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(Arc::new(ParquetRowGroupsTable { schema, batches }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::path::Path;

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("parquet_row_groups", Arc::new(ParquetRowGroupsFunc {}));
        ctx
    }

    /// Write a parquet file with 6 rows (values 0..6) split into 3 row groups
    /// of 2 rows each.
    fn write_parquet(path: &Path) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from((0..6).collect::<Vec<i64>>()))],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
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

    /// A single index reads only that row group.
    #[tokio::test]
    async fn test_single_row_group() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path);

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM parquet_row_groups('{}', 1) ORDER BY v",
            path.display()
        );
        assert_eq!(query_values(&ctx, &sql).await, vec![2, 3]);
    }

    /// A start and end index reads the inclusive range of row groups.
    #[tokio::test]
    async fn test_row_group_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path);

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM parquet_row_groups('{}', 1, 2) ORDER BY v",
            path.display()
        );
        assert_eq!(query_values(&ctx, &sql).await, vec![2, 3, 4, 5]);

        let sql = format!(
            "SELECT * FROM parquet_row_groups('{}', 0, 2) ORDER BY v",
            path.display()
        );
        assert_eq!(query_values(&ctx, &sql).await, vec![0, 1, 2, 3, 4, 5]);
    }

    /// An out of bounds index produces an error naming the file's row group count.
    #[tokio::test]
    async fn test_out_of_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path);

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_row_groups('{}', 3)", path.display());
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(
            err.contains("row group index 3 is out of bounds") && err.contains("3 row groups"),
            "{err}"
        );
    }

    /// An end index before the start index produces an error.
    #[tokio::test]
    async fn test_end_before_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path);

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM parquet_row_groups('{}', 2, 1)",
            path.display()
        );
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(
            err.contains("end index (1) must be greater than or equal to start index (2)"),
            "{err}"
        );
    }

    /// Missing or non-integer index arguments produce a plan error.
    #[tokio::test]
    async fn test_invalid_arguments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path);

        let ctx = make_ctx();
        for sql in [
            format!("SELECT * FROM parquet_row_groups('{}')", path.display()),
            format!(
                "SELECT * FROM parquet_row_groups('{}', 'a')",
                path.display()
            ),
            format!("SELECT * FROM parquet_row_groups('{}', -1)", path.display()),
            format!(
                "SELECT * FROM parquet_row_groups('{}', 0, 1, 2)",
                path.display()
            ),
        ] {
            assert!(ctx.sql(&sql).await.is_err(), "expected error for: {sql}");
        }
    }
}

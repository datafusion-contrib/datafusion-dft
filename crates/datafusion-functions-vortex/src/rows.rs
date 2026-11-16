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

use crate::common::{check_max_args, expr_to_string, expr_to_u64, vx_err, VortexInput};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::plan_err;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::sync::Arc;
use vortex::arrow::ArrowSessionExt;

const NAME: &str = "vortex_rows";

/// `vortex_rows` table-valued function
///
/// Reads the data rows of a Vortex file in the half-open row range
/// `[start, end)`. When `end` is omitted, rows are read to the end of the
/// file. An `end` past the last row is clamped to the row count. Only the
/// segments that hold the selected rows are read; zone maps and splits are
/// used to skip the others.
///
/// Examples:
/// ```sql
/// -- Rows 100 through 199
/// SELECT * FROM vortex_rows('file.vortex', 100, 200);
/// -- Rows from 1000 to the end of the file
/// SELECT * FROM vortex_rows('file.vortex', 1000);
/// ```
#[derive(Debug)]
pub struct VortexRowsFunc {}

impl TableFunctionImpl for VortexRowsFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        check_max_args(exprs, 3, NAME)?;
        let filename = expr_to_string(exprs.first(), NAME, "filename")?;
        let Some(start_expr) = exprs.get(1) else {
            return plan_err!("{NAME} requires a filename, a start row, and an optional end row");
        };
        let start = expr_to_u64(start_expr, NAME, "start row")?;
        let input = VortexInput::open(&filename, false)?;
        let row_count = input.file.row_count();
        let end = match exprs.get(2) {
            Some(expr) => expr_to_u64(expr, NAME, "end row")?.min(row_count),
            None => row_count,
        };
        if start > row_count {
            return plan_err!(
                "{NAME} start row {start} is out of bounds for {filename} which has {row_count} rows"
            );
        }
        if end < start {
            return plan_err!(
                "{NAME} end row ({end}) must be greater than or equal to start row ({start})"
            );
        }

        let schema = Arc::new(
            input
                .session
                .arrow()
                .to_arrow_schema(input.file.dtype())
                .map_err(vx_err)?,
        );
        let mut batches: Vec<RecordBatch> = Vec::new();
        if end > start {
            let reader = input
                .file
                .scan()
                .map_err(vx_err)?
                .with_row_range(start..end)
                .into_record_batch_reader(Arc::clone(&schema), &input.runtime)
                .map_err(vx_err)?;
            for batch in reader {
                batches.push(batch?);
            }
        }
        Ok(Arc::new(MemTable::try_new(schema, vec![batches])?))
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::*;

    /// A row range returns exactly the rows in that range.
    #[tokio::test]
    async fn test_row_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!("SELECT id, category FROM vortex_rows('{path}', 8190, 8195)"),
        )
        .await;
        assert_eq!(
            column_i64(&batches, "id"),
            (8190..8195).map(Some).collect::<Vec<_>>()
        );
        assert_eq!(
            column_strings(&batches, "category")[0].as_deref(),
            Some("alpha")
        );
    }

    /// Boundary ranges: empty, the full file, a clamped end, and an open end.
    #[tokio::test]
    async fn test_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let count = |sql: String| {
            let ctx = &ctx;
            async move { num_rows(&run(ctx, &sql).await) }
        };
        assert_eq!(
            count(format!("SELECT * FROM vortex_rows('{path}', 5, 5)")).await,
            0
        );
        assert_eq!(
            count(format!("SELECT * FROM vortex_rows('{path}', 0, 20000)")).await,
            STANDARD_ROWS
        );
        assert_eq!(
            count(format!(
                "SELECT * FROM vortex_rows('{path}', 19990, 999999)"
            ))
            .await,
            10
        );
        assert_eq!(
            count(format!("SELECT * FROM vortex_rows('{path}', 19999)")).await,
            1
        );
        assert_eq!(
            count(format!("SELECT * FROM vortex_rows('{path}', 20000)")).await,
            0
        );
        // The nullable column keeps its nulls
        let batches = run(
            &ctx,
            &format!("SELECT value FROM vortex_rows('{path}', 0, 2)"),
        )
        .await;
        assert_eq!(
            column_strings(&batches, "value"),
            vec![None, Some("0.5".into())]
        );
    }

    /// Invalid ranges and arguments produce plan errors.
    #[tokio::test]
    async fn test_bad_arguments() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let err = run_err(&ctx, &format!("SELECT * FROM vortex_rows('{path}')")).await;
        assert!(err.contains("requires a filename, a start row"), "{err}");
        let err = run_err(&ctx, &format!("SELECT * FROM vortex_rows('{path}', 20001)")).await;
        assert!(err.contains("out of bounds"), "{err}");
        let err = run_err(&ctx, &format!("SELECT * FROM vortex_rows('{path}', 10, 5)")).await;
        assert!(err.contains("must be greater than or equal"), "{err}");
        let err = run_err(&ctx, &format!("SELECT * FROM vortex_rows('{path}', -1)")).await;
        assert!(err.contains("non-negative integer"), "{err}");
        let err = run_err(&ctx, &format!("SELECT * FROM vortex_rows('{path}', 'a')")).await;
        assert!(err.contains("non-negative integer"), "{err}");
    }
}

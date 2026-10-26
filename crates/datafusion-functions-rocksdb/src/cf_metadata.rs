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

//! `rocksdb_cf_metadata` table-valued function
//!
//! Exposes RocksDB's `GetColumnFamilyMetaData()` for every column family in
//! a database. An optional second argument filters to one column family.
//!
//! Example:
//! ```sql
//! SELECT * FROM rocksdb_cf_metadata('/path/to/db');
//! SELECT * FROM rocksdb_cf_metadata('/path/to/db', 'my_cf');
//! ```

use crate::{open_read_only, optional_cf_arg, path_arg, validate_cf_filter, BatchTable};
use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::sync::Arc;

#[derive(Debug)]
pub struct RocksDbCfMetadataFunc {}

impl TableFunctionImpl for RocksDbCfMetadataFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let path = path_arg("rocksdb_cf_metadata", exprs)?;
        let cf_filter = optional_cf_arg("rocksdb_cf_metadata", exprs)?;
        let (db, cfs) = open_read_only(path)?;
        validate_cf_filter(path, cf_filter.as_deref(), &cfs)?;

        let mut path_arr: Vec<Option<String>> = vec![];
        let mut cf_arr: Vec<Option<String>> = vec![];
        let mut size_arr: Vec<Option<u64>> = vec![];
        let mut file_count_arr: Vec<Option<u64>> = vec![];

        for cf_name in cfs
            .iter()
            .filter(|c| cf_filter.as_deref().is_none_or(|cf| cf == c.as_str()))
        {
            let Some(cf) = db.cf_handle(cf_name) else {
                continue;
            };
            let metadata = db.get_column_family_metadata_cf(cf);
            path_arr.push(Some(path.to_string()));
            cf_arr.push(Some(metadata.name));
            size_arr.push(Some(metadata.size));
            file_count_arr.push(Some(metadata.file_count as u64));
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("column_family", DataType::Utf8, false),
            Field::new("size_bytes", DataType::UInt64, false),
            Field::new("file_count", DataType::UInt64, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(path_arr)),
                Arc::new(StringArray::from(cf_arr)),
                Arc::new(UInt64Array::from(size_arr)),
                Arc::new(UInt64Array::from(file_count_arr)),
            ],
        )?;

        Ok(BatchTable::new(schema, batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::create_db;
    use datafusion::prelude::SessionContext;

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("rocksdb_cf_metadata", Arc::new(RocksDbCfMetadataFunc {}));
        ctx
    }

    fn u64_val(batch: &RecordBatch, row: usize, col: &str) -> u64 {
        batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(row)
    }

    #[tokio::test]
    async fn test_one_row_per_column_family() {
        let dir = tempfile::tempdir().unwrap();
        create_db(dir.path());

        let ctx = make_ctx();
        let sql = format!(
            "SELECT column_family, size_bytes, file_count FROM rocksdb_cf_metadata('{}') \
             ORDER BY column_family",
            dir.path().display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(u64_val(&batch, 0, "file_count"), 1);
        assert_eq!(u64_val(&batch, 1, "file_count"), 1);
        assert!(u64_val(&batch, 0, "size_bytes") > 0);
        assert!(u64_val(&batch, 1, "size_bytes") > 0);
    }

    #[tokio::test]
    async fn test_cf_filter() {
        let dir = tempfile::tempdir().unwrap();
        create_db(dir.path());

        let ctx = make_ctx();
        let sql = format!(
            "SELECT column_family FROM rocksdb_cf_metadata('{}', 'metrics')",
            dir.path().display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        let column_family = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(column_family.value(0), "metrics");
    }

    #[tokio::test]
    async fn test_unknown_cf() {
        let dir = tempfile::tempdir().unwrap();
        create_db(dir.path());

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM rocksdb_cf_metadata('{}', 'nope')",
            dir.path().display()
        );
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(err.contains("unknown column family 'nope'"), "{err}");
    }
}

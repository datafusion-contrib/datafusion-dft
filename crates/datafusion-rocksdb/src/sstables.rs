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

//! `rocksdb_sstables` table-valued function
//!
//! Opens a RocksDB database read-only and returns one row per live SST file:
//! its column family, file name, LSM level, size, entry and deletion counts,
//! and key range. Keys are arbitrary bytes, so the key range is exposed both
//! as lossless hex and as lossy UTF-8. An optional second argument filters to
//! a single column family.
//!
//! Example:
//! ```sql
//! SELECT * FROM rocksdb_sstables('/path/to/db');
//! SELECT * FROM rocksdb_sstables('/path/to/db', 'my_cf');
//! ```

use crate::{open_read_only, optional_cf_arg, path_arg, to_hex, validate_cf_filter, BatchTable};
use arrow::array::{Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use std::sync::Arc;

#[derive(Debug)]
pub struct RocksDbSstablesFunc {}

impl TableFunctionImpl for RocksDbSstablesFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let path = path_arg("rocksdb_sstables", exprs)?;
        let cf_filter = optional_cf_arg("rocksdb_sstables", exprs)?;
        let (db, cfs) = open_read_only(path)?;
        validate_cf_filter(path, cf_filter.as_deref(), &cfs)?;

        let mut files = db.live_files().map_err(|e| {
            DataFusionError::Execution(format!(
                "failed to list live files of RocksDB database at {path}: {e}"
            ))
        })?;
        files.retain(|f| {
            cf_filter
                .as_deref()
                .is_none_or(|cf| f.column_family_name == cf)
        });
        files.sort_by(|a, b| {
            a.column_family_name
                .cmp(&b.column_family_name)
                .then_with(|| a.name.cmp(&b.name))
        });

        let schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, true),
            Field::new("column_family", DataType::Utf8, true),
            Field::new("file_name", DataType::Utf8, true),
            Field::new("level", DataType::Int32, true),
            Field::new("size_bytes", DataType::UInt64, true),
            Field::new("num_entries", DataType::UInt64, true),
            Field::new("num_deletions", DataType::UInt64, true),
            Field::new("start_key_hex", DataType::Utf8, true),
            Field::new("start_key_utf8", DataType::Utf8, true),
            Field::new("end_key_hex", DataType::Utf8, true),
            Field::new("end_key_utf8", DataType::Utf8, true),
        ]));

        let mut path_arr: Vec<Option<String>> = vec![];
        let mut cf_arr: Vec<Option<String>> = vec![];
        let mut file_name_arr: Vec<Option<String>> = vec![];
        let mut level_arr: Vec<Option<i32>> = vec![];
        let mut size_arr: Vec<Option<u64>> = vec![];
        let mut entries_arr: Vec<Option<u64>> = vec![];
        let mut deletions_arr: Vec<Option<u64>> = vec![];
        let mut start_hex_arr: Vec<Option<String>> = vec![];
        let mut start_utf8_arr: Vec<Option<String>> = vec![];
        let mut end_hex_arr: Vec<Option<String>> = vec![];
        let mut end_utf8_arr: Vec<Option<String>> = vec![];

        for file in &files {
            path_arr.push(Some(path.to_string()));
            cf_arr.push(Some(file.column_family_name.clone()));
            file_name_arr.push(Some(file.name.clone()));
            level_arr.push(Some(file.level));
            size_arr.push(Some(file.size as u64));
            entries_arr.push(Some(file.num_entries));
            deletions_arr.push(Some(file.num_deletions));
            start_hex_arr.push(file.start_key.as_deref().map(to_hex));
            start_utf8_arr.push(
                file.start_key
                    .as_deref()
                    .map(|k| String::from_utf8_lossy(k).into_owned()),
            );
            end_hex_arr.push(file.end_key.as_deref().map(to_hex));
            end_utf8_arr.push(
                file.end_key
                    .as_deref()
                    .map(|k| String::from_utf8_lossy(k).into_owned()),
            );
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(path_arr)),
                Arc::new(StringArray::from(cf_arr)),
                Arc::new(StringArray::from(file_name_arr)),
                Arc::new(Int32Array::from(level_arr)),
                Arc::new(UInt64Array::from(size_arr)),
                Arc::new(UInt64Array::from(entries_arr)),
                Arc::new(UInt64Array::from(deletions_arr)),
                Arc::new(StringArray::from(start_hex_arr)),
                Arc::new(StringArray::from(start_utf8_arr)),
                Arc::new(StringArray::from(end_hex_arr)),
                Arc::new(StringArray::from(end_utf8_arr)),
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
        ctx.register_udtf("rocksdb_sstables", Arc::new(RocksDbSstablesFunc {}));
        ctx
    }

    fn str_val(batch: &RecordBatch, row: usize, col: &str) -> Option<String> {
        let array = batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        arrow::array::Array::is_valid(array, row).then(|| array.value(row).to_string())
    }

    fn u64_val(batch: &RecordBatch, row: usize, col: &str) -> Option<u64> {
        let array = batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        arrow::array::Array::is_valid(array, row).then(|| array.value(row))
    }

    /// One row per live SST file with level, counts, and key range in both
    /// hex and lossy UTF-8, ordered by column family then file name.
    #[tokio::test]
    async fn test_one_row_per_sst_file() {
        let dir = tempfile::tempdir().unwrap();
        create_db(dir.path());

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM rocksdb_sstables('{}')", dir.path().display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        assert_eq!(batch.num_rows(), 2);

        // The default column family holds keys "alpha" and 0xfffe00.
        assert_eq!(
            str_val(&batch, 0, "column_family").as_deref(),
            Some("default")
        );
        assert_eq!(u64_val(&batch, 0, "num_entries"), Some(2));
        assert_eq!(u64_val(&batch, 0, "num_deletions"), Some(0));
        assert_eq!(
            str_val(&batch, 0, "start_key_utf8").as_deref(),
            Some("alpha")
        );
        assert_eq!(
            str_val(&batch, 0, "start_key_hex").as_deref(),
            Some("616c706861")
        );
        assert_eq!(str_val(&batch, 0, "end_key_hex").as_deref(), Some("fffe00"));

        let level = batch
            .column(batch.schema().index_of("level").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(level.value(0), 0);

        assert_eq!(
            str_val(&batch, 1, "column_family").as_deref(),
            Some("metrics")
        );
        assert_eq!(u64_val(&batch, 1, "num_entries"), Some(2));
        assert_eq!(str_val(&batch, 1, "start_key_utf8").as_deref(), Some("m1"));
        assert_eq!(str_val(&batch, 1, "end_key_utf8").as_deref(), Some("m2"));
        assert!(str_val(&batch, 1, "file_name").unwrap().ends_with(".sst"));
        assert!(u64_val(&batch, 1, "size_bytes").unwrap() > 0);
    }

    /// The optional second argument filters to one column family.
    #[tokio::test]
    async fn test_cf_filter() {
        let dir = tempfile::tempdir().unwrap();
        create_db(dir.path());

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM rocksdb_sstables('{}', 'metrics')",
            dir.path().display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            str_val(&batch, 0, "column_family").as_deref(),
            Some("metrics")
        );
    }

    /// An unknown column family produces a plan error listing the available
    /// column families.
    #[tokio::test]
    async fn test_unknown_cf() {
        let dir = tempfile::tempdir().unwrap();
        create_db(dir.path());

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM rocksdb_sstables('{}', 'nope')",
            dir.path().display()
        );
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(
            err.contains("unknown column family 'nope'") && err.contains("metrics"),
            "{err}"
        );
    }
}

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

//! `rocksdb_metadata` table-valued function
//!
//! Opens a RocksDB database read-only and returns a single summary row of
//! database-level metadata: column families, sequence number, live SST file
//! counts and sizes, estimated key count, and MANIFEST / WAL file details.
//!
//! Example:
//! ```sql
//! SELECT * FROM rocksdb_metadata('/path/to/db');
//! ```

use crate::{open_read_only, path_arg, BatchTable};
use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug)]
pub struct RocksDbMetadataFunc {}

impl TableFunctionImpl for RocksDbMetadataFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let path = path_arg("rocksdb_metadata", exprs)?;
        let (db, cfs) = open_read_only(path)?;

        let live_files = db.live_files().map_err(|e| {
            DataFusionError::Execution(format!(
                "failed to list live files of RocksDB database at {path}: {e}"
            ))
        })?;
        let num_live_sst_files = live_files.len() as u64;
        let total_live_sst_size: u64 = live_files.iter().map(|f| f.size as u64).sum();

        let mut estimated_num_keys: Option<u64> = None;
        let mut num_snapshots: Option<u64> = None;
        for cf_name in &cfs {
            if let Some(cf) = db.cf_handle(cf_name) {
                if let Ok(Some(v)) = db.property_int_value_cf(cf, "rocksdb.estimate-num-keys") {
                    *estimated_num_keys.get_or_insert(0) += v;
                }
            }
        }
        if let Ok(v) = db.property_int_value("rocksdb.num-snapshots") {
            num_snapshots = v;
        }

        // The Rust binding exposes no MANIFEST or WAL APIs, so these come
        // from a best-effort filesystem scan of the database directory.
        let db_dir = Path::new(path);
        let manifest_file = std::fs::read_to_string(db_dir.join("CURRENT"))
            .ok()
            .map(|s| s.trim().to_string());
        let manifest_size = manifest_file
            .as_ref()
            .and_then(|m| std::fs::metadata(db_dir.join(m)).ok())
            .map(|m| m.len());
        let (wal_files, wal_size) = match std::fs::read_dir(db_dir) {
            Ok(entries) => {
                let (mut count, mut size) = (0u64, 0u64);
                for entry in entries.flatten() {
                    if entry.path().extension().is_some_and(|e| e == "log") {
                        count += 1;
                        size += entry.metadata().map(|m| m.len()).unwrap_or(0);
                    }
                }
                (Some(count), Some(size))
            }
            Err(_) => (None, None),
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, true),
            Field::new("column_families", DataType::Utf8, true),
            Field::new("num_column_families", DataType::UInt64, true),
            Field::new("latest_sequence_number", DataType::UInt64, true),
            Field::new("num_live_sst_files", DataType::UInt64, true),
            Field::new("total_live_sst_size", DataType::UInt64, true),
            Field::new("estimated_num_keys", DataType::UInt64, true),
            Field::new("num_snapshots", DataType::UInt64, true),
            Field::new("manifest_file", DataType::Utf8, true),
            Field::new("manifest_size", DataType::UInt64, true),
            Field::new("wal_files", DataType::UInt64, true),
            Field::new("wal_size", DataType::UInt64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some(path.to_string())])),
                Arc::new(StringArray::from(vec![Some(cfs.join(", "))])),
                Arc::new(UInt64Array::from(vec![Some(cfs.len() as u64)])),
                Arc::new(UInt64Array::from(vec![Some(db.latest_sequence_number())])),
                Arc::new(UInt64Array::from(vec![Some(num_live_sst_files)])),
                Arc::new(UInt64Array::from(vec![Some(total_live_sst_size)])),
                Arc::new(UInt64Array::from(vec![estimated_num_keys])),
                Arc::new(UInt64Array::from(vec![num_snapshots])),
                Arc::new(StringArray::from(vec![manifest_file])),
                Arc::new(UInt64Array::from(vec![manifest_size])),
                Arc::new(UInt64Array::from(vec![wal_files])),
                Arc::new(UInt64Array::from(vec![wal_size])),
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
        ctx.register_udtf("rocksdb_metadata", Arc::new(RocksDbMetadataFunc {}));
        ctx
    }

    fn u64_val(batch: &RecordBatch, row: usize, col: &str) -> Option<u64> {
        let array = batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        arrow::array::Array::is_valid(array, row).then(|| array.value(row))
    }

    fn str_val(batch: &RecordBatch, row: usize, col: &str) -> Option<String> {
        let array = batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        arrow::array::Array::is_valid(array, row).then(|| array.value(row).to_string())
    }

    /// A single summary row with column family, key, and file details.
    #[tokio::test]
    async fn test_metadata_summary() {
        let dir = tempfile::tempdir().unwrap();
        create_db(dir.path());

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM rocksdb_metadata('{}')", dir.path().display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        assert_eq!(batch.num_rows(), 1);

        assert_eq!(
            str_val(&batch, 0, "column_families").as_deref(),
            Some("default, metrics")
        );
        assert_eq!(u64_val(&batch, 0, "num_column_families"), Some(2));
        assert_eq!(u64_val(&batch, 0, "latest_sequence_number"), Some(4));
        assert_eq!(u64_val(&batch, 0, "num_live_sst_files"), Some(2));
        assert_eq!(u64_val(&batch, 0, "estimated_num_keys"), Some(4));
        assert_eq!(u64_val(&batch, 0, "num_snapshots"), Some(0));
        assert!(u64_val(&batch, 0, "total_live_sst_size").unwrap() > 0);
        assert!(str_val(&batch, 0, "manifest_file")
            .unwrap()
            .starts_with("MANIFEST-"));
        assert!(u64_val(&batch, 0, "manifest_size").unwrap() > 0);
        assert!(u64_val(&batch, 0, "wal_files").is_some());
        assert_eq!(
            str_val(&batch, 0, "path").as_deref(),
            Some(dir.path().display().to_string().as_str())
        );
    }

    /// Missing argument produces a plan error.
    #[tokio::test]
    async fn test_missing_argument() {
        let ctx = make_ctx();
        let err = ctx.sql("SELECT * FROM rocksdb_metadata()").await;
        assert!(err.is_err());
    }

    /// A nonexistent path produces an error naming the path.
    #[tokio::test]
    async fn test_nonexistent_path() {
        let ctx = make_ctx();
        let sql = "SELECT * FROM rocksdb_metadata('/nonexistent/rocksdb/path')";
        let err = ctx.sql(sql).await.unwrap_err().to_string();
        assert!(err.contains("/nonexistent/rocksdb/path"), "{err}");
    }
}

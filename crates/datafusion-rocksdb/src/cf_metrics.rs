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

//! `rocksdb_cf_metrics` table-valued function
//!
//! Opens a RocksDB database read-only and returns one row per column family
//! and RocksDB property (long format: `column_family`, `property`, `value`),
//! covering key counts, SST and memtable sizes, compaction state, block
//! cache usage, and per-level file counts. An optional second argument
//! filters to a single column family.
//!
//! Because the database is opened read-only, memtable and compaction
//! counters mostly read 0; the on-disk footprint and level distribution
//! properties are the meaningful ones.
//!
//! Example:
//! ```sql
//! SELECT * FROM rocksdb_cf_metrics('/path/to/db');
//! SELECT * FROM rocksdb_cf_metrics('/path/to/db', 'my_cf');
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

/// Integer-valued per-column-family properties reported by the function.
/// Properties a RocksDB version does not recognize are silently skipped, so
/// this list can grow without breaking existing queries.
const CF_PROPERTIES: &[&str] = &[
    "rocksdb.estimate-num-keys",
    "rocksdb.estimate-live-data-size",
    "rocksdb.total-sst-files-size",
    "rocksdb.live-sst-files-size",
    "rocksdb.size-all-mem-tables",
    "rocksdb.cur-size-all-mem-tables",
    "rocksdb.num-entries-active-mem-table",
    "rocksdb.num-entries-imm-mem-tables",
    "rocksdb.num-immutable-mem-table",
    "rocksdb.estimate-table-readers-mem",
    "rocksdb.estimate-pending-compaction-bytes",
    "rocksdb.num-running-compactions",
    "rocksdb.num-running-flushes",
    "rocksdb.compaction-pending",
    "rocksdb.mem-table-flush-pending",
    "rocksdb.background-errors",
    "rocksdb.num-snapshots",
    "rocksdb.block-cache-capacity",
    "rocksdb.block-cache-usage",
    "rocksdb.block-cache-pinned-usage",
];

/// Number of LSM levels probed with `rocksdb.num-files-at-level<N>`.
const NUM_LEVELS: usize = 7;

#[derive(Debug)]
pub struct RocksDbCfMetricsFunc {}

impl TableFunctionImpl for RocksDbCfMetricsFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let path = path_arg("rocksdb_cf_metrics", exprs)?;
        let cf_filter = optional_cf_arg("rocksdb_cf_metrics", exprs)?;
        let (db, cfs) = open_read_only(path)?;
        validate_cf_filter(path, cf_filter.as_deref(), &cfs)?;

        let mut path_arr: Vec<Option<String>> = vec![];
        let mut cf_arr: Vec<Option<String>> = vec![];
        let mut property_arr: Vec<Option<String>> = vec![];
        let mut value_arr: Vec<Option<u64>> = vec![];

        let mut push = |cf_name: &str, property: &str, value: u64| {
            path_arr.push(Some(path.to_string()));
            cf_arr.push(Some(cf_name.to_string()));
            property_arr.push(Some(property.to_string()));
            value_arr.push(Some(value));
        };

        for cf_name in cfs
            .iter()
            .filter(|c| cf_filter.as_deref().is_none_or(|cf| cf == c.as_str()))
        {
            let Some(cf) = db.cf_handle(cf_name) else {
                continue;
            };
            for property in CF_PROPERTIES {
                if let Ok(Some(value)) = db.property_int_value_cf(cf, *property) {
                    push(cf_name, property, value);
                }
            }
            for level in 0..NUM_LEVELS {
                let property = format!("rocksdb.num-files-at-level{level}");
                if let Ok(Some(value)) = db.property_int_value_cf(cf, property.as_str()) {
                    push(cf_name, &property, value);
                }
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, true),
            Field::new("column_family", DataType::Utf8, true),
            Field::new("property", DataType::Utf8, true),
            Field::new("value", DataType::UInt64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(path_arr)),
                Arc::new(StringArray::from(cf_arr)),
                Arc::new(StringArray::from(property_arr)),
                Arc::new(UInt64Array::from(value_arr)),
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
        ctx.register_udtf("rocksdb_cf_metrics", Arc::new(RocksDbCfMetricsFunc {}));
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

    /// Known properties are reported per column family with correct values.
    #[tokio::test]
    async fn test_property_values() {
        let dir = tempfile::tempdir().unwrap();
        create_db(dir.path());

        let ctx = make_ctx();
        let sql = format!(
            "SELECT value FROM rocksdb_cf_metrics('{}') \
             WHERE column_family = 'metrics' AND property = 'rocksdb.estimate-num-keys'",
            dir.path().display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(u64_val(&batch, 0, "value"), Some(2));

        let sql = format!(
            "SELECT value FROM rocksdb_cf_metrics('{}') \
             WHERE column_family = 'default' AND property = 'rocksdb.num-files-at-level0'",
            dir.path().display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(u64_val(&batch, 0, "value"), Some(1));
    }

    /// Every column family is present, and the optional second argument
    /// filters to one.
    #[tokio::test]
    async fn test_cf_filter() {
        let dir = tempfile::tempdir().unwrap();
        create_db(dir.path());

        let ctx = make_ctx();
        let sql = format!(
            "SELECT DISTINCT column_family FROM rocksdb_cf_metrics('{}') ORDER BY column_family",
            dir.path().display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let sql = format!(
            "SELECT DISTINCT column_family FROM rocksdb_cf_metrics('{}', 'default')",
            dir.path().display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    /// An unknown column family produces a plan error.
    #[tokio::test]
    async fn test_unknown_cf() {
        let dir = tempfile::tempdir().unwrap();
        create_db(dir.path());

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM rocksdb_cf_metrics('{}', 'nope')",
            dir.path().display()
        );
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(err.contains("unknown column family 'nope'"), "{err}");
    }
}

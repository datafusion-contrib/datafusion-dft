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

mod cf_metrics;
mod metadata;
mod sstables;

pub use cf_metrics::RocksDbCfMetricsFunc;
pub use metadata::RocksDbMetadataFunc;
pub use sstables::RocksDbSstablesFunc;

// Re-exported so downstream test code can create fixture databases without
// depending on `rocksdb` directly (and without pinning a second version).
pub use rocksdb;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{plan_err, Column};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use rocksdb::{Options, DB};
use std::sync::Arc;

/// A single in-memory [`RecordBatch`] exposed as a [`TableProvider`].
#[derive(Debug)]
struct BatchTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

impl BatchTable {
    fn new(schema: SchemaRef, batch: RecordBatch) -> Arc<Self> {
        Arc::new(Self { schema, batch })
    }
}

#[async_trait]
impl TableProvider for BatchTable {
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
            &[vec![self.batch.clone()]],
            TableProvider::schema(self),
            projection.cloned(),
        )?)
    }
}

/// Extract the database path argument shared by all functions in this crate.
fn path_arg<'a>(func_name: &str, exprs: &'a [Expr]) -> Result<&'a str> {
    match exprs.first() {
        Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => Ok(s),
        Some(Expr::Column(Column { name, .. })) => Ok(name),
        _ => plan_err!("{func_name} requires a string database path as its first argument"),
    }
}

/// Extract the optional column family name argument.
fn optional_cf_arg(func_name: &str, exprs: &[Expr]) -> Result<Option<String>> {
    match exprs.get(1) {
        None => Ok(None),
        Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => Ok(Some(s.clone())),
        Some(Expr::Column(Column { name, .. })) => Ok(Some(name.clone())),
        _ => plan_err!(
            "{func_name} requires a string column family name as its optional second argument"
        ),
    }
}

/// Open a RocksDB database read-only with all of its column families.
///
/// Listing column families first (which only reads the MANIFEST) lets the
/// read-only open include every column family, including non-default ones.
/// A read-only open never takes the database LOCK, so this works while
/// another process has the database open read-write. Note that data still in
/// the WAL (not yet flushed to SST files) is not visible to a read-only
/// handle.
fn open_read_only(path: &str) -> Result<(DB, Vec<String>)> {
    let opts = Options::default();
    let cfs = DB::list_cf(&opts, path).map_err(|e| {
        DataFusionError::Execution(format!(
            "failed to list column families of RocksDB database at {path}: {e}"
        ))
    })?;
    let db = DB::open_cf_for_read_only(&opts, path, &cfs, false).map_err(|e| {
        DataFusionError::Execution(format!(
            "failed to open RocksDB database at {path} read-only: {e}"
        ))
    })?;
    Ok((db, cfs))
}

/// Validate an optional column family filter against the database's column
/// families.
fn validate_cf_filter(path: &str, cf_filter: Option<&str>, cfs: &[String]) -> Result<()> {
    if let Some(cf) = cf_filter {
        if !cfs.iter().any(|c| c == cf) {
            return plan_err!("unknown column family '{cf}' in {path}; available: {cfs:?}");
        }
    }
    Ok(())
}

/// Render arbitrary key bytes as a lowercase hex string.
fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
pub(crate) mod test_util {
    use rocksdb::{Options, DB};
    use std::path::Path;

    /// Create a small database with a `default` and a `metrics` column
    /// family, two flushed keys in each (one of them a non-UTF8 binary key),
    /// so `live_files()` returns one SST file per column family.
    pub(crate) fn create_db(path: &Path) {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = DB::open_cf(&opts, path, ["default", "metrics"]).unwrap();
        db.put(b"alpha", b"1").unwrap();
        db.put([0xff, 0xfe, 0x00], b"2").unwrap();
        let cf = db.cf_handle("metrics").unwrap();
        db.put_cf(cf, b"m1", b"10").unwrap();
        db.put_cf(cf, b"m2", b"20").unwrap();
        db.flush().unwrap();
        db.flush_cf(cf).unwrap();
    }
}

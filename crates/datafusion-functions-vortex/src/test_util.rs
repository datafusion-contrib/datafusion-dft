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

//! Test helpers that write Vortex files with known contents

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use std::path::Path;
use std::sync::Arc;
use vortex::arrow::ArrowSessionExt;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;

/// Number of rows in [`standard_batch`]. The default zone length is 8192, so
/// each column has three zones: 8192, 8192, and 3616 rows.
pub const STANDARD_ROWS: usize = 20_000;
pub const CATEGORIES: [&str; 3] = ["alpha", "beta", "gamma"];

/// A batch with a sorted `id` column, a low-cardinality `category` column,
/// and a nullable `value` column with a null in every seventh row.
pub fn standard_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ]));
    let ids: Vec<i64> = (0..STANDARD_ROWS as i64).collect();
    let categories: Vec<&str> = (0..STANDARD_ROWS).map(|i| CATEGORIES[i % 3]).collect();
    let values: Vec<Option<f64>> = (0..STANDARD_ROWS)
        .map(|i| (i % 7 != 0).then_some(i as f64 * 0.5))
        .collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(categories)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

/// Write `batch` to `path` with the default Vortex write strategy and the
/// given user-defined metadata segments
pub fn write_vortex_with_metadata(path: &Path, batch: RecordBatch, metadata: &[(&str, &[u8])]) {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let schema = batch.schema();
    let array = session
        .arrow()
        .from_arrow_record_batch(batch, &schema)
        .unwrap();
    let mut options = session.write_options();
    for (key, value) in metadata {
        options = options.with_metadata_segment(*key, value.to_vec());
    }
    let file = std::fs::File::create(path).unwrap();
    options
        .blocking(&runtime)
        .write(file, array.to_array_iterator())
        .unwrap();
}

/// Write `batch` to `path` with the default Vortex write strategy
pub fn write_vortex(path: &Path, batch: RecordBatch) {
    write_vortex_with_metadata(path, batch, &[]);
}

/// Write [`standard_batch`] to `name` in `dir` and return the path as a string
pub fn write_standard(dir: &Path, name: &str) -> String {
    let path = dir.join(name);
    write_vortex(&path, standard_batch());
    path.display().to_string()
}

/// A session with every function in this crate registered
pub fn make_ctx() -> SessionContext {
    let ctx = SessionContext::new();
    crate::register_all(&ctx);
    ctx
}

/// Run `sql` and return the batches
pub async fn run(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

/// Run `sql` and return the error message
pub async fn run_err(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(e) => e.to_string(),
        Ok(df) => df.collect().await.unwrap_err().to_string(),
    }
}

/// Total row count over `batches`
pub fn num_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Concatenate column `col` of every batch, formatted as strings
pub fn column_strings(batches: &[RecordBatch], col: &str) -> Vec<Option<String>> {
    use arrow::array::Array;
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    let mut out = Vec::new();
    for batch in batches {
        let array = batch.column(batch.schema().index_of(col).unwrap());
        let options = FormatOptions::default();
        let formatter = ArrayFormatter::try_new(array.as_ref(), &options).unwrap();
        for i in 0..array.len() {
            out.push((!array.is_null(i)).then(|| formatter.value(i).to_string()));
        }
    }
    out
}

/// Concatenate column `col` of every batch as `i64` values
pub fn column_i64(batches: &[RecordBatch], col: &str) -> Vec<Option<i64>> {
    column_strings(batches, col)
        .into_iter()
        .map(|v| v.map(|s| s.parse().unwrap()))
        .collect()
}

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

use arrow::array::{BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use std::fmt::Display;
use std::fs::File;
use std::sync::Arc;

#[derive(Debug)]
struct ParquetPageIndexTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for ParquetPageIndexTable {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
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

fn page_min_value(index: &ColumnIndexMetaData, page_idx: usize) -> Option<String> {
    if index.is_null_page(page_idx) {
        return None;
    }
    match index {
        ColumnIndexMetaData::NONE => None,
        ColumnIndexMetaData::BOOLEAN(idx) => fmt_val(idx.min_value(page_idx)),
        ColumnIndexMetaData::INT32(idx) => fmt_val(idx.min_value(page_idx)),
        ColumnIndexMetaData::INT64(idx) => fmt_val(idx.min_value(page_idx)),
        ColumnIndexMetaData::INT96(idx) => fmt_val(idx.min_value(page_idx)),
        ColumnIndexMetaData::FLOAT(idx) => fmt_val(idx.min_value(page_idx)),
        ColumnIndexMetaData::DOUBLE(idx) => fmt_val(idx.min_value(page_idx)),
        ColumnIndexMetaData::BYTE_ARRAY(idx) => idx.min_value(page_idx).map(bytes_to_string),
        ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(idx) => {
            idx.min_value(page_idx).map(bytes_to_string)
        }
    }
}

fn page_max_value(index: &ColumnIndexMetaData, page_idx: usize) -> Option<String> {
    if index.is_null_page(page_idx) {
        return None;
    }
    match index {
        ColumnIndexMetaData::NONE => None,
        ColumnIndexMetaData::BOOLEAN(idx) => fmt_val(idx.max_value(page_idx)),
        ColumnIndexMetaData::INT32(idx) => fmt_val(idx.max_value(page_idx)),
        ColumnIndexMetaData::INT64(idx) => fmt_val(idx.max_value(page_idx)),
        ColumnIndexMetaData::INT96(idx) => fmt_val(idx.max_value(page_idx)),
        ColumnIndexMetaData::FLOAT(idx) => fmt_val(idx.max_value(page_idx)),
        ColumnIndexMetaData::DOUBLE(idx) => fmt_val(idx.max_value(page_idx)),
        ColumnIndexMetaData::BYTE_ARRAY(idx) => idx.max_value(page_idx).map(bytes_to_string),
        ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(idx) => {
            idx.max_value(page_idx).map(bytes_to_string)
        }
    }
}

fn fmt_val<T: Display>(v: Option<&T>) -> Option<String> {
    v.map(|v| v.to_string())
}

fn bytes_to_string(bytes: &[u8]) -> String {
    std::str::from_utf8(bytes)
        .map(|s| s.to_string())
        .unwrap_or_else(|_| format!("{bytes:?}"))
}

/// `parquet_page_index` table-valued function
///
/// Returns page-level statistics from the Parquet page index (column index +
/// offset index). Each row represents one data page within a column chunk.
///
/// Example:
/// ```sql
/// SELECT * FROM parquet_page_index('file.parquet');
/// ```
#[derive(Debug)]
pub struct ParquetPageIndexFunc {}

impl TableFunctionImpl for ParquetPageIndexFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = match exprs.first() {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => s,
            Some(Expr::Column(Column { name, .. })) => name,
            _ => {
                return plan_err!("parquet_page_index requires a string argument as its input");
            }
        };

        let file = File::open(filename.clone())?;
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Optional)
            .parse_and_finish(&file)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("row_group_id", DataType::Int64, true),
            Field::new("row_group_num_rows", DataType::Int64, true),
            Field::new("column_id", DataType::Int64, true),
            Field::new("page_id", DataType::Int64, true),
            Field::new("path_in_schema", DataType::Utf8, true),
            Field::new("null_count", DataType::Int64, true),
            Field::new("null_page", DataType::Boolean, true),
            Field::new("min", DataType::Utf8, true),
            Field::new("max", DataType::Utf8, true),
            Field::new("offset", DataType::Int64, true),
            Field::new("compressed_page_size", DataType::Int32, true),
            Field::new("first_row_index", DataType::Int64, true),
        ]));

        let mut filename_arr: Vec<Option<String>> = vec![];
        let mut row_group_id_arr: Vec<Option<i64>> = vec![];
        let mut row_group_num_rows_arr: Vec<Option<i64>> = vec![];
        let mut column_id_arr: Vec<Option<i64>> = vec![];
        let mut page_id_arr: Vec<Option<i64>> = vec![];
        let mut path_in_schema_arr: Vec<Option<String>> = vec![];
        let mut null_count_arr: Vec<Option<i64>> = vec![];
        let mut null_page_arr: Vec<Option<bool>> = vec![];
        let mut min_arr: Vec<Option<String>> = vec![];
        let mut max_arr: Vec<Option<String>> = vec![];
        let mut offset_arr: Vec<Option<i64>> = vec![];
        let mut compressed_page_size_arr: Vec<Option<i32>> = vec![];
        let mut first_row_index_arr: Vec<Option<i64>> = vec![];

        let column_indexes = metadata.column_index();
        let offset_indexes = metadata.offset_index();

        for (rg_idx, row_group) in metadata.row_groups().iter().enumerate() {
            let rg_num_rows = row_group.num_rows();

            for (col_idx, column) in row_group.columns().iter().enumerate() {
                let path = column.column_path().string();

                let col_index = column_indexes
                    .and_then(|ci| ci.get(rg_idx))
                    .and_then(|rg| rg.get(col_idx));

                let offset_index = offset_indexes
                    .and_then(|oi| oi.get(rg_idx))
                    .and_then(|rg| rg.get(col_idx));

                // Determine page count from whichever index is available
                let num_pages = match (col_index, offset_index) {
                    (_, Some(oi)) => oi.page_locations().len(),
                    (Some(ci), _) if !matches!(ci, ColumnIndexMetaData::NONE) => {
                        ci.num_pages() as usize
                    }
                    _ => continue,
                };

                for page_idx in 0..num_pages {
                    filename_arr.push(Some(filename.clone()));
                    row_group_id_arr.push(Some(rg_idx as i64));
                    row_group_num_rows_arr.push(Some(rg_num_rows));
                    column_id_arr.push(Some(col_idx as i64));
                    page_id_arr.push(Some(page_idx as i64));
                    path_in_schema_arr.push(Some(path.clone()));

                    match col_index {
                        Some(ci) if !matches!(ci, ColumnIndexMetaData::NONE) => {
                            null_count_arr.push(ci.null_count(page_idx));
                            null_page_arr.push(Some(ci.is_null_page(page_idx)));
                            min_arr.push(page_min_value(ci, page_idx));
                            max_arr.push(page_max_value(ci, page_idx));
                        }
                        _ => {
                            null_count_arr.push(None);
                            null_page_arr.push(None);
                            min_arr.push(None);
                            max_arr.push(None);
                        }
                    }

                    match offset_index.and_then(|oi| oi.page_locations().get(page_idx)) {
                        Some(loc) => {
                            offset_arr.push(Some(loc.offset));
                            compressed_page_size_arr.push(Some(loc.compressed_page_size));
                            first_row_index_arr.push(Some(loc.first_row_index));
                        }
                        None => {
                            offset_arr.push(None);
                            compressed_page_size_arr.push(None);
                            first_row_index_arr.push(None);
                        }
                    }
                }
            }
        }

        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(filename_arr)),
                Arc::new(Int64Array::from(row_group_id_arr)),
                Arc::new(Int64Array::from(row_group_num_rows_arr)),
                Arc::new(Int64Array::from(column_id_arr)),
                Arc::new(Int64Array::from(page_id_arr)),
                Arc::new(StringArray::from(path_in_schema_arr)),
                Arc::new(Int64Array::from(null_count_arr)),
                Arc::new(BooleanArray::from(null_page_arr)),
                Arc::new(StringArray::from(min_arr)),
                Arc::new(StringArray::from(max_arr)),
                Arc::new(Int64Array::from(offset_arr)),
                Arc::new(Int32Array::from(compressed_page_size_arr)),
                Arc::new(Int64Array::from(first_row_index_arr)),
            ],
        )?;

        Ok(Arc::new(ParquetPageIndexTable { schema, batch: rb }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::path::Path;

    /// Write `batches` to a parquet file at `path` using `props`.
    fn write_parquet(path: &Path, batches: &[RecordBatch], props: Option<WriterProperties>) {
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batches[0].schema(), props).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
    }

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("parquet_page_index", Arc::new(ParquetPageIndexFunc {}));
        ctx
    }

    /// Return the single string value at `(row, col)` from a flat result set.
    fn str_val(batches: &[RecordBatch], row: usize, col: &str) -> Option<String> {
        let mut offset = 0;
        for batch in batches {
            if row < offset + batch.num_rows() {
                let local = row - offset;
                let array = batch
                    .column(batch.schema().index_of(col).unwrap())
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                return array
                    .is_valid(local)
                    .then(|| array.value(local).to_string());
            }
            offset += batch.num_rows();
        }
        panic!("row {row} out of range");
    }

    fn i64_val(batches: &[RecordBatch], row: usize, col: &str) -> Option<i64> {
        use arrow::array::Int64Array;
        let mut offset = 0;
        for batch in batches {
            if row < offset + batch.num_rows() {
                let local = row - offset;
                let array = batch
                    .column(batch.schema().index_of(col).unwrap())
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                return array.is_valid(local).then(|| array.value(local));
            }
            offset += batch.num_rows();
        }
        panic!("row {row} out of range");
    }

    fn bool_val(batches: &[RecordBatch], row: usize, col: &str) -> Option<bool> {
        use arrow::array::BooleanArray;
        let mut offset = 0;
        for batch in batches {
            if row < offset + batch.num_rows() {
                let local = row - offset;
                let array = batch
                    .column(batch.schema().index_of(col).unwrap())
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                return array.is_valid(local).then(|| array.value(local));
            }
            offset += batch.num_rows();
        }
        panic!("row {row} out of range");
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    // --- tests ---

    /// Basic schema: result has the expected column names.
    #[tokio::test]
    async fn test_schema_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1i32, 2, 3]))])
                .unwrap();
        write_parquet(&path, &[batch], None);

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_page_index('{}')", path.display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert!(!result.is_empty());

        let s = result[0].schema();
        for col in &[
            "filename",
            "row_group_id",
            "row_group_num_rows",
            "column_id",
            "page_id",
            "path_in_schema",
            "null_count",
            "null_page",
            "min",
            "max",
            "offset",
            "compressed_page_size",
            "first_row_index",
        ] {
            assert!(s.field_with_name(col).is_ok(), "missing column: {col}");
        }
    }

    /// Integer column: min/max match the actual data.
    #[tokio::test]
    async fn test_int_min_max() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![10i32, 20, 30, 5, 50]))],
        )
        .unwrap();
        write_parquet(&path, &[batch], None);

        let ctx = make_ctx();
        let sql = format!(
            "SELECT min, max FROM parquet_page_index('{}') \
             WHERE page_id = 0 AND column_id = 0",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 1);
        assert_eq!(str_val(&result, 0, "min").as_deref(), Some("5"));
        assert_eq!(str_val(&result, 0, "max").as_deref(), Some("50"));
    }

    /// String column: min/max match alphabetical order.
    #[tokio::test]
    async fn test_string_min_max() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                "banana", "apple", "cherry",
            ]))],
        )
        .unwrap();
        write_parquet(&path, &[batch], None);

        let ctx = make_ctx();
        let sql = format!(
            "SELECT min, max FROM parquet_page_index('{}') \
             WHERE page_id = 0 AND column_id = 0",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 1);
        assert_eq!(str_val(&result, 0, "min").as_deref(), Some("apple"));
        assert_eq!(str_val(&result, 0, "max").as_deref(), Some("cherry"));
    }

    /// A column with only null values produces null_page = true.
    #[tokio::test]
    async fn test_null_page() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![None::<i32>, None, None]))],
        )
        .unwrap();
        write_parquet(&path, &[batch], None);

        let ctx = make_ctx();
        let sql = format!(
            "SELECT null_page, null_count FROM parquet_page_index('{}') \
             WHERE page_id = 0 AND column_id = 0",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 1);
        assert_eq!(bool_val(&result, 0, "null_page"), Some(true));
        assert_eq!(i64_val(&result, 0, "null_count"), Some(3));
    }

    /// Multiple columns: one row per column per page.
    #[tokio::test]
    async fn test_multiple_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1i32, 2, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap();
        write_parquet(&path, &[batch], None);

        let ctx = make_ctx();
        // With 2 columns and 1 page each, expect 2 rows
        let sql = format!(
            "SELECT column_id, path_in_schema FROM parquet_page_index('{}') ORDER BY column_id",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 2);
        assert_eq!(i64_val(&result, 0, "column_id"), Some(0));
        assert_eq!(str_val(&result, 0, "path_in_schema").as_deref(), Some("a"));
        assert_eq!(i64_val(&result, 1, "column_id"), Some(1));
        assert_eq!(str_val(&result, 1, "path_in_schema").as_deref(), Some("b"));
    }

    /// Multiple row groups: row_group_id increments per group.
    #[tokio::test]
    async fn test_multiple_row_groups() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        // Force a new row group after every row
        let props = WriterProperties::builder()
            .set_max_row_group_size(2)
            .build();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1i32, 2, 3, 4]))],
        )
        .unwrap();
        write_parquet(&path, &[batch], Some(props));

        let ctx = make_ctx();
        let sql = format!(
            "SELECT DISTINCT row_group_id FROM parquet_page_index('{}') ORDER BY row_group_id",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        // 4 rows / 2 per group = 2 row groups
        assert_eq!(total_rows(&result), 2);
        assert_eq!(i64_val(&result, 0, "row_group_id"), Some(0));
        assert_eq!(i64_val(&result, 1, "row_group_id"), Some(1));
    }

    /// row_group_num_rows reflects the actual row count of each row group.
    #[tokio::test]
    async fn test_row_group_num_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let props = WriterProperties::builder()
            .set_max_row_group_size(3)
            .build();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1i32, 2, 3, 4, 5]))],
        )
        .unwrap();
        write_parquet(&path, &[batch], Some(props));

        let ctx = make_ctx();
        let sql = format!(
            "SELECT row_group_id, row_group_num_rows FROM parquet_page_index('{}') ORDER BY row_group_id",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        // row group 0: 3 rows, row group 1: 2 rows
        assert_eq!(i64_val(&result, 0, "row_group_num_rows"), Some(3));
        assert_eq!(i64_val(&result, 1, "row_group_num_rows"), Some(2));
    }

    /// Multiple pages within a column: page_id increments and
    /// first_row_index advances by the page row count.
    #[tokio::test]
    async fn test_multiple_pages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        // Tiny page limit forces one row per page
        let props = WriterProperties::builder()
            .set_data_page_size_limit(1)
            .set_write_batch_size(1)
            .build();
        let values: Vec<i32> = (0..5).collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap();
        write_parquet(&path, &[batch], Some(props));

        let ctx = make_ctx();
        let sql = format!(
            "SELECT page_id, first_row_index, min, max \
             FROM parquet_page_index('{}') \
             WHERE column_id = 0 ORDER BY page_id",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let n = total_rows(&result);
        assert!(n > 1, "expected multiple pages, got {n}");

        // page_id should increment monotonically starting from 0
        for i in 0..n {
            assert_eq!(i64_val(&result, i, "page_id"), Some(i as i64));
        }
        // first page starts at row 0
        assert_eq!(i64_val(&result, 0, "first_row_index"), Some(0));
        // min/max for each page is the single value written to it
        assert_eq!(str_val(&result, 0, "min"), str_val(&result, 0, "max"));
    }

    /// Offset index fields (offset, compressed_page_size, first_row_index)
    /// are present and non-null.
    #[tokio::test]
    async fn test_offset_index_fields_populated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1i32, 2, 3]))])
                .unwrap();
        write_parquet(&path, &[batch], None);

        let ctx = make_ctx();
        let sql = format!(
            "SELECT offset, compressed_page_size, first_row_index \
             FROM parquet_page_index('{}') WHERE page_id = 0",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 1);

        use arrow::array::Int32Array as I32A;
        let batch = &result[0];
        let offsets = batch
            .column(batch.schema().index_of("offset").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sizes = batch
            .column(batch.schema().index_of("compressed_page_size").unwrap())
            .as_any()
            .downcast_ref::<I32A>()
            .unwrap();
        let first_rows = batch
            .column(batch.schema().index_of("first_row_index").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert!(offsets.is_valid(0), "offset should be non-null");
        assert!(offsets.value(0) > 0, "offset should be positive");
        assert!(sizes.is_valid(0), "compressed_page_size should be non-null");
        assert!(
            sizes.value(0) > 0,
            "compressed_page_size should be positive"
        );
        assert!(first_rows.is_valid(0), "first_row_index should be non-null");
        assert_eq!(first_rows.value(0), 0, "first page starts at row 0");
    }
}

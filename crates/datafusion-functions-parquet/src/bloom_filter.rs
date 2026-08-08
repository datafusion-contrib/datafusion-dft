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

use crate::expr_to_string;
use arrow::array::{BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
use parquet::basic::Type as PhysicalType;
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::ParquetMetaDataReader;
use std::fs::File;
use std::sync::Arc;

/// Size in bytes of a single split block bloom filter block
const BLOCK_SIZE_BYTES: i64 = 32;

#[derive(Debug)]
struct ParquetBloomFilterTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for ParquetBloomFilterTable {
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

/// `parquet_bloom_filter` table-valued function
///
/// Returns bloom filter details for each column chunk in a Parquet file. Each
/// row represents one column within a row group and reports whether a bloom
/// filter is present along with its location and size.
///
/// Example:
/// ```sql
/// SELECT * FROM parquet_bloom_filter('file.parquet');
/// ```
#[derive(Debug)]
pub struct ParquetBloomFilterFunc {}

impl TableFunctionImpl for ParquetBloomFilterFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = expr_to_string(exprs.first(), "parquet_bloom_filter", "filename")?;

        let file = File::open(&filename)?;
        let metadata = ParquetMetaDataReader::new().parse_and_finish(&file)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("row_group_id", DataType::Int64, true),
            Field::new("column_id", DataType::Int64, true),
            Field::new("path_in_schema", DataType::Utf8, true),
            Field::new("has_bloom_filter", DataType::Boolean, true),
            Field::new("bloom_filter_offset", DataType::Int64, true),
            Field::new("bloom_filter_length", DataType::Int64, true),
            Field::new("num_blocks", DataType::Int64, true),
            Field::new("bitset_size_bytes", DataType::Int64, true),
        ]));

        let mut filename_arr: Vec<Option<String>> = vec![];
        let mut row_group_id_arr: Vec<Option<i64>> = vec![];
        let mut column_id_arr: Vec<Option<i64>> = vec![];
        let mut path_in_schema_arr: Vec<Option<String>> = vec![];
        let mut has_bloom_filter_arr: Vec<Option<bool>> = vec![];
        let mut bloom_filter_offset_arr: Vec<Option<i64>> = vec![];
        let mut bloom_filter_length_arr: Vec<Option<i64>> = vec![];
        let mut num_blocks_arr: Vec<Option<i64>> = vec![];
        let mut bitset_size_bytes_arr: Vec<Option<i64>> = vec![];

        for (rg_idx, row_group) in metadata.row_groups().iter().enumerate() {
            for (col_idx, column) in row_group.columns().iter().enumerate() {
                filename_arr.push(Some(filename.clone()));
                row_group_id_arr.push(Some(rg_idx as i64));
                column_id_arr.push(Some(col_idx as i64));
                path_in_schema_arr.push(Some(column.column_path().string()));
                has_bloom_filter_arr.push(Some(column.bloom_filter_offset().is_some()));
                bloom_filter_offset_arr.push(column.bloom_filter_offset());
                bloom_filter_length_arr.push(column.bloom_filter_length().map(|l| l as i64));

                let num_blocks = Sbbf::read_from_column_chunk(column, &file)?
                    .map(|sbbf| sbbf.num_blocks() as i64);
                num_blocks_arr.push(num_blocks);
                bitset_size_bytes_arr.push(num_blocks.map(|n| n * BLOCK_SIZE_BYTES));
            }
        }

        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(filename_arr)),
                Arc::new(Int64Array::from(row_group_id_arr)),
                Arc::new(Int64Array::from(column_id_arr)),
                Arc::new(StringArray::from(path_in_schema_arr)),
                Arc::new(BooleanArray::from(has_bloom_filter_arr)),
                Arc::new(Int64Array::from(bloom_filter_offset_arr)),
                Arc::new(Int64Array::from(bloom_filter_length_arr)),
                Arc::new(Int64Array::from(num_blocks_arr)),
                Arc::new(Int64Array::from(bitset_size_bytes_arr)),
            ],
        )?;

        Ok(Arc::new(ParquetBloomFilterTable { schema, batch: rb }))
    }
}

/// Probe `sbbf` for `value`, converting the string to the column's physical
/// type first so the hashed bytes match what the writer inserted.
fn check_value(sbbf: &Sbbf, physical_type: PhysicalType, value: &str) -> Result<bool> {
    let parse_err =
        |ty: &str| plan_err!("parquet_bloom_filter_check could not parse value '{value}' as {ty}");
    match physical_type {
        PhysicalType::BOOLEAN => match value.parse::<bool>() {
            Ok(v) => Ok(sbbf.check(&v)),
            Err(_) => parse_err("BOOLEAN"),
        },
        PhysicalType::INT32 => match value.parse::<i32>() {
            Ok(v) => Ok(sbbf.check(&v)),
            Err(_) => parse_err("INT32"),
        },
        PhysicalType::INT64 => match value.parse::<i64>() {
            Ok(v) => Ok(sbbf.check(&v)),
            Err(_) => parse_err("INT64"),
        },
        PhysicalType::FLOAT => match value.parse::<f32>() {
            Ok(v) => Ok(sbbf.check(&v)),
            Err(_) => parse_err("FLOAT"),
        },
        PhysicalType::DOUBLE => match value.parse::<f64>() {
            Ok(v) => Ok(sbbf.check(&v)),
            Err(_) => parse_err("DOUBLE"),
        },
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => Ok(sbbf.check(value)),
        PhysicalType::INT96 => {
            plan_err!("parquet_bloom_filter_check does not support INT96 columns")
        }
    }
}

/// `parquet_bloom_filter_check` table-valued function
///
/// Probes the bloom filter of a column for a value, returning one row per row
/// group with whether the value might be present. A `false` in `might_contain`
/// guarantees the value is absent from that row group; `true` means it might
/// be present (subject to the filter's false positive probability). Row groups
/// without a bloom filter return a null `might_contain`.
///
/// Example:
/// ```sql
/// SELECT * FROM parquet_bloom_filter_check('file.parquet', 'user_id', 'abc123');
/// ```
#[derive(Debug)]
pub struct ParquetBloomFilterCheckFunc {}

impl TableFunctionImpl for ParquetBloomFilterCheckFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = expr_to_string(exprs.first(), "parquet_bloom_filter_check", "filename")?;
        let column_name = expr_to_string(exprs.get(1), "parquet_bloom_filter_check", "column")?;
        let value = match exprs.get(2) {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => s.clone(),
            Some(Expr::Literal(ScalarValue::Int64(Some(i)), _)) => i.to_string(),
            Some(Expr::Literal(ScalarValue::Float64(Some(f)), _)) => f.to_string(),
            Some(Expr::Literal(ScalarValue::Boolean(Some(b)), _)) => b.to_string(),
            _ => {
                return plan_err!(
                    "parquet_bloom_filter_check requires a string, numeric or boolean value argument"
                );
            }
        };

        let file = File::open(&filename)?;
        let metadata = ParquetMetaDataReader::new().parse_and_finish(&file)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("row_group_id", DataType::Int64, true),
            Field::new("column_id", DataType::Int64, true),
            Field::new("path_in_schema", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
            Field::new("has_bloom_filter", DataType::Boolean, true),
            Field::new("might_contain", DataType::Boolean, true),
        ]));

        let mut filename_arr: Vec<Option<String>> = vec![];
        let mut row_group_id_arr: Vec<Option<i64>> = vec![];
        let mut column_id_arr: Vec<Option<i64>> = vec![];
        let mut path_in_schema_arr: Vec<Option<String>> = vec![];
        let mut value_arr: Vec<Option<String>> = vec![];
        let mut has_bloom_filter_arr: Vec<Option<bool>> = vec![];
        let mut might_contain_arr: Vec<Option<bool>> = vec![];

        let mut column_found = false;
        for (rg_idx, row_group) in metadata.row_groups().iter().enumerate() {
            for (col_idx, column) in row_group.columns().iter().enumerate() {
                let path = column.column_path().string();
                if path != column_name {
                    continue;
                }
                column_found = true;

                let might_contain = match Sbbf::read_from_column_chunk(column, &file)? {
                    Some(sbbf) => Some(check_value(&sbbf, column.column_type(), &value)?),
                    None => None,
                };

                filename_arr.push(Some(filename.clone()));
                row_group_id_arr.push(Some(rg_idx as i64));
                column_id_arr.push(Some(col_idx as i64));
                path_in_schema_arr.push(Some(path));
                value_arr.push(Some(value.clone()));
                has_bloom_filter_arr.push(Some(might_contain.is_some()));
                might_contain_arr.push(might_contain);
            }
        }

        if !column_found {
            let available: Vec<String> = metadata
                .row_groups()
                .first()
                .map(|rg| {
                    rg.columns()
                        .iter()
                        .map(|c| c.column_path().string())
                        .collect()
                })
                .unwrap_or_default();
            return plan_err!(
                "column '{column_name}' not found in '{filename}'. Available columns: {}",
                available.join(", ")
            );
        }

        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(filename_arr)),
                Arc::new(Int64Array::from(row_group_id_arr)),
                Arc::new(Int64Array::from(column_id_arr)),
                Arc::new(StringArray::from(path_in_schema_arr)),
                Arc::new(StringArray::from(value_arr)),
                Arc::new(BooleanArray::from(has_bloom_filter_arr)),
                Arc::new(BooleanArray::from(might_contain_arr)),
            ],
        )?;

        Ok(Arc::new(ParquetBloomFilterTable { schema, batch: rb }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
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

    fn bloom_props() -> WriterProperties {
        WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .build()
    }

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("parquet_bloom_filter", Arc::new(ParquetBloomFilterFunc {}));
        ctx.register_udtf(
            "parquet_bloom_filter_check",
            Arc::new(ParquetBloomFilterCheckFunc {}),
        );
        ctx
    }

    fn i64_val(batches: &[RecordBatch], row: usize, col: &str) -> Option<i64> {
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

    fn string_batch(values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
    }

    // --- parquet_bloom_filter tests ---

    /// Basic schema: result has the expected column names.
    #[tokio::test]
    async fn test_schema_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path, &[string_batch(vec!["a", "b"])], Some(bloom_props()));

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_bloom_filter('{}')", path.display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert!(!result.is_empty());

        let s = result[0].schema();
        for col in &[
            "filename",
            "row_group_id",
            "column_id",
            "path_in_schema",
            "has_bloom_filter",
            "bloom_filter_offset",
            "bloom_filter_length",
            "num_blocks",
            "bitset_size_bytes",
        ] {
            assert!(s.field_with_name(col).is_ok(), "missing column: {col}");
        }
    }

    /// A file written without bloom filters reports has_bloom_filter = false
    /// and null details.
    #[tokio::test]
    async fn test_no_bloom_filter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path, &[string_batch(vec!["a", "b"])], None);

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_bloom_filter('{}')", path.display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 1);
        assert_eq!(bool_val(&result, 0, "has_bloom_filter"), Some(false));
        assert_eq!(i64_val(&result, 0, "bloom_filter_offset"), None);
        assert_eq!(i64_val(&result, 0, "num_blocks"), None);
        assert_eq!(i64_val(&result, 0, "bitset_size_bytes"), None);
    }

    /// A file written with bloom filters reports offset, length and size.
    #[tokio::test]
    async fn test_bloom_filter_details() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path, &[string_batch(vec!["a", "b"])], Some(bloom_props()));

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_bloom_filter('{}')", path.display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 1);
        assert_eq!(bool_val(&result, 0, "has_bloom_filter"), Some(true));
        assert!(i64_val(&result, 0, "bloom_filter_offset").unwrap() > 0);
        assert!(i64_val(&result, 0, "bloom_filter_length").unwrap() > 0);
        let num_blocks = i64_val(&result, 0, "num_blocks").unwrap();
        assert!(num_blocks > 0);
        assert_eq!(
            i64_val(&result, 0, "bitset_size_bytes"),
            Some(num_blocks * 32)
        );
    }

    /// One row per (row group, column).
    #[tokio::test]
    async fn test_multiple_row_groups_and_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1i32, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["w", "x", "y", "z"])),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .set_max_row_group_size(2)
            .build();
        write_parquet(&path, &[batch], Some(props));

        let ctx = make_ctx();
        let sql = format!(
            "SELECT row_group_id, column_id FROM parquet_bloom_filter('{}') \
             ORDER BY row_group_id, column_id",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        // 2 row groups x 2 columns
        assert_eq!(total_rows(&result), 4);
        assert_eq!(i64_val(&result, 0, "row_group_id"), Some(0));
        assert_eq!(i64_val(&result, 3, "row_group_id"), Some(1));
        assert_eq!(i64_val(&result, 3, "column_id"), Some(1));
    }

    // --- parquet_bloom_filter_check tests ---

    /// A value present in the file returns might_contain = true.
    #[tokio::test]
    async fn test_check_string_present() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(
            &path,
            &[string_batch(vec!["apple", "banana", "cherry"])],
            Some(bloom_props()),
        );

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM parquet_bloom_filter_check('{}', 'name', 'banana')",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 1);
        assert_eq!(bool_val(&result, 0, "has_bloom_filter"), Some(true));
        assert_eq!(bool_val(&result, 0, "might_contain"), Some(true));
    }

    /// A value absent from the file returns might_contain = false.
    #[tokio::test]
    async fn test_check_string_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(
            &path,
            &[string_batch(vec!["apple", "banana", "cherry"])],
            Some(bloom_props()),
        );

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM parquet_bloom_filter_check('{}', 'name', 'durian')",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 1);
        assert_eq!(bool_val(&result, 0, "might_contain"), Some(false));
    }

    /// Integer columns hash the physical representation, so an unquoted
    /// integer literal works.
    #[tokio::test]
    async fn test_check_int_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id32", DataType::Int32, false),
            Field::new("id64", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![10i32, 20, 30])),
                Arc::new(Int64Array::from(vec![100i64, 200, 300])),
            ],
        )
        .unwrap();
        write_parquet(&path, &[batch], Some(bloom_props()));

        let ctx = make_ctx();
        for (col, present, absent) in [("id32", 20, 25), ("id64", 200, 250)] {
            let sql = format!(
                "SELECT might_contain FROM parquet_bloom_filter_check('{}', '{col}', {present})",
                path.display()
            );
            let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
            assert_eq!(bool_val(&result, 0, "might_contain"), Some(true));

            let sql = format!(
                "SELECT might_contain FROM parquet_bloom_filter_check('{}', '{col}', {absent})",
                path.display()
            );
            let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
            assert_eq!(bool_val(&result, 0, "might_contain"), Some(false));
        }
    }

    /// Double columns accept numeric literals.
    #[tokio::test]
    async fn test_check_double_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![1.5f64, 2.5, 3.5]))],
        )
        .unwrap();
        write_parquet(&path, &[batch], Some(bloom_props()));

        let ctx = make_ctx();
        let sql = format!(
            "SELECT might_contain FROM parquet_bloom_filter_check('{}', 'v', 2.5)",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(bool_val(&result, 0, "might_contain"), Some(true));

        let sql = format!(
            "SELECT might_contain FROM parquet_bloom_filter_check('{}', 'v', 9.5)",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(bool_val(&result, 0, "might_contain"), Some(false));
    }

    /// With multiple row groups, might_contain is evaluated per row group.
    #[tokio::test]
    async fn test_check_per_row_group() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        // 2 rows per group: "apple"/"banana" in group 0, "cherry"/"durian" in group 1
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .set_max_row_group_size(2)
            .build();
        write_parquet(
            &path,
            &[string_batch(vec!["apple", "banana", "cherry", "durian"])],
            Some(props),
        );

        let ctx = make_ctx();
        let sql = format!(
            "SELECT row_group_id, might_contain \
             FROM parquet_bloom_filter_check('{}', 'name', 'apple') \
             ORDER BY row_group_id",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 2);
        assert_eq!(bool_val(&result, 0, "might_contain"), Some(true));
        assert_eq!(bool_val(&result, 1, "might_contain"), Some(false));
    }

    /// A file without bloom filters returns null might_contain.
    #[tokio::test]
    async fn test_check_no_bloom_filter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path, &[string_batch(vec!["apple"])], None);

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM parquet_bloom_filter_check('{}', 'name', 'apple')",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 1);
        assert_eq!(bool_val(&result, 0, "has_bloom_filter"), Some(false));
        assert_eq!(bool_val(&result, 0, "might_contain"), None);
    }

    /// An unknown column produces a helpful error.
    #[tokio::test]
    async fn test_check_unknown_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path, &[string_batch(vec!["apple"])], Some(bloom_props()));

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM parquet_bloom_filter_check('{}', 'nope', 'apple')",
            path.display()
        );
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(err.contains("column 'nope' not found"), "got: {err}");
        assert!(err.contains("name"), "should list available columns: {err}");
    }

    /// A non-numeric value against an integer column produces a parse error.
    #[tokio::test]
    async fn test_check_type_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64, 2]))]).unwrap();
        write_parquet(&path, &[batch], Some(bloom_props()));

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM parquet_bloom_filter_check('{}', 'id', 'abc')",
            path.display()
        );
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(err.contains("could not parse value 'abc'"), "got: {err}");
    }
}

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

use crate::{byte_array_to_string, expr_to_string, fixed_len_byte_array_to_string};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{exec_datafusion_err, exec_err, plan_err};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use parquet::basic::{ConvertedType, Type as PhysicalType};
use parquet::column::page::Page;
use parquet::data_type::{ByteArray, FixedLenByteArray, Int96};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::fs::File;
use std::sync::Arc;

#[derive(Debug)]
struct ParquetDictionaryTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for ParquetDictionaryTable {
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

/// Decode a plain-encoded dictionary page buffer into one string per entry
fn decode_plain_dictionary(
    buf: &[u8],
    num_values: usize,
    physical_type: PhysicalType,
    converted_type: ConvertedType,
    type_length: i32,
) -> Result<Vec<String>> {
    let truncated = || {
        exec_datafusion_err!(
            "parquet_dictionary found a truncated dictionary page ({num_values} values)"
        )
    };

    let fixed_width = |width: usize| -> Result<Vec<&[u8]>> {
        if buf.len() < num_values * width {
            return Err(truncated());
        }
        Ok(buf[..num_values * width].chunks_exact(width).collect())
    };

    match physical_type {
        PhysicalType::BOOLEAN => {
            exec_err!("parquet_dictionary does not support BOOLEAN columns")
        }
        PhysicalType::INT32 => Ok(fixed_width(4)?
            .iter()
            .map(|c| i32::from_le_bytes(c[..4].try_into().unwrap()).to_string())
            .collect()),
        PhysicalType::INT64 => Ok(fixed_width(8)?
            .iter()
            .map(|c| i64::from_le_bytes(c[..8].try_into().unwrap()).to_string())
            .collect()),
        PhysicalType::INT96 => Ok(fixed_width(12)?
            .iter()
            .map(|c| {
                let mut v = Int96::new();
                v.set_data(
                    u32::from_le_bytes(c[0..4].try_into().unwrap()),
                    u32::from_le_bytes(c[4..8].try_into().unwrap()),
                    u32::from_le_bytes(c[8..12].try_into().unwrap()),
                );
                v.to_string()
            })
            .collect()),
        PhysicalType::FLOAT => Ok(fixed_width(4)?
            .iter()
            .map(|c| f32::from_le_bytes(c[..4].try_into().unwrap()).to_string())
            .collect()),
        PhysicalType::DOUBLE => Ok(fixed_width(8)?
            .iter()
            .map(|c| f64::from_le_bytes(c[..8].try_into().unwrap()).to_string())
            .collect()),
        PhysicalType::BYTE_ARRAY => {
            let mut values = Vec::with_capacity(num_values);
            let mut offset = 0;
            for _ in 0..num_values {
                if offset + 4 > buf.len() {
                    return Err(truncated());
                }
                let len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + len > buf.len() {
                    return Err(truncated());
                }
                let value = ByteArray::from(buf[offset..offset + len].to_vec());
                offset += len;
                let value = match converted_type {
                    ConvertedType::UTF8 => byte_array_to_string(Some(&value)).unwrap(),
                    _ => value.to_string(),
                };
                values.push(value);
            }
            Ok(values)
        }
        PhysicalType::FIXED_LEN_BYTE_ARRAY => {
            let width = type_length as usize;
            Ok(fixed_width(width)?
                .iter()
                .map(|c| {
                    let value = FixedLenByteArray::from(ByteArray::from(c.to_vec()));
                    match converted_type {
                        ConvertedType::UTF8 => {
                            fixed_len_byte_array_to_string(Some(&value)).unwrap()
                        }
                        _ => value.to_string(),
                    }
                })
                .collect())
        }
    }
}

/// `parquet_dictionary` table-valued function
///
/// Reads the dictionary page of a column, returning one row per dictionary
/// entry per row group. Row groups where the column has no dictionary page
/// (for example when dictionary encoding is disabled) produce no rows.
///
/// Example:
/// ```sql
/// SELECT * FROM parquet_dictionary('file.parquet', 'user_id');
/// ```
#[derive(Debug)]
pub struct ParquetDictionaryFunc {}

impl TableFunctionImpl for ParquetDictionaryFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = expr_to_string(exprs.first(), "parquet_dictionary", "filename")?;
        let column_name = expr_to_string(exprs.get(1), "parquet_dictionary", "column")?;

        let file = File::open(&filename)?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("row_group_id", DataType::Int64, true),
            Field::new("column_id", DataType::Int64, true),
            Field::new("path_in_schema", DataType::Utf8, true),
            Field::new("dictionary_index", DataType::Int64, true),
            Field::new("value", DataType::Utf8, true),
        ]));

        let mut filename_arr: Vec<Option<String>> = vec![];
        let mut row_group_id_arr: Vec<Option<i64>> = vec![];
        let mut column_id_arr: Vec<Option<i64>> = vec![];
        let mut path_in_schema_arr: Vec<Option<String>> = vec![];
        let mut dictionary_index_arr: Vec<Option<i64>> = vec![];
        let mut value_arr: Vec<Option<String>> = vec![];

        let mut column_found = false;
        for rg_idx in 0..metadata.num_row_groups() {
            for (col_idx, column) in metadata.row_group(rg_idx).columns().iter().enumerate() {
                let path = column.column_path().string();
                if path != column_name {
                    continue;
                }
                column_found = true;
                if column.dictionary_page_offset().is_none() {
                    continue;
                }

                let mut page_reader = reader
                    .get_row_group(rg_idx)?
                    .get_column_page_reader(col_idx)?;
                // The dictionary page, when present, is the first page of the
                // column chunk
                let Some(Page::DictionaryPage {
                    buf, num_values, ..
                }) = page_reader.get_next_page()?
                else {
                    continue;
                };
                let values = decode_plain_dictionary(
                    &buf,
                    num_values as usize,
                    column.column_type(),
                    column.column_descr().converted_type(),
                    column.column_descr().type_length(),
                )?;
                for (idx, value) in values.into_iter().enumerate() {
                    filename_arr.push(Some(filename.clone()));
                    row_group_id_arr.push(Some(rg_idx as i64));
                    column_id_arr.push(Some(col_idx as i64));
                    path_in_schema_arr.push(Some(path.clone()));
                    dictionary_index_arr.push(Some(idx as i64));
                    value_arr.push(Some(value));
                }
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
                Arc::new(Int64Array::from(dictionary_index_arr)),
                Arc::new(StringArray::from(value_arr)),
            ],
        )?;

        Ok(Arc::new(ParquetDictionaryTable { schema, batch: rb }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::path::Path;

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("parquet_dictionary", Arc::new(ParquetDictionaryFunc {}));
        ctx
    }

    /// Write `batches` to a parquet file at `path` using `props`.
    fn write_parquet(path: &Path, batches: &[RecordBatch], props: Option<WriterProperties>) {
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batches[0].schema(), props).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
    }

    fn string_batch(values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
    }

    async fn query_strings(ctx: &SessionContext, sql: &str, col: &str) -> Vec<Option<String>> {
        let result = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        result
            .iter()
            .flat_map(|batch| {
                let array = batch
                    .column(batch.schema().index_of(col).unwrap())
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..array.len())
                    .map(|i| array.is_valid(i).then(|| array.value(i).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// String dictionaries return the distinct values in insertion order.
    #[tokio::test]
    async fn test_string_dictionary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(
            &path,
            &[string_batch(vec!["apple", "banana", "apple", "cherry"])],
            None,
        );

        let ctx = make_ctx();
        let sql = format!(
            "SELECT value FROM parquet_dictionary('{}', 'name') ORDER BY dictionary_index",
            path.display()
        );
        assert_eq!(
            query_strings(&ctx, &sql, "value").await,
            vec![
                Some("apple".to_string()),
                Some("banana".to_string()),
                Some("cherry".to_string())
            ]
        );
    }

    /// Numeric column dictionaries decode per physical type.
    #[tokio::test]
    async fn test_numeric_dictionaries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("i32", DataType::Int32, false),
            Field::new("i64", DataType::Int64, false),
            Field::new("f64", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![10i32, 20, 10])),
                Arc::new(Int64Array::from(vec![100i64, 200, 100])),
                Arc::new(Float64Array::from(vec![1.5f64, 2.5, 1.5])),
            ],
        )
        .unwrap();
        write_parquet(&path, &[batch], None);

        let ctx = make_ctx();
        for (col, expected) in [
            ("i32", vec!["10", "20"]),
            ("i64", vec!["100", "200"]),
            ("f64", vec!["1.5", "2.5"]),
        ] {
            let sql = format!(
                "SELECT value FROM parquet_dictionary('{}', '{col}') ORDER BY dictionary_index",
                path.display()
            );
            let expected: Vec<Option<String>> =
                expected.into_iter().map(|v| Some(v.to_string())).collect();
            assert_eq!(query_strings(&ctx, &sql, "value").await, expected, "{col}");
        }
    }

    /// Each row group has its own dictionary.
    #[tokio::test]
    async fn test_per_row_group_dictionaries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        // 2 rows per group: "apple"/"banana" in group 0, "cherry"/"cherry" in group 1
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        write_parquet(
            &path,
            &[string_batch(vec!["apple", "banana", "cherry", "cherry"])],
            Some(props),
        );

        let ctx = make_ctx();
        let sql = format!(
            "SELECT row_group_id, value FROM parquet_dictionary('{}', 'name') \
             ORDER BY row_group_id, dictionary_index",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let row_groups: Vec<i64> = result
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(row_groups, vec![0, 0, 1]);
        assert_eq!(
            query_strings(&ctx, &sql, "value").await,
            vec![
                Some("apple".to_string()),
                Some("banana".to_string()),
                Some("cherry".to_string())
            ]
        );
    }

    /// A file written without dictionary encoding produces no rows.
    #[tokio::test]
    async fn test_no_dictionary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .build();
        write_parquet(&path, &[string_batch(vec!["apple", "banana"])], Some(props));

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM parquet_dictionary('{}', 'name')",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    /// An unknown column produces a helpful error.
    #[tokio::test]
    async fn test_unknown_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path, &[string_batch(vec!["apple"])], None);

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM parquet_dictionary('{}', 'nope')",
            path.display()
        );
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(err.contains("column 'nope' not found"), "got: {err}");
        assert!(err.contains("name"), "should list available columns: {err}");
    }

    /// Missing arguments produce a plan error.
    #[tokio::test]
    async fn test_missing_arguments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path, &[string_batch(vec!["apple"])], None);

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_dictionary('{}')", path.display());
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(err.contains("requires a string column argument"), "{err}");
    }
}

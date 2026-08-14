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
use parquet::column::page::Page;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::fs::File;
use std::sync::Arc;

#[derive(Debug)]
struct ParquetPagesTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for ParquetPagesTable {
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

/// One row of `parquet_pages` output, built from a decoded page plus any
/// location information from the offset index
struct PageRow {
    page_type: String,
    encoding: String,
    num_values: i64,
    num_rows: Option<i64>,
    num_nulls: Option<i64>,
    uncompressed_size: i64,
    def_level_encoding: Option<String>,
    rep_level_encoding: Option<String>,
    has_statistics: bool,
    offset: Option<i64>,
    compressed_page_size: Option<i64>,
    first_row_index: Option<i64>,
}

fn page_to_row(page: &Page) -> PageRow {
    match page {
        Page::DataPage {
            buf,
            num_values,
            encoding,
            def_level_encoding,
            rep_level_encoding,
            statistics,
        } => PageRow {
            page_type: "DATA_PAGE".to_string(),
            encoding: format!("{encoding:?}"),
            num_values: *num_values as i64,
            num_rows: None,
            num_nulls: None,
            uncompressed_size: buf.len() as i64,
            def_level_encoding: Some(format!("{def_level_encoding:?}")),
            rep_level_encoding: Some(format!("{rep_level_encoding:?}")),
            has_statistics: statistics.is_some(),
            offset: None,
            compressed_page_size: None,
            first_row_index: None,
        },
        Page::DataPageV2 {
            buf,
            num_values,
            encoding,
            num_nulls,
            num_rows,
            statistics,
            ..
        } => PageRow {
            page_type: "DATA_PAGE_V2".to_string(),
            encoding: format!("{encoding:?}"),
            num_values: *num_values as i64,
            num_rows: Some(*num_rows as i64),
            num_nulls: Some(*num_nulls as i64),
            uncompressed_size: buf.len() as i64,
            def_level_encoding: None,
            rep_level_encoding: None,
            has_statistics: statistics.is_some(),
            offset: None,
            compressed_page_size: None,
            first_row_index: None,
        },
        Page::DictionaryPage {
            buf,
            num_values,
            encoding,
            ..
        } => PageRow {
            page_type: "DICTIONARY_PAGE".to_string(),
            encoding: format!("{encoding:?}"),
            num_values: *num_values as i64,
            num_rows: None,
            num_nulls: None,
            uncompressed_size: buf.len() as i64,
            def_level_encoding: None,
            rep_level_encoding: None,
            has_statistics: false,
            offset: None,
            compressed_page_size: None,
            first_row_index: None,
        },
    }
}

/// `parquet_pages` table-valued function
///
/// Returns the physical page layout of a Parquet file with one row per page
/// (dictionary and data pages), decoded by walking every page of every column
/// chunk. Takes a filename and an optional column name to restrict the output
/// to a single column.
///
/// The `offset`, `compressed_page_size` (which includes the page header), and
/// `first_row_index` columns come from the offset index and are null for
/// files written without a page index; `uncompressed_size` is the size of the
/// page data after decompression, excluding the page header.
///
/// Examples:
/// ```sql
/// SELECT * FROM parquet_pages('file.parquet');
/// SELECT * FROM parquet_pages('file.parquet', 'user_id');
/// ```
#[derive(Debug)]
pub struct ParquetPagesFunc {}

impl TableFunctionImpl for ParquetPagesFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = expr_to_string(exprs.first(), "parquet_pages", "filename")?;
        let column_name = match exprs.get(1) {
            Some(_) => Some(expr_to_string(exprs.get(1), "parquet_pages", "column")?),
            None => None,
        };

        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Optional)
            .parse_and_finish(&File::open(&filename)?)?;
        let offset_indexes = metadata.offset_index();

        let reader = SerializedFileReader::new(File::open(&filename)?)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("row_group_id", DataType::Int64, true),
            Field::new("column_id", DataType::Int64, true),
            Field::new("path_in_schema", DataType::Utf8, true),
            Field::new("page_id", DataType::Int64, true),
            Field::new("page_type", DataType::Utf8, true),
            Field::new("encoding", DataType::Utf8, true),
            Field::new("num_values", DataType::Int64, true),
            Field::new("num_rows", DataType::Int64, true),
            Field::new("num_nulls", DataType::Int64, true),
            Field::new("uncompressed_size", DataType::Int64, true),
            Field::new("offset", DataType::Int64, true),
            Field::new("compressed_page_size", DataType::Int64, true),
            Field::new("first_row_index", DataType::Int64, true),
            Field::new("def_level_encoding", DataType::Utf8, true),
            Field::new("rep_level_encoding", DataType::Utf8, true),
            Field::new("has_statistics", DataType::Boolean, true),
        ]));

        let mut rows: Vec<(i64, i64, String, i64, PageRow)> = vec![];

        let mut column_found = false;
        for rg_idx in 0..metadata.num_row_groups() {
            let row_group = metadata.row_group(rg_idx);
            for (col_idx, column) in row_group.columns().iter().enumerate() {
                let path = column.column_path().string();
                if let Some(name) = &column_name {
                    if &path != name {
                        continue;
                    }
                }
                column_found = true;

                let page_locations = offset_indexes
                    .and_then(|oi| oi.get(rg_idx))
                    .and_then(|rg| rg.get(col_idx))
                    .map(|oi| oi.page_locations());

                let mut page_reader = reader
                    .get_row_group(rg_idx)?
                    .get_column_page_reader(col_idx)?;

                let mut page_idx: i64 = 0;
                let mut data_page_idx: usize = 0;
                while let Some(page) = page_reader.get_next_page()? {
                    let mut row = page_to_row(&page);
                    match &page {
                        Page::DictionaryPage { .. } => {
                            // The dictionary page is not tracked by the offset
                            // index, but the column chunk metadata records
                            // where it starts
                            row.offset = column.dictionary_page_offset();
                        }
                        _ => {
                            if let Some(loc) =
                                page_locations.and_then(|locs| locs.get(data_page_idx))
                            {
                                row.offset = Some(loc.offset);
                                row.compressed_page_size = Some(loc.compressed_page_size as i64);
                                row.first_row_index = Some(loc.first_row_index);
                            }
                            data_page_idx += 1;
                        }
                    }
                    rows.push((rg_idx as i64, col_idx as i64, path.clone(), page_idx, row));
                    page_idx += 1;
                }
            }
        }

        if let Some(name) = &column_name {
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
                    "column '{name}' not found in '{filename}'. Available columns: {}",
                    available.join(", ")
                );
            }
        }

        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some(filename); rows.len()])),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2.clone()).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.4.page_type.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.4.encoding.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.4.num_values).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.4.num_rows).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.4.num_nulls).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter()
                        .map(|r| r.4.uncompressed_size)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.4.offset).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter()
                        .map(|r| r.4.compressed_page_size)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.4.first_row_index).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.4.def_level_encoding.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.4.rep_level_encoding.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|r| r.4.has_statistics).collect::<Vec<_>>(),
                )),
            ],
        )?;

        Ok(Arc::new(ParquetPagesTable { schema, batch: rb }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::path::Path;

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("parquet_pages", Arc::new(ParquetPagesFunc {}));
        ctx
    }

    /// Write a two column parquet file with 6 rows split into 3 row groups.
    fn write_parquet(path: &Path, props: Option<WriterProperties>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..6).collect::<Vec<i64>>())),
                Arc::new(StringArray::from(vec!["a", "b", "a", "b", "a", "b"])),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, props).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    async fn collect(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql).await.unwrap().collect().await.unwrap()
    }

    fn string_col(batches: &[RecordBatch], col: &str) -> Vec<Option<String>> {
        batches
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

    fn int_col(batches: &[RecordBatch], col: &str) -> Vec<Option<i64>> {
        batches
            .iter()
            .flat_map(|batch| {
                let array = batch
                    .column(batch.schema().index_of(col).unwrap())
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..array.len())
                    .map(|i| array.is_valid(i).then(|| array.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Every page of every column chunk is reported with its type and
    /// location, and dictionary pages precede data pages.
    #[tokio::test]
    async fn test_all_pages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path, None);

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM parquet_pages('{}') ORDER BY row_group_id, column_id, page_id",
            path.display()
        );
        let batches = collect(&ctx, &sql).await;

        let page_types = string_col(&batches, "page_type");
        // 1 row group x 2 columns, each with a dictionary page and a data page
        assert_eq!(
            page_types,
            vec![
                Some("DICTIONARY_PAGE".to_string()),
                Some("DATA_PAGE".to_string()),
                Some("DICTIONARY_PAGE".to_string()),
                Some("DATA_PAGE".to_string()),
            ]
        );

        // Offsets are present for every page (dictionary offsets come from the
        // column chunk metadata, data page offsets from the offset index)
        assert!(int_col(&batches, "offset").iter().all(|v| v.is_some()));
        // The offset index provides sizes and row indexes for data pages
        let compressed = int_col(&batches, "compressed_page_size");
        assert!(compressed[1].is_some() && compressed[3].is_some());
        let first_row = int_col(&batches, "first_row_index");
        assert_eq!(first_row[1], Some(0));
        // num_values counts values in each page
        let num_values = int_col(&batches, "num_values");
        assert_eq!(num_values[1], Some(6));
        assert_eq!(num_values[3], Some(6));
    }

    /// A column argument restricts output to that column's pages.
    #[tokio::test]
    async fn test_column_filter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path, None);

        let ctx = make_ctx();
        let sql = format!(
            "SELECT DISTINCT path_in_schema FROM parquet_pages('{}', 's')",
            path.display()
        );
        let batches = collect(&ctx, &sql).await;
        assert_eq!(
            string_col(&batches, "path_in_schema"),
            vec![Some("s".to_string())]
        );
    }

    /// Disabling dictionary encoding removes dictionary pages from the output.
    #[tokio::test]
    async fn test_no_dictionary_pages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .build();
        write_parquet(&path, Some(props));

        let ctx = make_ctx();
        let sql = format!(
            "SELECT page_type FROM parquet_pages('{}', 'v')",
            path.display()
        );
        let batches = collect(&ctx, &sql).await;
        assert_eq!(
            string_col(&batches, "page_type"),
            vec![Some("DATA_PAGE".to_string())]
        );
    }

    /// Multiple row groups report pages per row group.
    #[tokio::test]
    async fn test_multiple_row_groups() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        write_parquet(&path, Some(props));

        let ctx = make_ctx();
        let sql = format!(
            "SELECT DISTINCT row_group_id FROM parquet_pages('{}', 'v') ORDER BY row_group_id",
            path.display()
        );
        let batches = collect(&ctx, &sql).await;
        assert_eq!(
            int_col(&batches, "row_group_id"),
            vec![Some(0), Some(1), Some(2)]
        );
    }

    /// An unknown column produces a helpful error.
    #[tokio::test]
    async fn test_unknown_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path, None);

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_pages('{}', 'nope')", path.display());
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(err.contains("column 'nope' not found"), "got: {err}");
    }

    /// A missing filename argument produces a plan error.
    #[tokio::test]
    async fn test_missing_argument() {
        let ctx = make_ctx();
        let err = ctx
            .sql("SELECT * FROM parquet_pages()")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("requires a string filename argument"), "{err}");
    }
}

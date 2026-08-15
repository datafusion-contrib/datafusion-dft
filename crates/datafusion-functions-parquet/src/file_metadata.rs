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
use arrow::array::{BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::exec_datafusion_err;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

#[derive(Debug)]
struct ParquetFileMetadataTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for ParquetFileMetadataTable {
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

/// Read the length of the footer metadata from the 8-byte trailer
/// (4-byte little-endian metadata length followed by the `PAR1` magic)
fn read_footer_metadata_len(file: &mut File) -> Result<i64> {
    let mut trailer = [0u8; 8];
    file.seek(SeekFrom::End(-8))?;
    file.read_exact(&mut trailer)?;
    if &trailer[4..] != b"PAR1" {
        return Err(exec_datafusion_err!(
            "file does not end with the parquet magic bytes"
        ));
    }
    Ok(u32::from_le_bytes(trailer[..4].try_into().unwrap()) as i64)
}

/// `parquet_file_metadata` table-valued function
///
/// Returns a single summary row for a Parquet file: writer (`created_by`) and
/// format version, row/row group/column counts, file and footer sizes, total
/// compressed and uncompressed data sizes, and whether the file has column
/// statistics, a page index, or bloom filters.
///
/// Example:
/// ```sql
/// SELECT * FROM parquet_file_metadata('file.parquet');
/// ```
#[derive(Debug)]
pub struct ParquetFileMetadataFunc {}

impl TableFunctionImpl for ParquetFileMetadataFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = expr_to_string(exprs.first(), "parquet_file_metadata", "filename")?;

        let mut file = File::open(&filename)?;
        let file_size = file.metadata()?.len() as i64;
        let footer_metadata_len = read_footer_metadata_len(&mut file)?;

        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();
        let file_metadata = metadata.file_metadata();

        let mut total_compressed_size: i64 = 0;
        let mut total_uncompressed_size: i64 = 0;
        let mut has_column_statistics = false;
        let mut has_page_index = false;
        let mut has_bloom_filters = false;
        for row_group in metadata.row_groups() {
            for column in row_group.columns() {
                total_compressed_size += column.compressed_size();
                total_uncompressed_size += column.uncompressed_size();
                has_column_statistics |= column.statistics().is_some();
                has_page_index |= column.column_index_offset().is_some()
                    || column.offset_index_offset().is_some();
                has_bloom_filters |= column.bloom_filter_offset().is_some();
            }
        }

        let num_key_value_metadata = file_metadata
            .key_value_metadata()
            .map(|kv| kv.len() as i64)
            .unwrap_or(0);

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("version", DataType::Int32, true),
            Field::new("created_by", DataType::Utf8, true),
            Field::new("num_rows", DataType::Int64, true),
            Field::new("num_row_groups", DataType::Int64, true),
            Field::new("num_columns", DataType::Int64, true),
            Field::new("file_size", DataType::Int64, true),
            Field::new("footer_size", DataType::Int64, true),
            Field::new("total_compressed_size", DataType::Int64, true),
            Field::new("total_uncompressed_size", DataType::Int64, true),
            Field::new("num_key_value_metadata", DataType::Int64, true),
            Field::new("has_column_statistics", DataType::Boolean, true),
            Field::new("has_page_index", DataType::Boolean, true),
            Field::new("has_bloom_filters", DataType::Boolean, true),
        ]));

        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some(filename)])),
                Arc::new(Int32Array::from(vec![file_metadata.version()])),
                Arc::new(StringArray::from(vec![file_metadata
                    .created_by()
                    .map(|s| s.to_string())])),
                Arc::new(Int64Array::from(vec![file_metadata.num_rows()])),
                Arc::new(Int64Array::from(vec![metadata.num_row_groups() as i64])),
                Arc::new(Int64Array::from(vec![
                    file_metadata.schema_descr().num_columns() as i64,
                ])),
                Arc::new(Int64Array::from(vec![file_size])),
                Arc::new(Int64Array::from(vec![footer_metadata_len])),
                Arc::new(Int64Array::from(vec![total_compressed_size])),
                Arc::new(Int64Array::from(vec![total_uncompressed_size])),
                Arc::new(Int64Array::from(vec![num_key_value_metadata])),
                Arc::new(BooleanArray::from(vec![has_column_statistics])),
                Arc::new(BooleanArray::from(vec![has_page_index])),
                Arc::new(BooleanArray::from(vec![has_bloom_filters])),
            ],
        )?;

        Ok(Arc::new(ParquetFileMetadataTable { schema, batch: rb }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array as ArrowInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::path::Path;

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf(
            "parquet_file_metadata",
            Arc::new(ParquetFileMetadataFunc {}),
        );
        ctx
    }

    /// Write a parquet file with 6 rows split into 3 row groups of 2 rows.
    fn write_parquet(path: &Path) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ArrowInt64Array::from(
                (0..6).collect::<Vec<i64>>(),
            ))],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// The summary row reports counts, sizes, and feature flags.
    #[tokio::test]
    async fn test_summary_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path);
        let file_size = std::fs::metadata(&path).unwrap().len() as i64;

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_file_metadata('{}')", path.display());
        let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let batch = &batches[0];

        let get_i64 = |col: &str| {
            batch
                .column(batch.schema().index_of(col).unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        let get_bool = |col: &str| {
            batch
                .column(batch.schema().index_of(col).unwrap())
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        };

        assert_eq!(get_i64("num_rows"), 6);
        assert_eq!(get_i64("num_row_groups"), 3);
        assert_eq!(get_i64("num_columns"), 1);
        assert_eq!(get_i64("file_size"), file_size);
        assert!(get_i64("footer_size") > 0);
        assert!(get_i64("total_compressed_size") > 0);
        assert!(get_i64("total_uncompressed_size") > 0);
        // arrow-rs writes statistics and a page index by default, but not
        // bloom filters
        assert!(get_bool("has_column_statistics"));
        assert!(get_bool("has_page_index"));
        assert!(!get_bool("has_bloom_filters"));

        let created_by = batch
            .column(batch.schema().index_of("created_by").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(created_by.value(0).contains("parquet-rs"));
    }

    /// Bloom filters are reported when the writer produces them.
    #[tokio::test]
    async fn test_bloom_filters_flag() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ArrowInt64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .build();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let ctx = make_ctx();
        let sql = format!(
            "SELECT has_bloom_filters FROM parquet_file_metadata('{}')",
            path.display()
        );
        let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let has_bloom = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0);
        assert!(has_bloom);
    }

    /// A non-parquet file produces an error.
    #[tokio::test]
    async fn test_not_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.txt");
        std::fs::write(&path, "not a parquet file").unwrap();

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_file_metadata('{}')", path.display());
        assert!(ctx.sql(&sql).await.is_err());
    }

    /// A missing filename argument produces a plan error.
    #[tokio::test]
    async fn test_missing_argument() {
        let ctx = make_ctx();
        let err = ctx
            .sql("SELECT * FROM parquet_file_metadata()")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("requires a string filename argument"), "{err}");
    }
}

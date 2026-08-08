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
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::fs::File;
use std::sync::Arc;

#[derive(Debug)]
struct ParquetCompressionTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for ParquetCompressionTable {
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

/// Percentage of space saved by compression, or `None` when the uncompressed
/// size is not positive
fn compression_efficiency(uncompressed: i64, compressed: i64) -> Option<f64> {
    (uncompressed > 0).then(|| (1.0 - compressed as f64 / uncompressed as f64) * 100.0)
}

/// `parquet_compression` table-valued function
///
/// Reports compression details with one row per column chunk per row group:
/// the codec, the encodings, the uncompressed (pre-compression) and compressed
/// (post-compression) sizes in bytes, and the compression efficiency as the
/// percentage of space saved (negative when compression grew the data).
///
/// Example:
/// ```sql
/// SELECT * FROM parquet_compression('file.parquet');
/// ```
#[derive(Debug)]
pub struct ParquetCompressionFunc {}

impl TableFunctionImpl for ParquetCompressionFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = expr_to_string(exprs.first(), "parquet_compression", "filename")?;

        let file = File::open(&filename)?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("row_group_id", DataType::Int64, true),
            Field::new("column_id", DataType::Int64, true),
            Field::new("path_in_schema", DataType::Utf8, true),
            Field::new("compression", DataType::Utf8, true),
            Field::new("encodings", DataType::Utf8, true),
            Field::new("uncompressed_size", DataType::Int64, true),
            Field::new("compressed_size", DataType::Int64, true),
            Field::new("compression_efficiency", DataType::Float64, true),
        ]));

        let mut filename_arr: Vec<Option<String>> = vec![];
        let mut row_group_id_arr: Vec<Option<i64>> = vec![];
        let mut column_id_arr: Vec<Option<i64>> = vec![];
        let mut path_in_schema_arr: Vec<Option<String>> = vec![];
        let mut compression_arr: Vec<Option<String>> = vec![];
        let mut encodings_arr: Vec<Option<String>> = vec![];
        let mut uncompressed_size_arr: Vec<Option<i64>> = vec![];
        let mut compressed_size_arr: Vec<Option<i64>> = vec![];
        let mut compression_efficiency_arr: Vec<Option<f64>> = vec![];

        for (rg_idx, row_group) in metadata.row_groups().iter().enumerate() {
            for (col_idx, column) in row_group.columns().iter().enumerate() {
                filename_arr.push(Some(filename.clone()));
                row_group_id_arr.push(Some(rg_idx as i64));
                column_id_arr.push(Some(col_idx as i64));
                path_in_schema_arr.push(Some(column.column_path().string()));
                compression_arr.push(Some(format!("{:?}", column.compression())));
                encodings_arr.push(Some(format!(
                    "{:?}",
                    column.encodings().collect::<Vec<_>>()
                )));
                uncompressed_size_arr.push(Some(column.uncompressed_size()));
                compressed_size_arr.push(Some(column.compressed_size()));
                compression_efficiency_arr.push(compression_efficiency(
                    column.uncompressed_size(),
                    column.compressed_size(),
                ));
            }
        }

        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(filename_arr)),
                Arc::new(Int64Array::from(row_group_id_arr)),
                Arc::new(Int64Array::from(column_id_arr)),
                Arc::new(StringArray::from(path_in_schema_arr)),
                Arc::new(StringArray::from(compression_arr)),
                Arc::new(StringArray::from(encodings_arr)),
                Arc::new(Int64Array::from(uncompressed_size_arr)),
                Arc::new(Int64Array::from(compressed_size_arr)),
                Arc::new(Float64Array::from(compression_efficiency_arr)),
            ],
        )?;

        Ok(Arc::new(ParquetCompressionTable { schema, batch: rb }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use std::path::Path;

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("parquet_compression", Arc::new(ParquetCompressionFunc {}));
        ctx
    }

    /// Write a single-column string parquet file at `path` using `props`.
    fn write_parquet(path: &Path, values: Vec<&str>, props: Option<WriterProperties>) {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))])
            .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, props).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    async fn collect(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql).await.unwrap().collect().await.unwrap()
    }

    fn i64_column(batches: &[RecordBatch], col: &str) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(batch.schema().index_of(col).unwrap())
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn f64_column(batches: &[RecordBatch], col: &str) -> Vec<Option<f64>> {
        batches
            .iter()
            .flat_map(|batch| {
                let array = batch
                    .column(batch.schema().index_of(col).unwrap())
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..array.len())
                    .map(|i| array.is_valid(i).then(|| array.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn string_column(batches: &[RecordBatch], col: &str) -> Vec<String> {
        batches
            .iter()
            .flat_map(|batch| {
                let array = batch
                    .column(batch.schema().index_of(col).unwrap())
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..array.len())
                    .map(|i| array.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Compressible data reports the codec, positive sizes, and a positive
    /// space saving.
    #[tokio::test]
    async fn test_compressed_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        // Repetitive strings with dictionary encoding disabled so the codec
        // has redundant bytes to remove
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_dictionary_enabled(false)
            .build();
        write_parquet(&path, vec!["abcabcabc"; 1000], Some(props));

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_compression('{}')", path.display());
        let batches = collect(&ctx, &sql).await;

        assert_eq!(string_column(&batches, "compression"), vec!["SNAPPY"]);
        assert!(string_column(&batches, "encodings")[0].contains("PLAIN"));
        let uncompressed = i64_column(&batches, "uncompressed_size")[0];
        let compressed = i64_column(&batches, "compressed_size")[0];
        assert!(uncompressed > 0);
        assert!(compressed > 0);
        assert!(compressed < uncompressed);
        let efficiency = f64_column(&batches, "compression_efficiency")[0].unwrap();
        assert!(efficiency > 0.0 && efficiency < 100.0, "{efficiency}");
        let expected = (1.0 - compressed as f64 / uncompressed as f64) * 100.0;
        assert!((efficiency - expected).abs() < 1e-9);
    }

    /// An uncompressed file reports equal sizes and 0% efficiency.
    #[tokio::test]
    async fn test_uncompressed_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .build();
        write_parquet(&path, vec!["apple", "banana", "cherry"], Some(props));

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_compression('{}')", path.display());
        let batches = collect(&ctx, &sql).await;

        assert_eq!(string_column(&batches, "compression"), vec!["UNCOMPRESSED"]);
        let uncompressed = i64_column(&batches, "uncompressed_size")[0];
        let compressed = i64_column(&batches, "compressed_size")[0];
        assert_eq!(uncompressed, compressed);
        assert_eq!(
            f64_column(&batches, "compression_efficiency"),
            vec![Some(0.0)]
        );
    }

    /// One row is produced per column chunk per row group.
    #[tokio::test]
    async fn test_per_row_group_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        write_parquet(
            &path,
            vec!["apple", "banana", "cherry", "durian", "elderberry", "fig"],
            Some(props),
        );

        let ctx = make_ctx();
        let sql = format!(
            "SELECT * FROM parquet_compression('{}') ORDER BY row_group_id",
            path.display()
        );
        let batches = collect(&ctx, &sql).await;
        assert_eq!(i64_column(&batches, "row_group_id"), vec![0, 1, 2]);
        assert_eq!(
            string_column(&batches, "path_in_schema"),
            vec!["name", "name", "name"]
        );
    }

    /// A missing filename argument produces a plan error.
    #[tokio::test]
    async fn test_missing_argument() {
        let ctx = make_ctx();
        let err = ctx
            .sql("SELECT * FROM parquet_compression()")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("requires a string filename argument"), "{err}");
    }
}

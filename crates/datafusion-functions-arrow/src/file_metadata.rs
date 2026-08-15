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

use crate::{filename_arg, read_footer, read_record_batch_header, BatchTable};
use arrow::array::{BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::fs::File;
use std::sync::Arc;

/// `arrow_file_metadata` table-valued function
///
/// Returns a single summary row for an Arrow IPC file: IPC metadata version,
/// record batch and dictionary counts, total row count, total body bytes,
/// and whether any record batch body is compressed.
///
/// Example:
/// ```sql
/// SELECT * FROM arrow_file_metadata('file.arrow');
/// ```
#[derive(Debug)]
pub struct ArrowFileMetadataFunc {}

impl TableFunctionImpl for ArrowFileMetadataFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = filename_arg("arrow_file_metadata", exprs)?;
        let footer = read_footer(filename)?;
        let mut file = File::open(filename)?;

        let mut total_rows = 0i64;
        let mut total_body_bytes = 0i64;
        let mut compressed = false;
        for block in &footer.record_batch_blocks {
            let header = read_record_batch_header(&mut file, block)?;
            total_rows += header.num_rows;
            total_body_bytes += block.body_length;
            compressed |= header.compression_codec.is_some();
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("version", DataType::Utf8, true),
            Field::new("num_batches", DataType::Int64, true),
            Field::new("num_dictionaries", DataType::Int64, true),
            Field::new("total_rows", DataType::Int64, true),
            Field::new("total_body_bytes", DataType::Int64, true),
            Field::new("compressed", DataType::Boolean, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some(filename.to_string())])),
                Arc::new(StringArray::from(vec![Some(footer.version)])),
                Arc::new(Int64Array::from(vec![Some(
                    footer.record_batch_blocks.len() as i64,
                )])),
                Arc::new(Int64Array::from(vec![Some(
                    footer.dictionary_blocks.len() as i64
                )])),
                Arc::new(Int64Array::from(vec![Some(total_rows)])),
                Arc::new(Int64Array::from(vec![Some(total_body_bytes)])),
                Arc::new(BooleanArray::from(vec![Some(compressed)])),
            ],
        )?;

        Ok(BatchTable::new(schema, batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array};
    use datafusion::prelude::SessionContext;
    use std::path::Path;

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("arrow_file_metadata", Arc::new(ArrowFileMetadataFunc {}));
        ctx
    }

    fn write_ipc(path: &Path, compression: Option<arrow::ipc::CompressionType>) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let options = arrow::ipc::writer::IpcWriteOptions::default()
            .try_with_compression(compression)
            .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer =
            arrow::ipc::writer::FileWriter::try_new_with_options(file, &schema, options).unwrap();
        for i in 0..2 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![i, i + 1, i + 2]))],
            )
            .unwrap();
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
    }

    fn int_val(batch: &RecordBatch, col: &str) -> i64 {
        batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    fn str_val(batch: &RecordBatch, col: &str) -> String {
        batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    }

    /// The summary row reports counts, rows, sizes, and compression state.
    #[tokio::test]
    async fn test_summary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        write_ipc(&path, None);

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM arrow_file_metadata('{}')", path.display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        assert_eq!(batch.num_rows(), 1);

        assert_eq!(str_val(&batch, "filename"), path.display().to_string());
        assert_eq!(str_val(&batch, "version"), "V5");
        assert_eq!(int_val(&batch, "num_batches"), 2);
        assert_eq!(int_val(&batch, "num_dictionaries"), 0);
        assert_eq!(int_val(&batch, "total_rows"), 6);
        assert!(int_val(&batch, "total_body_bytes") > 0);
        let compressed = batch
            .column(batch.schema().index_of("compressed").unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!compressed.value(0));
    }

    /// Compressed files are reported as compressed.
    #[tokio::test]
    async fn test_compressed_summary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        write_ipc(&path, Some(arrow::ipc::CompressionType::ZSTD));

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM arrow_file_metadata('{}')", path.display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        let compressed = batch
            .column(batch.schema().index_of("compressed").unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(compressed.value(0));
    }

    /// A stream-format IPC file (no footer) produces an error.
    #[tokio::test]
    async fn test_stream_format_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrows");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1i32]))])
                .unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM arrow_file_metadata('{}')", path.display());
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(err.contains("footer"), "{err}");
    }

    /// Missing argument produces a plan error.
    #[tokio::test]
    async fn test_missing_argument() {
        let ctx = make_ctx();
        let err = ctx.sql("SELECT * FROM arrow_file_metadata()").await;
        assert!(err.is_err());
    }
}

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
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::fs::File;
use std::sync::Arc;

/// `arrow_batches` table-valued function
///
/// Returns one row per record batch block in an Arrow IPC file, including
/// the block's file offset, metadata and body sizes, row count, and
/// compression codec (null when the batch body is not compressed).
///
/// Example:
/// ```sql
/// SELECT * FROM arrow_batches('file.arrow');
/// ```
#[derive(Debug)]
pub struct ArrowBatchesFunc {}

impl TableFunctionImpl for ArrowBatchesFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = filename_arg("arrow_batches", exprs)?;
        let footer = read_footer(filename)?;
        let mut file = File::open(filename)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("batch_index", DataType::Int64, true),
            Field::new("offset", DataType::Int64, true),
            Field::new("metadata_length", DataType::Int64, true),
            Field::new("body_length", DataType::Int64, true),
            Field::new("num_rows", DataType::Int64, true),
            Field::new("compression_codec", DataType::Utf8, true),
            Field::new("compression_method", DataType::Utf8, true),
        ]));

        let mut filename_arr: Vec<Option<String>> = vec![];
        let mut batch_index_arr: Vec<Option<i64>> = vec![];
        let mut offset_arr: Vec<Option<i64>> = vec![];
        let mut metadata_length_arr: Vec<Option<i64>> = vec![];
        let mut body_length_arr: Vec<Option<i64>> = vec![];
        let mut num_rows_arr: Vec<Option<i64>> = vec![];
        let mut codec_arr: Vec<Option<String>> = vec![];
        let mut method_arr: Vec<Option<String>> = vec![];

        for (index, block) in footer.record_batch_blocks.iter().enumerate() {
            let header = read_record_batch_header(&mut file, block)?;
            filename_arr.push(Some(filename.to_string()));
            batch_index_arr.push(Some(index as i64));
            offset_arr.push(Some(block.offset));
            metadata_length_arr.push(Some(block.metadata_length));
            body_length_arr.push(Some(block.body_length));
            num_rows_arr.push(Some(header.num_rows));
            codec_arr.push(header.compression_codec);
            method_arr.push(header.compression_method);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(filename_arr)),
                Arc::new(Int64Array::from(batch_index_arr)),
                Arc::new(Int64Array::from(offset_arr)),
                Arc::new(Int64Array::from(metadata_length_arr)),
                Arc::new(Int64Array::from(body_length_arr)),
                Arc::new(Int64Array::from(num_rows_arr)),
                Arc::new(StringArray::from(codec_arr)),
                Arc::new(StringArray::from(method_arr)),
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
        ctx.register_udtf("arrow_batches", Arc::new(ArrowBatchesFunc {}));
        ctx
    }

    /// Write an IPC file with 3 batches of 2 rows each, optionally compressed.
    fn write_ipc(path: &Path, compression: Option<arrow::ipc::CompressionType>) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let options = arrow::ipc::writer::IpcWriteOptions::default()
            .try_with_compression(compression)
            .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer =
            arrow::ipc::writer::FileWriter::try_new_with_options(file, &schema, options).unwrap();
        for i in 0..3 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![i * 2, i * 2 + 1]))],
            )
            .unwrap();
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
    }

    fn int_val(batch: &RecordBatch, row: usize, col: &str) -> Option<i64> {
        let array = batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        array.is_valid(row).then(|| array.value(row))
    }

    fn str_val(batch: &RecordBatch, row: usize, col: &str) -> Option<String> {
        let array = batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        array.is_valid(row).then(|| array.value(row).to_string())
    }

    /// One row per batch with offsets, sizes, and row counts.
    #[tokio::test]
    async fn test_uncompressed_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        write_ipc(&path, None);

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM arrow_batches('{}')", path.display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let mut prev_end = 0i64;
        for row in 0..3 {
            assert_eq!(int_val(&batch, row, "batch_index"), Some(row as i64));
            assert_eq!(int_val(&batch, row, "num_rows"), Some(2));
            let offset = int_val(&batch, row, "offset").unwrap();
            let meta = int_val(&batch, row, "metadata_length").unwrap();
            let body = int_val(&batch, row, "body_length").unwrap();
            assert!(meta > 0 && body > 0);
            // Blocks are laid out sequentially in the file
            assert!(offset >= prev_end);
            prev_end = offset + meta + body;
            assert_eq!(str_val(&batch, row, "compression_codec"), None);
            assert_eq!(str_val(&batch, row, "compression_method"), None);
        }
    }

    /// Compressed batches report their codec and method.
    #[tokio::test]
    async fn test_compressed_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        write_ipc(&path, Some(arrow::ipc::CompressionType::ZSTD));

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM arrow_batches('{}')", path.display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        assert_eq!(batch.num_rows(), 3);
        for row in 0..3 {
            assert_eq!(
                str_val(&batch, row, "compression_codec").as_deref(),
                Some("ZSTD")
            );
            assert_eq!(
                str_val(&batch, row, "compression_method").as_deref(),
                Some("BUFFER")
            );
        }
    }

    /// Missing argument produces a plan error.
    #[tokio::test]
    async fn test_missing_argument() {
        let ctx = make_ctx();
        let err = ctx.sql("SELECT * FROM arrow_batches()").await;
        assert!(err.is_err());
    }
}

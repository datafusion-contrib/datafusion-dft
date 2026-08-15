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

use crate::{filename_arg, read_dictionary_header, read_footer, BatchTable};
use arrow::array::{BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::fs::File;
use std::sync::Arc;

/// `arrow_dictionaries` table-valued function
///
/// Returns one row per dictionary block in an Arrow IPC file, including the
/// dictionary id, whether the block is a delta, its file offset, metadata
/// and body sizes, and entry count.
///
/// Example:
/// ```sql
/// SELECT * FROM arrow_dictionaries('file.arrow');
/// ```
#[derive(Debug)]
pub struct ArrowDictionariesFunc {}

impl TableFunctionImpl for ArrowDictionariesFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = filename_arg("arrow_dictionaries", exprs)?;
        let footer = read_footer(filename)?;
        let mut file = File::open(filename)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("dictionary_index", DataType::Int64, true),
            Field::new("dictionary_id", DataType::Int64, true),
            Field::new("is_delta", DataType::Boolean, true),
            Field::new("offset", DataType::Int64, true),
            Field::new("metadata_length", DataType::Int64, true),
            Field::new("body_length", DataType::Int64, true),
            Field::new("num_entries", DataType::Int64, true),
        ]));

        let mut filename_arr: Vec<Option<String>> = vec![];
        let mut index_arr: Vec<Option<i64>> = vec![];
        let mut id_arr: Vec<Option<i64>> = vec![];
        let mut is_delta_arr: Vec<Option<bool>> = vec![];
        let mut offset_arr: Vec<Option<i64>> = vec![];
        let mut metadata_length_arr: Vec<Option<i64>> = vec![];
        let mut body_length_arr: Vec<Option<i64>> = vec![];
        let mut num_entries_arr: Vec<Option<i64>> = vec![];

        for (index, block) in footer.dictionary_blocks.iter().enumerate() {
            let header = read_dictionary_header(&mut file, block)?;
            filename_arr.push(Some(filename.to_string()));
            index_arr.push(Some(index as i64));
            id_arr.push(Some(header.id));
            is_delta_arr.push(Some(header.is_delta));
            offset_arr.push(Some(block.offset));
            metadata_length_arr.push(Some(block.metadata_length));
            body_length_arr.push(Some(block.body_length));
            num_entries_arr.push(Some(header.num_rows));
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(filename_arr)),
                Arc::new(Int64Array::from(index_arr)),
                Arc::new(Int64Array::from(id_arr)),
                Arc::new(BooleanArray::from(is_delta_arr)),
                Arc::new(Int64Array::from(offset_arr)),
                Arc::new(Int64Array::from(metadata_length_arr)),
                Arc::new(Int64Array::from(body_length_arr)),
                Arc::new(Int64Array::from(num_entries_arr)),
            ],
        )?;

        Ok(BatchTable::new(schema, batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, DictionaryArray, Int32Array};
    use datafusion::prelude::SessionContext;
    use std::path::Path;

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("arrow_dictionaries", Arc::new(ArrowDictionariesFunc {}));
        ctx
    }

    /// Write an IPC file with a dictionary-encoded column (2 entries).
    fn write_ipc_with_dictionary(path: &Path) {
        let values: DictionaryArray<Int32Type> = vec!["a", "b", "a"].into_iter().collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            values.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)]).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    use arrow::datatypes::Int32Type;

    fn int_val(batch: &RecordBatch, row: usize, col: &str) -> Option<i64> {
        let array = batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        array.is_valid(row).then(|| array.value(row))
    }

    /// A dictionary-encoded column produces one dictionary block with its id
    /// and entry count.
    #[tokio::test]
    async fn test_dictionary_block() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        write_ipc_with_dictionary(&path);

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM arrow_dictionaries('{}')", path.display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(int_val(&batch, 0, "dictionary_index"), Some(0));
        assert_eq!(int_val(&batch, 0, "dictionary_id"), Some(0));
        assert_eq!(int_val(&batch, 0, "num_entries"), Some(2));
        assert!(int_val(&batch, 0, "offset").unwrap() >= 0);
        assert!(int_val(&batch, 0, "metadata_length").unwrap() > 0);
        assert!(int_val(&batch, 0, "body_length").unwrap() > 0);

        let is_delta = batch
            .column(batch.schema().index_of("is_delta").unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!is_delta.value(0));
    }

    /// A file without dictionary columns returns no rows.
    #[tokio::test]
    async fn test_no_dictionaries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1i32]))])
                .unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM arrow_dictionaries('{}')", path.display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(result.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    }

    /// Missing argument produces a plan error.
    #[tokio::test]
    async fn test_missing_argument() {
        let ctx = make_ctx();
        let err = ctx.sql("SELECT * FROM arrow_dictionaries()").await;
        assert!(err.is_err());
    }
}

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

use crate::{filename_arg, read_footer, BatchTable};
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::sync::Arc;

/// `arrow_metadata` table-valued function
///
/// Returns the custom key-value metadata stored in an Arrow IPC file's
/// footer. Each row represents one key-value pair.
///
/// Example:
/// ```sql
/// SELECT * FROM arrow_metadata('file.arrow');
/// ```
#[derive(Debug)]
pub struct ArrowMetadataFunc {}

impl TableFunctionImpl for ArrowMetadataFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = filename_arg("arrow_metadata", exprs)?;
        let footer = read_footer(filename)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
        ]));

        let mut filename_arr: Vec<Option<String>> = vec![];
        let mut key_arr: Vec<Option<String>> = vec![];
        let mut value_arr: Vec<Option<String>> = vec![];

        for (key, value) in &footer.custom_metadata {
            filename_arr.push(Some(filename.to_string()));
            key_arr.push(key.clone());
            value_arr.push(value.clone());
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(filename_arr)),
                Arc::new(StringArray::from(key_arr)),
                Arc::new(StringArray::from(value_arr)),
            ],
        )?;

        Ok(BatchTable::new(schema, batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::SchemaRef;
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;
    use std::path::Path;

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("arrow_metadata", Arc::new(ArrowMetadataFunc {}));
        ctx
    }

    /// Write an IPC file with the given footer-level custom metadata.
    fn write_ipc(path: &Path, metadata: HashMap<String, String>) {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1i32]))])
                .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &schema).unwrap();
        for (key, value) in metadata {
            writer.write_metadata(key, value);
        }
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

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

    /// Custom key-value pairs are returned with their values.
    #[tokio::test]
    async fn test_custom_kv_pairs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        let metadata = HashMap::from([
            ("writer".to_string(), "dft".to_string()),
            ("version".to_string(), "1.0".to_string()),
        ]);
        write_ipc(&path, metadata);

        let ctx = make_ctx();
        let sql = format!(
            "SELECT key, value FROM arrow_metadata('{}') ORDER BY key",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(result.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        assert_eq!(str_val(&result, 0, "key").as_deref(), Some("version"));
        assert_eq!(str_val(&result, 0, "value").as_deref(), Some("1.0"));
        assert_eq!(str_val(&result, 1, "key").as_deref(), Some("writer"));
        assert_eq!(str_val(&result, 1, "value").as_deref(), Some("dft"));
    }

    /// A file with no custom metadata returns no rows.
    #[tokio::test]
    async fn test_no_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        write_ipc(&path, HashMap::new());

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM arrow_metadata('{}')", path.display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(result.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    }

    /// Missing argument produces a plan error.
    #[tokio::test]
    async fn test_missing_argument() {
        let ctx = make_ctx();
        let err = ctx.sql("SELECT * FROM arrow_metadata()").await;
        assert!(err.is_err());
    }
}

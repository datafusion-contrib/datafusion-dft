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
use arrow::array::{BooleanArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::sync::Arc;

/// `arrow_schema` table-valued function
///
/// Reads the schema stored in an Arrow IPC file's footer and returns one row
/// per top-level field.
///
/// Example:
/// ```sql
/// SELECT * FROM arrow_schema('file.arrow');
/// ```
#[derive(Debug)]
pub struct ArrowSchemaFunc {}

impl TableFunctionImpl for ArrowSchemaFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = filename_arg("arrow_schema", exprs)?;
        let footer = read_footer(filename)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("field_name", DataType::Utf8, true),
            Field::new("data_type", DataType::Utf8, true),
            Field::new("nullable", DataType::Boolean, true),
            Field::new("metadata", DataType::Utf8, true),
        ]));

        let mut filename_arr: Vec<Option<String>> = vec![];
        let mut field_name_arr: Vec<Option<String>> = vec![];
        let mut data_type_arr: Vec<Option<String>> = vec![];
        let mut nullable_arr: Vec<Option<bool>> = vec![];
        let mut metadata_arr: Vec<Option<String>> = vec![];

        for field in footer.schema.fields() {
            filename_arr.push(Some(filename.to_string()));
            field_name_arr.push(Some(field.name().clone()));
            data_type_arr.push(Some(field.data_type().to_string()));
            nullable_arr.push(Some(field.is_nullable()));
            metadata_arr.push(if field.metadata().is_empty() {
                None
            } else {
                Some(format!("{:?}", field.metadata()))
            });
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(filename_arr)),
                Arc::new(StringArray::from(field_name_arr)),
                Arc::new(StringArray::from(data_type_arr)),
                Arc::new(BooleanArray::from(nullable_arr)),
                Arc::new(StringArray::from(metadata_arr)),
            ],
        )?;

        Ok(BatchTable::new(schema, batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray as StringArrayType};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;
    use std::path::Path;

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("arrow_schema", Arc::new(ArrowSchemaFunc {}));
        ctx
    }

    fn write_ipc(path: &Path) {
        let mut field_metadata: HashMap<String, String> = HashMap::new();
        field_metadata.insert("source".to_string(), "test".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            Field::new("s", DataType::Utf8, true).with_metadata(field_metadata),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1i32])),
                Arc::new(StringArrayType::from(vec![Some("a")])),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    fn str_val(batch: &RecordBatch, row: usize, col: &str) -> Option<String> {
        let array = batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<StringArrayType>()
            .unwrap();
        arrow::array::Array::is_valid(array, row).then(|| array.value(row).to_string())
    }

    /// One row per field with name, data type, nullability, and field metadata.
    #[tokio::test]
    async fn test_fields_returned() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        write_ipc(&path);

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM arrow_schema('{}')", path.display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        assert_eq!(batch.num_rows(), 2);

        assert_eq!(str_val(&batch, 0, "field_name").as_deref(), Some("v"));
        assert_eq!(str_val(&batch, 0, "data_type").as_deref(), Some("Int32"));
        assert_eq!(str_val(&batch, 1, "field_name").as_deref(), Some("s"));
        assert_eq!(str_val(&batch, 1, "data_type").as_deref(), Some("Utf8"));

        let nullable = batch
            .column(batch.schema().index_of("nullable").unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!nullable.value(0));
        assert!(nullable.value(1));

        assert_eq!(str_val(&batch, 0, "metadata"), None);
        assert!(
            str_val(&batch, 1, "metadata").unwrap().contains("source"),
            "expected field metadata"
        );

        assert_eq!(
            str_val(&batch, 0, "filename").as_deref(),
            Some(path.display().to_string().as_str())
        );
    }

    /// Missing argument produces a plan error.
    #[tokio::test]
    async fn test_missing_argument() {
        let ctx = make_ctx();
        let err = ctx.sql("SELECT * FROM arrow_schema()").await;
        assert!(err.is_err());
    }

    /// A non-IPC file produces an error.
    #[tokio::test]
    async fn test_not_an_ipc_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.txt");
        std::fs::write(&path, b"not an arrow ipc file at all").unwrap();

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM arrow_schema('{}')", path.display());
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(!err.is_empty());
    }
}

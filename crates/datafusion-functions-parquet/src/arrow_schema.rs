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

use arrow::array::{BooleanArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use base64::prelude::{Engine, BASE64_STANDARD};
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{plan_err, Column};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use parquet::arrow::ARROW_SCHEMA_META_KEY;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::fs::File;
use std::sync::Arc;

#[derive(Debug)]
struct ParquetArrowSchemaTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for ParquetArrowSchemaTable {
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

/// `parquet_arrow_schema` table-valued function
///
/// Decodes the Arrow schema stored in a Parquet file's `ARROW:schema`
/// key-value metadata entry (a base64-encoded Arrow IPC schema message) and
/// returns one row per field.
///
/// Example:
/// ```sql
/// SELECT * FROM parquet_arrow_schema('file.parquet');
/// ```
#[derive(Debug)]
pub struct ParquetArrowSchemaFunc {}

impl TableFunctionImpl for ParquetArrowSchemaFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = match exprs.first() {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => s,
            Some(Expr::Column(Column { name, .. })) => name,
            _ => {
                return plan_err!("parquet_arrow_schema requires a string argument as its input");
            }
        };

        let file = File::open(filename.clone())?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();

        let encoded = metadata
            .file_metadata()
            .key_value_metadata()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == ARROW_SCHEMA_META_KEY)
                    .and_then(|kv| kv.value.clone())
            });
        let Some(encoded) = encoded else {
            return plan_err!("no {ARROW_SCHEMA_META_KEY} key-value metadata found in {filename}");
        };

        let decoded = BASE64_STANDARD.decode(encoded).map_err(|e| {
            DataFusionError::Execution(format!(
                "failed to base64 decode {ARROW_SCHEMA_META_KEY} value in {filename}: {e}"
            ))
        })?;
        let arrow_schema = arrow::ipc::convert::try_schema_from_ipc_buffer(&decoded)?;

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

        for field in arrow_schema.fields() {
            filename_arr.push(Some(filename.clone()));
            field_name_arr.push(Some(field.name().clone()));
            data_type_arr.push(Some(field.data_type().to_string()));
            nullable_arr.push(Some(field.is_nullable()));
            metadata_arr.push(if field.metadata().is_empty() {
                None
            } else {
                Some(format!("{:?}", field.metadata()))
            });
        }

        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(filename_arr)),
                Arc::new(StringArray::from(field_name_arr)),
                Arc::new(StringArray::from(data_type_arr)),
                Arc::new(BooleanArray::from(nullable_arr)),
                Arc::new(StringArray::from(metadata_arr)),
            ],
        )?;

        Ok(Arc::new(ParquetArrowSchemaTable { schema, batch: rb }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use parquet::arrow::arrow_writer::ArrowWriterOptions;
    use parquet::arrow::ArrowWriter;
    use std::path::Path;

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("parquet_arrow_schema", Arc::new(ParquetArrowSchemaFunc {}));
        ctx
    }

    /// Write a parquet file at `path` with a nullable int column and a
    /// timezone-aware timestamp column (the latter is only fully described by
    /// the embedded Arrow schema, not the Parquet schema).
    fn write_parquet(path: &Path) {
        let tz = "America/New_York";
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into())),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1i32])),
                Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000i64]).with_timezone(tz)),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn str_val(batch: &RecordBatch, row: usize, col: &str) -> Option<String> {
        let array = batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        array.is_valid(row).then(|| array.value(row).to_string())
    }

    /// One row per field with name, data type (including timezone), and nullability.
    #[tokio::test]
    async fn test_fields_returned() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path);

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_arrow_schema('{}')", path.display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&result[0].schema(), &result).unwrap();
        assert_eq!(batch.num_rows(), 2);

        assert_eq!(str_val(&batch, 0, "field_name").as_deref(), Some("v"));
        assert_eq!(str_val(&batch, 0, "data_type").as_deref(), Some("Int32"));
        assert_eq!(str_val(&batch, 1, "field_name").as_deref(), Some("ts"));
        let ts_type = str_val(&batch, 1, "data_type").unwrap();
        assert!(
            ts_type.contains("Timestamp") && ts_type.contains("America/New_York"),
            "unexpected data_type: {ts_type}"
        );

        let nullable = batch
            .column(batch.schema().index_of("nullable").unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(nullable.value(0));
        assert!(!nullable.value(1));

        assert_eq!(
            str_val(&batch, 0, "filename").as_deref(),
            Some(path.display().to_string().as_str())
        );
    }

    /// A file written without the embedded Arrow schema produces an error.
    #[tokio::test]
    async fn test_missing_arrow_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1i32]))])
                .unwrap();
        let options = ArrowWriterOptions::new().with_skip_arrow_metadata(true);
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new_with_options(file, schema, options).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_arrow_schema('{}')", path.display());
        let err = ctx.sql(&sql).await.unwrap_err().to_string();
        assert!(
            err.contains("no ARROW:schema key-value metadata found"),
            "{err}"
        );
    }

    /// Missing argument produces a plan error.
    #[tokio::test]
    async fn test_missing_argument() {
        let ctx = make_ctx();
        let err = ctx.sql("SELECT * FROM parquet_arrow_schema()").await;
        assert!(err.is_err());
    }
}

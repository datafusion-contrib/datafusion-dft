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

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{plan_err, Column};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::fs::File;
use std::sync::Arc;

#[derive(Debug)]
struct ParquetKvMetadataTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for ParquetKvMetadataTable {
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

/// `parquet_kv_metadata` table-valued function
///
/// Returns the key-value metadata stored in a Parquet file footer. Each row
/// represents one key-value pair.
///
/// Example:
/// ```sql
/// SELECT * FROM parquet_kv_metadata('file.parquet');
/// ```
#[derive(Debug)]
pub struct ParquetKvMetadataFunc {}

impl TableFunctionImpl for ParquetKvMetadataFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = match exprs.first() {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => s,
            Some(Expr::Column(Column { name, .. })) => name,
            _ => {
                return plan_err!("parquet_kv_metadata requires a string argument as its input");
            }
        };

        let file = File::open(filename.clone())?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
        ]));

        let mut filename_arr: Vec<Option<String>> = vec![];
        let mut key_arr: Vec<Option<String>> = vec![];
        let mut value_arr: Vec<Option<String>> = vec![];

        if let Some(kv_metadata) = metadata.file_metadata().key_value_metadata() {
            for kv in kv_metadata {
                filename_arr.push(Some(filename.clone()));
                key_arr.push(Some(kv.key.clone()));
                value_arr.push(kv.value.clone());
            }
        }

        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(filename_arr)),
                Arc::new(StringArray::from(key_arr)),
                Arc::new(StringArray::from(value_arr)),
            ],
        )?;

        Ok(Arc::new(ParquetKvMetadataTable { schema, batch: rb }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    use std::path::Path;

    /// Write a single-column parquet file at `path` with the given key-value metadata.
    fn write_parquet(path: &Path, kv_metadata: Option<Vec<KeyValue>>) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1i32]))])
                .unwrap();
        let props = WriterProperties::builder()
            .set_key_value_metadata(kv_metadata)
            .build();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("parquet_kv_metadata", Arc::new(ParquetKvMetadataFunc {}));
        ctx
    }

    /// Return the single string value at `(row, col)` from a flat result set.
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

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Basic schema: result has the expected column names.
    #[tokio::test]
    async fn test_schema_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(
            &path,
            Some(vec![KeyValue::new("k".into(), "v".to_string())]),
        );

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_kv_metadata('{}')", path.display());
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert!(!result.is_empty());

        let s = result[0].schema();
        for col in &["filename", "key", "value"] {
            assert!(s.field_with_name(col).is_ok(), "missing column: {col}");
        }
    }

    /// Custom key-value pairs are returned with their values.
    #[tokio::test]
    async fn test_custom_kv_pairs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(
            &path,
            Some(vec![
                KeyValue::new("writer".into(), "dft".to_string()),
                KeyValue::new("version".into(), "1.0".to_string()),
            ]),
        );

        let ctx = make_ctx();
        // The ArrowWriter also stores an ARROW:schema entry, so filter to our keys
        let sql = format!(
            "SELECT key, value FROM parquet_kv_metadata('{}') \
             WHERE key IN ('writer', 'version') ORDER BY key",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 2);
        assert_eq!(str_val(&result, 0, "key").as_deref(), Some("version"));
        assert_eq!(str_val(&result, 0, "value").as_deref(), Some("1.0"));
        assert_eq!(str_val(&result, 1, "key").as_deref(), Some("writer"));
        assert_eq!(str_val(&result, 1, "value").as_deref(), Some("dft"));
    }

    /// A key with no value yields a null value.
    #[tokio::test]
    async fn test_null_value() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(
            &path,
            Some(vec![KeyValue {
                key: "flag".to_string(),
                value: None,
            }]),
        );

        let ctx = make_ctx();
        let sql = format!(
            "SELECT key, value FROM parquet_kv_metadata('{}') WHERE key = 'flag'",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 1);
        assert_eq!(str_val(&result, 0, "key").as_deref(), Some("flag"));
        assert_eq!(str_val(&result, 0, "value"), None);
    }

    /// The filename column echoes the queried path.
    #[tokio::test]
    async fn test_filename_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(
            &path,
            Some(vec![KeyValue::new("k".into(), "v".to_string())]),
        );

        let ctx = make_ctx();
        let sql = format!(
            "SELECT filename FROM parquet_kv_metadata('{}') WHERE key = 'k'",
            path.display()
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 1);
        assert_eq!(
            str_val(&result, 0, "filename").as_deref(),
            Some(path.display().to_string().as_str())
        );
    }

    /// The ArrowWriter encodes the Arrow schema in the ARROW:schema key, and
    /// decoding it round-trips timestamp timezone information.
    #[tokio::test]
    async fn test_arrow_schema_timestamp_timezone() {
        use arrow::array::TimestampNanosecondArray;
        use arrow::datatypes::TimeUnit;
        use base64::prelude::{Engine, BASE64_STANDARD};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");

        let tz = "America/New_York";
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into())),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                TimestampNanosecondArray::from(vec![1_000_000_000i64]).with_timezone(tz),
            )],
        )
        .unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let ctx = make_ctx();
        let sql = format!(
            "SELECT value FROM parquet_kv_metadata('{}') WHERE key = '{}'",
            path.display(),
            parquet::arrow::ARROW_SCHEMA_META_KEY
        );
        let result = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        assert_eq!(total_rows(&result), 1);
        let encoded = str_val(&result, 0, "value").expect("ARROW:SCHEMA value should be non-null");

        // The value is a base64-encoded Arrow IPC schema message
        let decoded = BASE64_STANDARD.decode(encoded).unwrap();
        let arrow_schema = arrow::ipc::convert::try_schema_from_ipc_buffer(&decoded).unwrap();
        assert_eq!(
            arrow_schema.field_with_name("ts").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into()))
        );
    }

    /// Missing argument produces a plan error.
    #[tokio::test]
    async fn test_missing_argument() {
        let ctx = make_ctx();
        let err = ctx.sql("SELECT * FROM parquet_kv_metadata()").await;
        assert!(err.is_err());
    }
}

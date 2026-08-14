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
use arrow::array::{Int32Array, Int64Array, StringArray};
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
use parquet::basic::ConvertedType;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::schema::types::Type;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

#[derive(Debug)]
struct ParquetSchemaTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for ParquetSchemaTable {
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

/// One row of `parquet_schema` output describing a node of the schema tree
#[derive(Default)]
struct SchemaRow {
    path: Option<String>,
    name: String,
    node_type: String,
    depth: i64,
    repetition: Option<String>,
    physical_type: Option<String>,
    type_length: Option<i32>,
    precision: Option<i32>,
    scale: Option<i32>,
    logical_type: Option<String>,
    converted_type: Option<String>,
    field_id: Option<i32>,
    max_definition_level: Option<i32>,
    max_repetition_level: Option<i32>,
    num_children: Option<i64>,
}

/// Recursively visit `node` and its children, appending one row per node.
/// `parents` holds the path parts up to (but not including) `node`, with the
/// root message node excluded to match `path_in_schema` elsewhere.
fn visit(
    node: &Type,
    parents: &[String],
    depth: i64,
    leaf_levels: &HashMap<String, (i16, i16)>,
    rows: &mut Vec<SchemaRow>,
) {
    let info = node.get_basic_info();
    let mut path_parts = parents.to_vec();
    // The root node keeps an empty path
    if depth > 0 {
        path_parts.push(info.name().to_string());
    }
    let path = (depth > 0).then(|| path_parts.join("."));

    let mut row = SchemaRow {
        path: path.clone(),
        name: info.name().to_string(),
        depth,
        repetition: info.has_repetition().then(|| info.repetition().to_string()),
        logical_type: info.logical_type_ref().map(|lt| format!("{lt:?}")),
        converted_type: match info.converted_type() {
            ConvertedType::NONE => None,
            ct => Some(ct.to_string()),
        },
        field_id: info.has_id().then(|| info.id()),
        ..Default::default()
    };

    match node {
        Type::PrimitiveType {
            physical_type,
            type_length,
            scale,
            precision,
            ..
        } => {
            row.node_type = "PRIMITIVE".to_string();
            row.physical_type = Some(physical_type.to_string());
            row.type_length = (*type_length > 0).then_some(*type_length);
            row.precision = (*precision > 0).then_some(*precision);
            row.scale = (*scale > 0).then_some(*scale);
            if let Some((max_def, max_rep)) = path.as_deref().and_then(|p| leaf_levels.get(p)) {
                row.max_definition_level = Some(*max_def as i32);
                row.max_repetition_level = Some(*max_rep as i32);
            }
            rows.push(row);
        }
        Type::GroupType { fields, .. } => {
            row.node_type = "GROUP".to_string();
            row.num_children = Some(fields.len() as i64);
            rows.push(row);
            for field in fields {
                visit(field, &path_parts, depth + 1, leaf_levels, rows);
            }
        }
    }
}

/// `parquet_schema` table-valued function
///
/// Returns the raw Parquet schema tree (as opposed to the embedded Arrow
/// schema shown by `parquet_arrow_schema`) with one row per schema node in
/// depth-first order, starting with the root message node. Reports each
/// node's repetition, physical/logical/converted type, field id, and, for
/// leaf columns, the maximum definition and repetition levels.
///
/// Example:
/// ```sql
/// SELECT * FROM parquet_schema('file.parquet');
/// ```
#[derive(Debug)]
pub struct ParquetSchemaFunc {}

impl TableFunctionImpl for ParquetSchemaFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = expr_to_string(exprs.first(), "parquet_schema", "filename")?;

        let file = File::open(&filename)?;
        let reader = SerializedFileReader::new(file)?;
        let schema_descr = reader.metadata().file_metadata().schema_descr();

        let leaf_levels: HashMap<String, (i16, i16)> = schema_descr
            .columns()
            .iter()
            .map(|c| (c.path().string(), (c.max_def_level(), c.max_rep_level())))
            .collect();

        let mut rows: Vec<SchemaRow> = vec![];
        visit(schema_descr.root_schema(), &[], 0, &leaf_levels, &mut rows);

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("path", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("node_type", DataType::Utf8, true),
            Field::new("depth", DataType::Int64, true),
            Field::new("repetition", DataType::Utf8, true),
            Field::new("physical_type", DataType::Utf8, true),
            Field::new("type_length", DataType::Int32, true),
            Field::new("precision", DataType::Int32, true),
            Field::new("scale", DataType::Int32, true),
            Field::new("logical_type", DataType::Utf8, true),
            Field::new("converted_type", DataType::Utf8, true),
            Field::new("field_id", DataType::Int32, true),
            Field::new("max_definition_level", DataType::Int32, true),
            Field::new("max_repetition_level", DataType::Int32, true),
            Field::new("num_children", DataType::Int64, true),
        ]));

        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some(filename); rows.len()])),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.path.clone()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.name.clone()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.node_type.clone()).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.depth).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.repetition.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.physical_type.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.type_length).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.precision).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.scale).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.logical_type.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.converted_type.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.field_id).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    rows.iter()
                        .map(|r| r.max_definition_level)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    rows.iter()
                        .map(|r| r.max_repetition_level)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.num_children).collect::<Vec<_>>(),
                )),
            ],
        )?;

        Ok(Arc::new(ParquetSchemaTable { schema, batch: rb }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array as ArrowInt64Array, ListArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use std::path::Path;

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udtf("parquet_schema", Arc::new(ParquetSchemaFunc {}));
        ctx
    }

    fn write_parquet(path: &Path, batch: &RecordBatch) {
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    fn flat_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ArrowInt64Array::from(vec![1i64, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap()
    }

    fn nested_batch() -> RecordBatch {
        let list_field = Field::new(
            "vals",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        );
        let schema = Arc::new(Schema::new(vec![list_field]));
        let list = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
        ]);
        RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap()
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

    fn int32_col(batches: &[RecordBatch], col: &str) -> Vec<Option<i32>> {
        batches
            .iter()
            .flat_map(|batch| {
                let array = batch
                    .column(batch.schema().index_of(col).unwrap())
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                (0..array.len())
                    .map(|i| array.is_valid(i).then(|| array.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// A flat schema returns the root group followed by one primitive row per
    /// column, with repetition and physical type.
    #[tokio::test]
    async fn test_flat_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path, &flat_batch());

        let ctx = make_ctx();
        let sql = format!("SELECT * FROM parquet_schema('{}')", path.display());
        let batches = collect(&ctx, &sql).await;

        assert_eq!(
            string_col(&batches, "node_type"),
            vec![
                Some("GROUP".to_string()),
                Some("PRIMITIVE".to_string()),
                Some("PRIMITIVE".to_string()),
            ]
        );
        assert_eq!(
            string_col(&batches, "path"),
            vec![None, Some("v".to_string()), Some("s".to_string())]
        );
        assert_eq!(
            string_col(&batches, "repetition"),
            vec![
                None,
                Some("REQUIRED".to_string()),
                Some("OPTIONAL".to_string())
            ]
        );
        assert_eq!(
            string_col(&batches, "physical_type"),
            vec![
                None,
                Some("INT64".to_string()),
                Some("BYTE_ARRAY".to_string())
            ]
        );
        // Required leaf has def level 0, optional leaf 1; neither is repeated
        assert_eq!(
            int32_col(&batches, "max_definition_level"),
            vec![None, Some(0), Some(1)]
        );
        assert_eq!(
            int32_col(&batches, "max_repetition_level"),
            vec![None, Some(0), Some(0)]
        );
    }

    /// A nested list schema reports the intermediate group nodes and the leaf
    /// with repetition levels.
    #[tokio::test]
    async fn test_nested_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        write_parquet(&path, &nested_batch());

        let ctx = make_ctx();
        let sql = format!(
            "SELECT path, node_type, max_repetition_level FROM parquet_schema('{}') \
             WHERE node_type = 'PRIMITIVE'",
            path.display()
        );
        let batches = collect(&ctx, &sql).await;
        assert_eq!(
            string_col(&batches, "path"),
            vec![Some("vals.list.item".to_string())]
        );
        assert_eq!(int32_col(&batches, "max_repetition_level"), vec![Some(1)]);

        // The tree includes the group nodes above the leaf
        let sql = format!(
            "SELECT count(*) AS c FROM parquet_schema('{}') WHERE node_type = 'GROUP'",
            path.display()
        );
        let batches = collect(&ctx, &sql).await;
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ArrowInt64Array>()
            .unwrap()
            .value(0);
        // root, vals, and the repeated "list" wrapper
        assert_eq!(count, 3);
    }

    /// A missing filename argument produces a plan error.
    #[tokio::test]
    async fn test_missing_argument() {
        let ctx = make_ctx();
        let err = ctx
            .sql("SELECT * FROM parquet_schema()")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("requires a string filename argument"), "{err}");
    }
}

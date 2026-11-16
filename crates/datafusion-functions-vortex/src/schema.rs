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

use crate::common::{check_max_args, expr_to_string, mem_table, VortexInput};
use arrow::array::{BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::sync::Arc;
use vortex::dtype::DType;

const NAME: &str = "vortex_schema";

/// One row of the flattened DType tree
#[derive(Default)]
struct Rows {
    path: Vec<String>,
    name: Vec<String>,
    depth: Vec<i64>,
    kind: Vec<&'static str>,
    dtype: Vec<String>,
    nullable: Vec<bool>,
    ptype: Vec<Option<String>>,
    precision: Vec<Option<i32>>,
    scale: Vec<Option<i32>>,
    list_size: Vec<Option<i64>>,
    extension_id: Vec<Option<String>>,
    storage_dtype: Vec<Option<String>>,
    num_children: Vec<i64>,
}

/// Children of a DType node with their names
fn dtype_children(dtype: &DType) -> Vec<(String, DType)> {
    match dtype {
        DType::Struct(fields, _) => fields
            .names()
            .iter()
            .map(|n| n.to_string())
            .zip(fields.fields())
            .collect(),
        DType::List(element, _) | DType::FixedSizeList(element, _, _) => {
            vec![("element".to_string(), element.as_ref().clone())]
        }
        DType::Map(map, _) => vec![
            ("key".to_string(), map.key_dtype()),
            ("value".to_string(), map.value_dtype()),
        ],
        DType::Union(variants, _) => variants
            .names()
            .iter()
            .map(|n| n.to_string())
            .zip(variants.variants())
            .collect(),
        _ => vec![],
    }
}

fn dtype_kind(dtype: &DType) -> &'static str {
    match dtype {
        DType::Null => "null",
        DType::Bool(_) => "bool",
        DType::Primitive(..) => "primitive",
        DType::Decimal(..) => "decimal",
        DType::Utf8(_) => "utf8",
        DType::Binary(_) => "binary",
        DType::List(..) => "list",
        DType::FixedSizeList(..) => "fixed_size_list",
        DType::Map(..) => "map",
        DType::Struct(..) => "struct",
        DType::Union(..) => "union",
        DType::Variant(_) => "variant",
        DType::Extension(_) => "extension",
    }
}

fn visit(dtype: &DType, path: &[String], name: String, depth: i64, rows: &mut Rows) {
    let children = dtype_children(dtype);
    rows.path.push(path.join("."));
    rows.name.push(name);
    rows.depth.push(depth);
    rows.kind.push(dtype_kind(dtype));
    rows.dtype.push(dtype.to_string());
    rows.nullable.push(dtype.is_nullable());
    rows.ptype.push(match dtype {
        DType::Primitive(ptype, _) => Some(ptype.to_string()),
        _ => None,
    });
    let (precision, scale) = match dtype {
        DType::Decimal(decimal, _) => (
            Some(i32::from(decimal.precision())),
            Some(i32::from(decimal.scale())),
        ),
        _ => (None, None),
    };
    rows.precision.push(precision);
    rows.scale.push(scale);
    rows.list_size.push(match dtype {
        DType::FixedSizeList(_, size, _) => Some(i64::from(*size)),
        _ => None,
    });
    let (extension_id, storage_dtype) = match dtype {
        DType::Extension(ext) => (
            Some(ext.id().to_string()),
            Some(ext.storage_dtype().to_string()),
        ),
        _ => (None, None),
    };
    rows.extension_id.push(extension_id);
    rows.storage_dtype.push(storage_dtype);
    rows.num_children.push(children.len() as i64);

    for (child_name, child) in children {
        let mut child_path = path.to_vec();
        child_path.push(child_name.clone());
        visit(&child, &child_path, child_name, depth + 1, rows);
    }
}

/// `vortex_schema` table-valued function
///
/// Returns one row per node of the file's DType tree in depth-first order.
/// The root node has depth 0 and an empty path. Struct fields, list
/// elements (`element`), map keys and values (`key`, `value`), and union
/// variants are child nodes. Only the footer is read.
///
/// Example:
/// ```sql
/// SELECT path, dtype, nullable FROM vortex_schema('file.vortex');
/// ```
#[derive(Debug)]
pub struct VortexSchemaFunc {}

impl TableFunctionImpl for VortexSchemaFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        check_max_args(exprs, 1, NAME)?;
        let filename = expr_to_string(exprs.first(), NAME, "filename")?;
        let input = VortexInput::open(&filename, false)?;

        let mut rows = Rows::default();
        visit(input.file.dtype(), &[], String::new(), 0, &mut rows);
        let n = rows.path.len();

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("depth", DataType::Int64, false),
            Field::new("type_kind", DataType::Utf8, false),
            Field::new("dtype", DataType::Utf8, false),
            Field::new("nullable", DataType::Boolean, false),
            Field::new("ptype", DataType::Utf8, true),
            Field::new("precision", DataType::Int32, true),
            Field::new("scale", DataType::Int32, true),
            Field::new("list_size", DataType::Int64, true),
            Field::new("extension_id", DataType::Utf8, true),
            Field::new("storage_dtype", DataType::Utf8, true),
            Field::new("num_children", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![filename; n])),
                Arc::new(StringArray::from(rows.path)),
                Arc::new(StringArray::from(rows.name)),
                Arc::new(Int64Array::from(rows.depth)),
                Arc::new(StringArray::from(rows.kind)),
                Arc::new(StringArray::from(rows.dtype)),
                Arc::new(BooleanArray::from(rows.nullable)),
                Arc::new(StringArray::from(rows.ptype)),
                Arc::new(Int32Array::from(rows.precision)),
                Arc::new(Int32Array::from(rows.scale)),
                Arc::new(Int64Array::from(rows.list_size)),
                Arc::new(StringArray::from(rows.extension_id)),
                Arc::new(StringArray::from(rows.storage_dtype)),
                Arc::new(Int64Array::from(rows.num_children)),
            ],
        )?;
        mem_table(schema, batch)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::*;
    use arrow::array::{
        ArrayRef, Decimal128Array, Int32Array, ListArray, RecordBatch, StructArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema, TimeUnit};
    use std::sync::Arc;

    /// Flat columns produce a root node plus one node per field.
    #[tokio::test]
    async fn test_flat_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!("SELECT * FROM vortex_schema('{path}') ORDER BY depth, path"),
        )
        .await;
        assert_eq!(
            column_strings(&batches, "path"),
            vec![
                Some("".into()),
                Some("category".into()),
                Some("id".into()),
                Some("value".into())
            ]
        );
        assert_eq!(
            column_strings(&batches, "type_kind"),
            vec![
                Some("struct".into()),
                Some("utf8".into()),
                Some("primitive".into()),
                Some("primitive".into())
            ]
        );
        assert_eq!(
            column_strings(&batches, "nullable"),
            vec![
                Some("false".into()),
                Some("false".into()),
                Some("false".into()),
                Some("true".into())
            ]
        );
        assert_eq!(
            column_strings(&batches, "ptype"),
            vec![None, None, Some("i64".into()), Some("f64".into())]
        );
        assert_eq!(column_i64(&batches, "num_children")[0], Some(3));
    }

    /// Nested, decimal, and extension types are flattened with their details.
    #[tokio::test]
    async fn test_nested_schema() {
        let point_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, true),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("point", DataType::Struct(point_fields.clone()), false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            ),
            Field::new("amount", DataType::Decimal128(10, 2), false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));
        let point = StructArray::new(
            point_fields,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(3), None])),
            ],
            None,
        );
        let tags = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
        ]);
        let amount = Decimal128Array::from(vec![100, 250])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let ts = TimestampMicrosecondArray::from(vec![0, 1_000_000]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(point),
                Arc::new(tags),
                Arc::new(amount),
                Arc::new(ts),
            ],
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.vortex");
        write_vortex(&path, batch);

        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!(
                "SELECT path, depth, type_kind, precision, scale, extension_id FROM vortex_schema('{}') ORDER BY path",
                path.display()
            ),
        )
        .await;
        let paths = column_strings(&batches, "path");
        let kinds = column_strings(&batches, "type_kind");
        let find = |p: &str| paths.iter().position(|x| x.as_deref() == Some(p)).unwrap();

        assert_eq!(kinds[find("point")].as_deref(), Some("struct"));
        assert_eq!(column_i64(&batches, "depth")[find("point.x")], Some(2));
        assert_eq!(kinds[find("tags")].as_deref(), Some("list"));
        assert_eq!(kinds[find("tags.element")].as_deref(), Some("primitive"));
        assert_eq!(kinds[find("amount")].as_deref(), Some("decimal"));
        assert_eq!(column_i64(&batches, "precision")[find("amount")], Some(10));
        assert_eq!(column_i64(&batches, "scale")[find("amount")], Some(2));
        assert_eq!(kinds[find("ts")].as_deref(), Some("extension"));
        assert!(column_strings(&batches, "extension_id")[find("ts")]
            .as_deref()
            .unwrap()
            .contains("timestamp"));
    }

    /// A single-column file has a root node and one field node.
    #[tokio::test]
    async fn test_single_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.vortex");
        write_vortex(&path, batch);
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!("SELECT * FROM vortex_schema('{}')", path.display()),
        )
        .await;
        assert_eq!(num_rows(&batches), 2);
    }
}

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

use crate::common::{
    check_column_exists, check_max_args, expr_to_string, mem_table, path_matches, walk_layouts,
    VortexInput,
};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::sync::Arc;
use vortex::layout::layouts::dict::Dict;

const NAME: &str = "vortex_dictionary";

/// `vortex_dictionary` table-valued function
///
/// Returns the values of every dictionary layout (`vortex.dict`) of a column
/// and its nested fields: one row per dictionary entry, in dictionary order.
/// A column can have several dictionaries, for example one per chunk;
/// `node_id` identifies the dictionary layout (see `vortex_layout`) and
/// `row_offset` and `row_count` give the file rows that it covers. Values are
/// formatted as strings.
///
/// Only layout-level dictionaries are returned. Dictionary encoding chosen by
/// the compressor inside a flat segment appears in `vortex_encodings` as the
/// `vortex.dict` encoding instead.
///
/// Example:
/// ```sql
/// SELECT dictionary_index, value FROM vortex_dictionary('file.vortex', 'category');
/// ```
#[derive(Debug)]
pub struct VortexDictionaryFunc {}

impl TableFunctionImpl for VortexDictionaryFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        check_max_args(exprs, 2, NAME)?;
        let filename = expr_to_string(exprs.first(), NAME, "filename")?;
        let column = expr_to_string(exprs.get(1), NAME, "column")?;
        let input = VortexInput::open(&filename, false)?;
        let nodes = walk_layouts(input.file.footer().layout())?;
        check_column_exists(&nodes, &column, NAME)?;

        let mut node_id = Vec::new();
        let mut path = Vec::new();
        let mut row_offset = Vec::new();
        let mut row_count = Vec::new();
        let mut index = Vec::new();
        let mut value = Vec::new();
        for node in nodes
            .iter()
            .filter(|n| n.layout.is::<Dict>() && path_matches(&n.path(), &column))
        {
            let children = node.layout.children().map_err(crate::common::vx_err)?;
            // Slot 0 holds the dictionary values and slot 1 the codes
            let Some(values_layout) = children.first() else {
                continue;
            };
            let values = input.array_to_strings(input.read_layout(values_layout)?)?;
            for (i, v) in values.into_iter().enumerate() {
                node_id.push(node.node_id as i64);
                path.push(node.path());
                row_offset.push(node.row_offset.map(|r| r as i64));
                row_count.push(node.layout.row_count() as i64);
                index.push(i as i64);
                value.push(v);
            }
        }
        let n = node_id.len();

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, false),
            Field::new("node_id", DataType::Int64, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("row_offset", DataType::Int64, true),
            Field::new("row_count", DataType::Int64, false),
            Field::new("dictionary_index", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![filename; n])),
                Arc::new(Int64Array::from(node_id)),
                Arc::new(StringArray::from(path)),
                Arc::new(Int64Array::from(row_offset)),
                Arc::new(Int64Array::from(row_count)),
                Arc::new(Int64Array::from(index)),
                Arc::new(StringArray::from(value)),
            ],
        )?;
        mem_table(schema, batch)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::*;

    /// The low-cardinality column has dictionaries that hold its three values.
    #[tokio::test]
    async fn test_dictionary_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!(
                "SELECT DISTINCT value FROM vortex_dictionary('{path}', 'category') ORDER BY value"
            ),
        )
        .await;
        assert_eq!(
            column_strings(&batches, "value"),
            CATEGORIES
                .iter()
                .map(|c| Some(c.to_string()))
                .collect::<Vec<_>>()
        );

        // Dictionaries cover every row of the column exactly once
        let coverage = run(
            &ctx,
            &format!(
                "SELECT sum(row_count) AS rows FROM \
                 (SELECT DISTINCT node_id, row_count FROM vortex_dictionary('{path}', 'category'))"
            ),
        )
        .await;
        assert_eq!(
            column_i64(&coverage, "rows"),
            vec![Some(STANDARD_ROWS as i64)]
        );
    }

    /// A column without dictionary layouts returns no rows.
    #[tokio::test]
    async fn test_no_dictionary() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!("SELECT * FROM vortex_dictionary('{path}', 'id')"),
        )
        .await;
        assert_eq!(num_rows(&batches), 0);
    }

    /// The column argument is required and must exist.
    #[tokio::test]
    async fn test_bad_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let err = run_err(&ctx, &format!("SELECT * FROM vortex_dictionary('{path}')")).await;
        assert!(err.contains("requires a string column argument"), "{err}");
        let err = run_err(
            &ctx,
            &format!("SELECT * FROM vortex_dictionary('{path}', 'nope')"),
        )
        .await;
        assert!(err.contains("does not exist"), "{err}");
    }
}

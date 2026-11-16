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
    check_column_exists, check_max_args, expr_to_string, mem_table, optional_expr_to_string,
    path_matches, walk_layouts, LayoutNode, VortexInput,
};
use arrow::array::{BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use vortex::array::ArrayRef;
use vortex::layout::layouts::flat::Flat;

const NAME: &str = "vortex_encodings";

/// Formats the encoding-specific metadata of one array node
struct MetadataDisplay<'a>(&'a ArrayRef);

impl Display for MetadataDisplay<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.metadata_fmt(f)
    }
}

/// One row per array node
#[derive(Default)]
pub(crate) struct EncodingRows {
    pub node_id: Vec<i64>,
    pub path: Vec<String>,
    pub role: Vec<String>,
    pub row_offset: Vec<Option<i64>>,
    pub array_node_id: Vec<i64>,
    pub array_parent_id: Vec<Option<i64>>,
    pub array_path: Vec<String>,
    pub depth: Vec<i64>,
    pub encoding: Vec<String>,
    pub dtype: Vec<String>,
    pub len: Vec<i64>,
    pub num_children: Vec<i64>,
    pub num_buffers: Vec<i64>,
    pub buffer_bytes: Vec<i64>,
    pub nbytes: Vec<i64>,
    pub metadata: Vec<Option<String>>,
    pub is_canonical: Vec<bool>,
}

impl EncodingRows {
    /// Append the array tree of `array`, decoded from the flat layout `node`,
    /// in depth-first pre-order
    fn push_tree(&mut self, node: &LayoutNode, array: ArrayRef) {
        let path = node.path();
        // (array, parent array node id, array path, depth)
        let mut stack: Vec<(ArrayRef, Option<i64>, String, i64)> =
            vec![(array, None, String::new(), 0)];
        let mut next_id = 0i64;
        while let Some((array, parent, array_path, depth)) = stack.pop() {
            let id = next_id;
            next_id += 1;
            let buffer_bytes: usize = array.buffer_handles().iter().map(|b| b.len()).sum();
            let metadata = MetadataDisplay(&array).to_string();

            self.node_id.push(node.node_id as i64);
            self.path.push(path.clone());
            self.role.push(node.role.clone());
            self.row_offset.push(node.row_offset.map(|r| r as i64));
            self.array_node_id.push(id);
            self.array_parent_id.push(parent);
            self.depth.push(depth);
            self.encoding.push(array.encoding_id().to_string());
            self.dtype.push(array.dtype().to_string());
            self.len.push(array.len() as i64);
            self.num_children.push(array.nchildren() as i64);
            self.num_buffers.push(array.nbuffers() as i64);
            self.buffer_bytes.push(buffer_bytes as i64);
            self.nbytes.push(array.nbytes() as i64);
            self.metadata
                .push((!metadata.is_empty()).then_some(metadata));
            self.is_canonical.push(array.is_canonical());

            let children = array.named_children();
            for (name, child) in children.into_iter().rev() {
                let child_path = if array_path.is_empty() {
                    name
                } else {
                    format!("{array_path}.{name}")
                };
                stack.push((child, Some(id), child_path, depth + 1));
            }
            self.array_path.push(array_path);
        }
    }
}

/// Decode the array trees of the flat layouts that match `column`
pub(crate) fn collect_encodings(
    input: &VortexInput,
    nodes: &[LayoutNode],
    column: Option<&str>,
) -> Result<EncodingRows> {
    let flats: Vec<&LayoutNode> = nodes
        .iter()
        .filter(|n| n.layout.is::<Flat>())
        .filter(|n| column.is_none_or(|c| path_matches(&n.path(), c)))
        .collect();
    let layouts: Vec<_> = flats.iter().map(|n| &n.layout).collect();
    let mut rows = EncodingRows::default();
    input.for_each_flat(&layouts, |i, array| {
        rows.push_tree(flats[i], array);
        Ok(())
    })?;
    Ok(rows)
}

/// `vortex_encodings` table-valued function
///
/// Returns the array encoding tree of every flat layout (the leaf segments
/// that hold data) in a Vortex file. Each flat segment is decoded into its
/// encoded array tree, without decompression, and each array node becomes
/// one row in depth-first pre-order. This shows the cascading compression
/// that the writer chose, for example `fastlanes.bitpacked` under
/// `fastlanes.for`, or `vortex.dict` with `fsst` values.
///
/// - `node_id` is the flat layout node (see `vortex_layout`).
/// - `array_node_id` and `array_parent_id` identify nodes within one flat
///   segment. `array_path` is the path of slot names from the root array.
/// - `buffer_bytes` counts the buffers of the node only. `nbytes` counts the
///   node and all of its children.
/// - `metadata` is the encoding-specific metadata, for example the bit width.
///
/// An optional second argument limits the output to one column path and its
/// nested fields. This function reads every matching flat segment, so its
/// I/O is proportional to the size of the selected columns.
///
/// Examples:
/// ```sql
/// SELECT path, encoding, count(*) FROM vortex_encodings('file.vortex') GROUP BY 1, 2;
/// SELECT * FROM vortex_encodings('file.vortex', 'id');
/// ```
#[derive(Debug)]
pub struct VortexEncodingsFunc {}

impl TableFunctionImpl for VortexEncodingsFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        check_max_args(exprs, 2, NAME)?;
        let filename = expr_to_string(exprs.first(), NAME, "filename")?;
        let column = optional_expr_to_string(exprs.get(1), NAME, "column")?;
        let input = VortexInput::open(&filename, false)?;
        let nodes = walk_layouts(input.file.footer().layout())?;
        if let Some(column) = &column {
            check_column_exists(&nodes, column, NAME)?;
        }
        let rows = collect_encodings(&input, &nodes, column.as_deref())?;
        let n = rows.node_id.len();

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, false),
            Field::new("node_id", DataType::Int64, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("role", DataType::Utf8, false),
            Field::new("row_offset", DataType::Int64, true),
            Field::new("array_node_id", DataType::Int64, false),
            Field::new("array_parent_id", DataType::Int64, true),
            Field::new("array_path", DataType::Utf8, false),
            Field::new("depth", DataType::Int64, false),
            Field::new("encoding", DataType::Utf8, false),
            Field::new("dtype", DataType::Utf8, false),
            Field::new("len", DataType::Int64, false),
            Field::new("num_children", DataType::Int64, false),
            Field::new("num_buffers", DataType::Int64, false),
            Field::new("buffer_bytes", DataType::Int64, false),
            Field::new("nbytes", DataType::Int64, false),
            Field::new("metadata", DataType::Utf8, true),
            Field::new("is_canonical", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![filename; n])),
                Arc::new(Int64Array::from(rows.node_id)),
                Arc::new(StringArray::from(rows.path)),
                Arc::new(StringArray::from(rows.role)),
                Arc::new(Int64Array::from(rows.row_offset)),
                Arc::new(Int64Array::from(rows.array_node_id)),
                Arc::new(Int64Array::from(rows.array_parent_id)),
                Arc::new(StringArray::from(rows.array_path)),
                Arc::new(Int64Array::from(rows.depth)),
                Arc::new(StringArray::from(rows.encoding)),
                Arc::new(StringArray::from(rows.dtype)),
                Arc::new(Int64Array::from(rows.len)),
                Arc::new(Int64Array::from(rows.num_children)),
                Arc::new(Int64Array::from(rows.num_buffers)),
                Arc::new(Int64Array::from(rows.buffer_bytes)),
                Arc::new(Int64Array::from(rows.nbytes)),
                Arc::new(StringArray::from(rows.metadata)),
                Arc::new(BooleanArray::from(rows.is_canonical)),
            ],
        )?;
        mem_table(schema, batch)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::*;

    /// Root array nodes cover every data row, and node sizes are consistent.
    #[tokio::test]
    async fn test_encoding_trees() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();

        // The root arrays of the data segments of `id` cover all rows
        let roots = run(
            &ctx,
            &format!(
                "SELECT sum(len) AS rows FROM vortex_encodings('{path}', 'id') \
                 WHERE array_parent_id IS NULL AND role = 'data'"
            ),
        )
        .await;
        assert_eq!(column_i64(&roots, "rows"), vec![Some(STANDARD_ROWS as i64)]);

        // nbytes of a root equals the sum of buffer_bytes of its tree
        let sizes = run(
            &ctx,
            &format!(
                "SELECT node_id, \
                   sum(buffer_bytes) AS buffers, \
                   max(CASE WHEN array_parent_id IS NULL THEN nbytes END) AS root \
                 FROM vortex_encodings('{path}') GROUP BY node_id"
            ),
        )
        .await;
        assert_eq!(column_i64(&sizes, "buffers"), column_i64(&sizes, "root"));

        // A sorted integer column compresses to a non-canonical encoding
        let id_roots = run(
            &ctx,
            &format!(
                "SELECT encoding, is_canonical FROM vortex_encodings('{path}', 'id') \
                 WHERE array_parent_id IS NULL AND role = 'data'"
            ),
        )
        .await;
        for canonical in column_strings(&id_roots, "is_canonical") {
            assert_eq!(canonical.as_deref(), Some("false"));
        }

        // Every parent id refers to a node in the same tree
        let orphans = run(
            &ctx,
            &format!(
                "SELECT count(*) AS c FROM vortex_encodings('{path}') c \
                 LEFT JOIN vortex_encodings('{path}') p \
                   ON c.node_id = p.node_id AND c.array_parent_id = p.array_node_id \
                 WHERE c.array_parent_id IS NOT NULL AND p.array_node_id IS NULL"
            ),
        )
        .await;
        assert_eq!(column_i64(&orphans, "c"), vec![Some(0)]);
    }

    /// The column filter limits the output to one field.
    #[tokio::test]
    async fn test_column_filter() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!("SELECT DISTINCT path FROM vortex_encodings('{path}', 'category')"),
        )
        .await;
        assert_eq!(
            column_strings(&batches, "path"),
            vec![Some("category".into())]
        );
        let err = run_err(
            &ctx,
            &format!("SELECT * FROM vortex_encodings('{path}', 'missing')"),
        )
        .await;
        assert!(err.contains("does not exist"), "{err}");
    }
}

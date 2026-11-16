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
    check_max_args, expr_to_string, mem_table, segment_owners, walk_layouts, VortexInput,
};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::sync::Arc;

const NAME: &str = "vortex_segments";

/// `vortex_segments` table-valued function
///
/// Returns one row per segment (contiguous byte range) of a Vortex file,
/// ordered by file offset, with the layout node that owns it:
///
/// - `role` is the purpose of the owning layout (`data`, `zone_map`,
///   `dict_values`, `validity`) or `metadata` for user-defined metadata
///   segments.
/// - `segment_id`, `node_id`, `path`, and `layout_encoding` are null for
///   metadata segments. `metadata_key` is set only for them.
///
/// Only the footer is read.
///
/// Example:
/// ```sql
/// SELECT role, count(*), sum(length) FROM vortex_segments('file.vortex') GROUP BY role;
/// ```
#[derive(Debug)]
pub struct VortexSegmentsFunc {}

impl TableFunctionImpl for VortexSegmentsFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        check_max_args(exprs, 1, NAME)?;
        let filename = expr_to_string(exprs.first(), NAME, "filename")?;
        let input = VortexInput::open(&filename, false)?;
        let footer = input.file.footer();
        let nodes = walk_layouts(footer.layout())?;
        let owners = segment_owners(&nodes);

        let mut segment_id = Vec::new();
        let mut offset = Vec::new();
        let mut length = Vec::new();
        let mut alignment = Vec::new();
        let mut node_id = Vec::new();
        let mut path = Vec::new();
        let mut encoding = Vec::new();
        let mut role = Vec::new();
        let mut metadata_key = Vec::new();

        for (id, spec) in footer.segment_map().iter().enumerate() {
            let owner = owners.get(&(id as u32)).map(|&i| &nodes[i]);
            segment_id.push(Some(id as i64));
            offset.push(spec.offset as i64);
            length.push(i64::from(spec.length));
            alignment.push(usize::from(spec.alignment) as i64);
            node_id.push(owner.map(|n| n.node_id as i64));
            path.push(owner.map(|n| n.path()));
            encoding.push(owner.map(|n| n.layout.encoding_id().to_string()));
            role.push(owner.map(|n| n.role.clone()));
            metadata_key.push(None);
        }
        for (key, spec) in footer.metadata_segments() {
            segment_id.push(None);
            offset.push(spec.offset as i64);
            length.push(i64::from(spec.length));
            alignment.push(usize::from(spec.alignment) as i64);
            node_id.push(None);
            path.push(None);
            encoding.push(None);
            role.push(Some("metadata".to_string()));
            metadata_key.push(Some(key.to_string()));
        }

        // Order by file offset so metadata segments appear where they are stored
        let mut order: Vec<usize> = (0..offset.len()).collect();
        order.sort_by_key(|&i| offset[i]);
        let pick = |v: &Vec<Option<String>>| -> Vec<Option<String>> {
            order.iter().map(|&i| v[i].clone()).collect()
        };
        let n = order.len();

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, false),
            Field::new("segment_id", DataType::Int64, true),
            Field::new("offset", DataType::Int64, false),
            Field::new("length", DataType::Int64, false),
            Field::new("alignment", DataType::Int64, false),
            Field::new("node_id", DataType::Int64, true),
            Field::new("path", DataType::Utf8, true),
            Field::new("layout_encoding", DataType::Utf8, true),
            Field::new("role", DataType::Utf8, true),
            Field::new("metadata_key", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![filename; n])),
                Arc::new(Int64Array::from_iter(order.iter().map(|&i| segment_id[i]))),
                Arc::new(Int64Array::from_iter_values(
                    order.iter().map(|&i| offset[i]),
                )),
                Arc::new(Int64Array::from_iter_values(
                    order.iter().map(|&i| length[i]),
                )),
                Arc::new(Int64Array::from_iter_values(
                    order.iter().map(|&i| alignment[i]),
                )),
                Arc::new(Int64Array::from_iter(order.iter().map(|&i| node_id[i]))),
                Arc::new(StringArray::from(pick(&path))),
                Arc::new(StringArray::from(pick(&encoding))),
                Arc::new(StringArray::from(pick(&role))),
                Arc::new(StringArray::from(pick(&metadata_key))),
            ],
        )?;
        mem_table(schema, batch)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::*;

    /// Segments are ordered, do not overlap, and are owned by layout nodes.
    #[tokio::test]
    async fn test_segments() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let file_size = std::fs::metadata(&path).unwrap().len() as i64;
        let ctx = make_ctx();
        let batches = run(&ctx, &format!("SELECT * FROM vortex_segments('{path}')")).await;
        let offsets = column_i64(&batches, "offset");
        let lengths = column_i64(&batches, "length");
        assert!(!offsets.is_empty());
        for i in 1..offsets.len() {
            let prev_end = offsets[i - 1].unwrap() + lengths[i - 1].unwrap();
            assert!(offsets[i].unwrap() >= prev_end, "segment {i} overlaps");
        }
        let last = offsets.len() - 1;
        assert!(offsets[last].unwrap() + lengths[last].unwrap() <= file_size);
        for node_id in column_i64(&batches, "node_id") {
            assert!(node_id.is_some());
        }
        for alignment in column_i64(&batches, "alignment") {
            let a = alignment.unwrap();
            assert!(
                a > 0 && (a & (a - 1)) == 0,
                "alignment {a} is not a power of two"
            );
        }

        // Data and zone map segments are both present
        let roles = run(
            &ctx,
            &format!("SELECT DISTINCT role FROM vortex_segments('{path}') ORDER BY role"),
        )
        .await;
        let roles = column_strings(&roles, "role");
        assert!(roles.contains(&Some("data".into())), "{roles:?}");
        assert!(roles.contains(&Some("zone_map".into())), "{roles:?}");

        // Node ids join with vortex_layout
        let joined = run(
            &ctx,
            &format!(
                "SELECT count(*) AS c FROM vortex_segments('{path}') s \
                 JOIN vortex_layout('{path}') l ON s.node_id = l.node_id \
                 WHERE array_has(l.segment_ids, s.segment_id)"
            ),
        )
        .await;
        assert_eq!(column_i64(&joined, "c"), vec![Some(offsets.len() as i64)]);
    }

    /// Metadata segments are listed with their keys.
    #[tokio::test]
    async fn test_metadata_segments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.vortex");
        write_vortex_with_metadata(&path, standard_batch(), &[("owner", b"dft")]);
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!(
                "SELECT segment_id, length, metadata_key FROM vortex_segments('{}') \
                 WHERE role = 'metadata'",
                path.display()
            ),
        )
        .await;
        assert_eq!(
            column_strings(&batches, "metadata_key"),
            vec![Some("owner".into())]
        );
        assert_eq!(column_i64(&batches, "segment_id"), vec![None]);
        assert_eq!(column_i64(&batches, "length"), vec![Some(3)]);
    }
}

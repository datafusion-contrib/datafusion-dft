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
    check_max_args, expr_to_string, mem_table, walk_layouts, zone_info, VortexInput,
};
use arrow::array::{Int64Array, Int64Builder, ListBuilder, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::sync::Arc;

const NAME: &str = "vortex_layout";

/// `vortex_layout` table-valued function
///
/// Returns one row per node of the file's layout tree in depth-first
/// pre-order. `node_id` is stable for a given file and matches the node ids
/// of `vortex_segments`, `vortex_zone_map`, `vortex_encodings`, and
/// `vortex_dictionary`.
///
/// - `child_type` is `root`, `field`, `chunk`, `transparent`, or `auxiliary`.
/// - `role` is `data`, `zone_map`, `dict_values`, `validity`, or the name of
///   another auxiliary child. Children inherit the role of their parent.
/// - `row_offset` is the absolute first row in the file. It is null inside
///   auxiliary children, whose rows are not file rows.
/// - `zone_len`, `num_zones`, and `zone_aggregates` are set for zoned layouts.
///
/// Only the footer is read.
///
/// Example:
/// ```sql
/// SELECT node_id, parent_id, path, encoding, row_count FROM vortex_layout('file.vortex');
/// ```
#[derive(Debug)]
pub struct VortexLayoutFunc {}

impl TableFunctionImpl for VortexLayoutFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        check_max_args(exprs, 1, NAME)?;
        let filename = expr_to_string(exprs.first(), NAME, "filename")?;
        let input = VortexInput::open(&filename, false)?;
        let footer = input.file.footer();
        let segment_map = footer.segment_map();
        let nodes = walk_layouts(footer.layout())?;
        let n = nodes.len();

        let mut segment_ids = ListBuilder::new(Int64Builder::new());
        let mut segment_bytes = Vec::with_capacity(n);
        let mut zone_len = Vec::with_capacity(n);
        let mut num_zones = Vec::with_capacity(n);
        let mut zone_aggregates = Vec::with_capacity(n);
        for node in &nodes {
            let ids = node.layout.segment_ids();
            let mut bytes = 0i64;
            for id in &ids {
                segment_ids.values().append_value(i64::from(**id));
                if let Some(spec) = segment_map.get(**id as usize) {
                    bytes += i64::from(spec.length);
                }
            }
            segment_ids.append(true);
            segment_bytes.push(bytes);
            let info = zone_info(&node.layout);
            zone_len.push(info.as_ref().map(|z| z.zone_len as i64));
            num_zones.push(info.as_ref().map(|z| z.num_zones as i64));
            zone_aggregates.push(info.map(|z| z.aggregates.join(", ")));
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, false),
            Field::new("node_id", DataType::Int64, false),
            Field::new("parent_id", DataType::Int64, true),
            Field::new("depth", DataType::Int64, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("child_type", DataType::Utf8, false),
            Field::new("role", DataType::Utf8, false),
            Field::new("encoding", DataType::Utf8, false),
            Field::new("dtype", DataType::Utf8, false),
            Field::new("row_offset", DataType::Int64, true),
            Field::new("row_count", DataType::Int64, false),
            Field::new("num_children", DataType::Int64, false),
            Field::new(
                "segment_ids",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
                true,
            ),
            Field::new("segment_bytes", DataType::Int64, false),
            Field::new("metadata_bytes", DataType::Int64, false),
            Field::new("zone_len", DataType::Int64, true),
            Field::new("num_zones", DataType::Int64, true),
            Field::new("zone_aggregates", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![filename; n])),
                Arc::new(Int64Array::from_iter_values(
                    nodes.iter().map(|x| x.node_id as i64),
                )),
                Arc::new(Int64Array::from_iter(
                    nodes.iter().map(|x| x.parent_id.map(|p| p as i64)),
                )),
                Arc::new(Int64Array::from_iter_values(
                    nodes.iter().map(|x| x.depth as i64),
                )),
                Arc::new(StringArray::from_iter_values(
                    nodes.iter().map(|x| x.path()),
                )),
                Arc::new(StringArray::from_iter_values(
                    nodes.iter().map(|x| x.name.as_str()),
                )),
                Arc::new(StringArray::from_iter_values(
                    nodes.iter().map(|x| x.child_type),
                )),
                Arc::new(StringArray::from_iter_values(
                    nodes.iter().map(|x| x.role.as_str()),
                )),
                Arc::new(StringArray::from_iter_values(
                    nodes.iter().map(|x| x.layout.encoding_id().to_string()),
                )),
                Arc::new(StringArray::from_iter_values(
                    nodes.iter().map(|x| x.layout.dtype().to_string()),
                )),
                Arc::new(Int64Array::from_iter(
                    nodes.iter().map(|x| x.row_offset.map(|r| r as i64)),
                )),
                Arc::new(Int64Array::from_iter_values(
                    nodes.iter().map(|x| x.layout.row_count() as i64),
                )),
                Arc::new(Int64Array::from_iter_values(
                    nodes.iter().map(|x| x.layout.nchildren() as i64),
                )),
                Arc::new(segment_ids.finish()),
                Arc::new(Int64Array::from(segment_bytes)),
                Arc::new(Int64Array::from_iter_values(
                    nodes.iter().map(|x| x.layout.metadata().len() as i64),
                )),
                Arc::new(Int64Array::from(zone_len)),
                Arc::new(Int64Array::from(num_zones)),
                Arc::new(StringArray::from(zone_aggregates)),
            ],
        )?;
        mem_table(schema, batch)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::*;

    /// The tree has a struct root, one subtree per field, and zoned layouts
    /// with the expected zone count.
    #[tokio::test]
    async fn test_layout_tree() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!("SELECT * FROM vortex_layout('{path}') ORDER BY node_id"),
        )
        .await;
        let n = num_rows(&batches);
        assert!(n > 4);

        let node_ids = column_i64(&batches, "node_id");
        assert_eq!(node_ids, (0..n as i64).map(Some).collect::<Vec<_>>());
        let parents = column_i64(&batches, "parent_id");
        assert_eq!(parents[0], None);
        // Pre-order: every parent precedes its children
        for (i, p) in parents.iter().enumerate().skip(1) {
            assert!(p.unwrap() < i as i64);
        }

        let encodings = column_strings(&batches, "encoding");
        assert_eq!(encodings[0].as_deref(), Some("vortex.struct"));
        assert_eq!(
            column_i64(&batches, "row_count")[0],
            Some(STANDARD_ROWS as i64)
        );
        assert_eq!(column_i64(&batches, "row_offset")[0], Some(0));

        // Each column has a zoned layout with three zones of 8192 rows
        let zoned = run(
            &ctx,
            &format!(
                "SELECT path, zone_len, num_zones, zone_aggregates FROM vortex_layout('{path}') \
                 WHERE encoding = 'vortex.zoned' ORDER BY path"
            ),
        )
        .await;
        assert_eq!(
            column_strings(&zoned, "path"),
            vec![
                Some("category".into()),
                Some("id".into()),
                Some("value".into())
            ]
        );
        assert_eq!(column_i64(&zoned, "zone_len"), vec![Some(8192); 3]);
        assert_eq!(column_i64(&zoned, "num_zones"), vec![Some(3); 3]);
        for aggs in column_strings(&zoned, "zone_aggregates") {
            let aggs = aggs.unwrap();
            assert!(aggs.contains("max") && aggs.contains("min"), "{aggs}");
        }

        // Zone map children are auxiliary and have no file row offset
        let zone_maps = run(
            &ctx,
            &format!(
                "SELECT child_type, row_offset FROM vortex_layout('{path}') \
                 WHERE role = 'zone_map' AND name = 'zones'"
            ),
        )
        .await;
        assert_eq!(num_rows(&zone_maps), 3);
        assert_eq!(
            column_strings(&zone_maps, "child_type"),
            vec![Some("auxiliary".into()); 3]
        );
        assert_eq!(column_i64(&zone_maps, "row_offset"), vec![None; 3]);

        // Flat layouts own segments, and the sum of segment bytes covers the data
        let sums = run(
            &ctx,
            &format!(
                "SELECT sum(segment_bytes) AS s, sum(cardinality(segment_ids)) AS c \
                 FROM vortex_layout('{path}')"
            ),
        )
        .await;
        let meta = run(
            &ctx,
            &format!("SELECT data_size, num_segments FROM vortex_file_metadata('{path}')"),
        )
        .await;
        assert_eq!(column_i64(&sums, "s"), column_i64(&meta, "data_size"));
        assert_eq!(column_i64(&sums, "c"), column_i64(&meta, "num_segments"));
    }
}

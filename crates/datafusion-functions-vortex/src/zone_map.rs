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
    array_to_arrow, arrow_to_strings, check_column_exists, check_max_args, expr_to_string,
    mem_table, optional_expr_to_string, path_matches, short_aggregate_name, walk_layouts,
    zone_info, VortexInput,
};
use arrow::array::{Array, AsArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::exec_err;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::sync::Arc;

const NAME: &str = "vortex_zone_map";

#[derive(Default)]
struct Rows {
    node_id: Vec<i64>,
    path: Vec<String>,
    zone_id: Vec<i64>,
    first_row: Vec<Option<i64>>,
    num_rows: Vec<i64>,
    aggregate: Vec<String>,
    aggregate_id: Vec<String>,
    value: Vec<Option<String>>,
}

/// `vortex_zone_map` table-valued function
///
/// Returns the zone maps (per-zone pruning statistics) of a Vortex file in
/// long format: one row per zoned layout, zone, and aggregate. The set of
/// aggregates depends on the column type and the writer, for example
/// `min`, `max`, `null_count`, and `sum`. `aggregate` is the short name and
/// `aggregate_id` is the name stored in the file (for example
/// `vortex.max()`). Values are formatted as strings.
///
/// - `node_id` is the zoned layout node (see `vortex_layout`).
/// - `first_row` is the absolute first row of the zone in the file.
///
/// An optional second argument limits the output to one column path and
/// its nested fields. Only
/// the footer and the zone map segments are read.
///
/// Examples:
/// ```sql
/// SELECT * FROM vortex_zone_map('file.vortex');
/// SELECT zone_id, aggregate, value FROM vortex_zone_map('file.vortex', 'id');
/// ```
#[derive(Debug)]
pub struct VortexZoneMapFunc {}

impl TableFunctionImpl for VortexZoneMapFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        check_max_args(exprs, 2, NAME)?;
        let filename = expr_to_string(exprs.first(), NAME, "filename")?;
        let column = optional_expr_to_string(exprs.get(1), NAME, "column")?;
        let input = VortexInput::open(&filename, false)?;
        let nodes = walk_layouts(input.file.footer().layout())?;
        if let Some(column) = &column {
            check_column_exists(&nodes, column, NAME)?;
        }

        let mut rows = Rows::default();
        for node in &nodes {
            let path = node.path();
            if column.as_ref().is_some_and(|c| !path_matches(&path, c)) {
                continue;
            }
            let Some(info) = zone_info(&node.layout) else {
                continue;
            };
            if info.zone_len == 0 || info.num_zones == 0 {
                continue;
            }
            let children = node.layout.children().map_err(crate::common::vx_err)?;
            let Some(zones_layout) = children.get(1) else {
                return exec_err!("zoned layout {} has no zone map child", node.node_id);
            };
            let zones = input.read_layout(zones_layout)?;
            let zones = array_to_arrow(&input.session, zones)?;
            let Some(zones) = zones.as_struct_opt() else {
                return exec_err!("zone map of layout {} is not a struct", node.node_id);
            };
            let aggregates: Vec<(String, Vec<Option<String>>)> = zones
                .column_names()
                .into_iter()
                .map(String::from)
                .zip(zones.columns())
                .map(|(name, col)| Ok((name, arrow_to_strings(col.as_ref())?)))
                .collect::<Result<_>>()?;

            let row_count = node.layout.row_count();
            for zone in 0..zones.len() {
                let start = zone as u64 * info.zone_len;
                let len = info.zone_len.min(row_count.saturating_sub(start));
                for (name, values) in &aggregates {
                    rows.node_id.push(node.node_id as i64);
                    rows.path.push(path.clone());
                    rows.zone_id.push(zone as i64);
                    rows.first_row
                        .push(node.row_offset.map(|offset| (offset + start) as i64));
                    rows.num_rows.push(len as i64);
                    rows.aggregate.push(short_aggregate_name(name));
                    rows.aggregate_id.push(name.clone());
                    rows.value.push(values[zone].clone());
                }
            }
        }

        let n = rows.node_id.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, false),
            Field::new("node_id", DataType::Int64, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("zone_id", DataType::Int64, false),
            Field::new("first_row", DataType::Int64, true),
            Field::new("num_rows", DataType::Int64, false),
            Field::new("aggregate", DataType::Utf8, false),
            Field::new("aggregate_id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![filename; n])),
                Arc::new(Int64Array::from(rows.node_id)),
                Arc::new(StringArray::from(rows.path)),
                Arc::new(Int64Array::from(rows.zone_id)),
                Arc::new(Int64Array::from(rows.first_row)),
                Arc::new(Int64Array::from(rows.num_rows)),
                Arc::new(StringArray::from(rows.aggregate)),
                Arc::new(StringArray::from(rows.aggregate_id)),
                Arc::new(StringArray::from(rows.value)),
            ],
        )?;
        mem_table(schema, batch)
    }
}

#[cfg(test)]
mod tests {
    use crate::common::short_aggregate_name;
    use crate::test_util::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// The `id` column has three zones whose bounds match the written data.
    #[tokio::test]
    async fn test_id_zone_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!(
                "SELECT zone_id, first_row, num_rows, aggregate, value \
                 FROM vortex_zone_map('{path}', 'id') ORDER BY zone_id, aggregate"
            ),
        )
        .await;
        let aggs = column_strings(&batches, "aggregate");
        let values = column_strings(&batches, "value");
        let zones = column_i64(&batches, "zone_id");
        let lookup = |zone: i64, prefix: &str| {
            (0..aggs.len())
                .find(|&i| zones[i] == Some(zone) && aggs[i].as_deref() == Some(prefix))
                .map(|i| values[i].clone().unwrap())
                .unwrap_or_else(|| panic!("no {prefix} for zone {zone}: {aggs:?}"))
        };
        assert_eq!(lookup(0, "min"), "0");
        assert_eq!(lookup(0, "max"), "8191");
        assert_eq!(lookup(1, "min"), "8192");
        assert_eq!(lookup(1, "max"), "16383");
        assert_eq!(lookup(2, "min"), "16384");
        assert_eq!(lookup(2, "max"), "19999");

        let first = run(
            &ctx,
            &format!(
                "SELECT DISTINCT zone_id, first_row, num_rows \
                 FROM vortex_zone_map('{path}', 'id') ORDER BY zone_id"
            ),
        )
        .await;
        assert_eq!(
            column_i64(&first, "first_row"),
            vec![Some(0), Some(8192), Some(16384)]
        );
        assert_eq!(
            column_i64(&first, "num_rows"),
            vec![Some(8192), Some(8192), Some(3616)]
        );
    }

    /// Aggregate ids keep the stored name and aggregates use the short name.
    #[tokio::test]
    async fn test_aggregate_names() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!(
                "SELECT DISTINCT aggregate, aggregate_id FROM vortex_zone_map('{path}', 'id') \
                 ORDER BY aggregate"
            ),
        )
        .await;
        let aggs = column_strings(&batches, "aggregate");
        let ids = column_strings(&batches, "aggregate_id");
        assert!(aggs.contains(&Some("min".into())), "{aggs:?}");
        assert!(aggs.contains(&Some("max".into())), "{aggs:?}");
        assert!(ids.contains(&Some("vortex.min()".into())), "{ids:?}");
    }

    /// Without a column argument every zoned column is returned.
    #[tokio::test]
    async fn test_all_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!("SELECT DISTINCT path FROM vortex_zone_map('{path}') ORDER BY path"),
        )
        .await;
        assert_eq!(
            column_strings(&batches, "path"),
            vec![
                Some("category".into()),
                Some("id".into()),
                Some("value".into())
            ]
        );
    }

    /// Null counts per zone add up to the column null count.
    #[tokio::test]
    async fn test_null_counts_sum() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!(
                "SELECT sum(CAST(value AS BIGINT)) AS nulls FROM vortex_zone_map('{path}', 'value') \
                 WHERE aggregate = 'null_count'"
            ),
        )
        .await;
        assert_eq!(column_i64(&batches, "nulls"), vec![Some(2858)]);
    }

    /// A file smaller than one zone has a single short zone.
    #[tokio::test]
    async fn test_single_zone() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![5, -3, 9]))]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.vortex");
        write_vortex(&path, batch);
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!(
                "SELECT DISTINCT zone_id, num_rows FROM vortex_zone_map('{}')",
                path.display()
            ),
        )
        .await;
        // Small files may skip zone maps entirely; when present there is one zone
        if num_rows(&batches) > 0 {
            assert_eq!(column_i64(&batches, "zone_id"), vec![Some(0)]);
            assert_eq!(column_i64(&batches, "num_rows"), vec![Some(3)]);
        }
    }

    /// Short names strip the `vortex.` prefix and empty argument lists only.
    #[test]
    fn test_short_aggregate_name() {
        assert_eq!(short_aggregate_name("vortex.max()"), "max");
        assert_eq!(
            short_aggregate_name("vortex.bounded_max(64)"),
            "bounded_max(64)"
        );
        assert_eq!(short_aggregate_name("custom.agg()"), "custom.agg");
        assert_eq!(short_aggregate_name(""), "");
    }

    /// An unknown column produces an error.
    #[tokio::test]
    async fn test_unknown_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let err = run_err(
            &ctx,
            &format!("SELECT * FROM vortex_zone_map('{path}', 'nope')"),
        )
        .await;
        assert!(err.contains("column 'nope' does not exist"), "{err}");
    }
}

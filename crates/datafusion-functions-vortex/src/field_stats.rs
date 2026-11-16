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
    check_max_args, expr_to_string, mem_table, segment_owners, top_level_fields, walk_layouts,
    VortexInput,
};
use arrow::array::{BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::sync::Arc;
use vortex::array::expr::stats::{Precision, Stat};
use vortex::array::stats::StatsSet;
use vortex::dtype::DType;
use vortex::scalar::Scalar;

const NAME: &str = "vortex_field_stats";

/// Read `stat` as a typed scalar. Returns the scalar and whether it is exact.
fn stat_scalar(stats: &StatsSet, stat: Stat, dtype: &DType) -> Option<(Scalar, bool)> {
    let stat_dtype = stat.dtype(dtype)?;
    let (value, exact) = match stats.get(stat) {
        Precision::Exact(v) => (v, true),
        Precision::Inexact(v) => (v, false),
        Precision::Absent => return None,
    };
    Scalar::try_new(stat_dtype, Some(value))
        .ok()
        .map(|s| (s, exact))
}

fn stat_u64(stats: &StatsSet, stat: Stat, dtype: &DType) -> Option<i64> {
    stat_scalar(stats, stat, dtype)
        .and_then(|(s, _)| u64::try_from(&s).ok())
        .map(|v| v as i64)
}

fn stat_bool(stats: &StatsSet, stat: Stat, dtype: &DType) -> Option<bool> {
    stat_scalar(stats, stat, dtype).and_then(|(s, _)| bool::try_from(&s).ok())
}

/// `vortex_field_stats` table-valued function
///
/// Returns one row per top-level field with the file-level statistics that
/// the writer stored in the footer (min, max, sum, null count, NaN count, and
/// others when present) and the compressed size of the field. Min and max of
/// variable-length types can be truncated by the writer; `min_exact` and
/// `max_exact` are false in that case. Only the footer is read.
///
/// Example:
/// ```sql
/// SELECT field_name, min, max, null_count FROM vortex_field_stats('file.vortex');
/// ```
#[derive(Debug)]
pub struct VortexFieldStatsFunc {}

impl TableFunctionImpl for VortexFieldStatsFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        check_max_args(exprs, 1, NAME)?;
        let filename = expr_to_string(exprs.first(), NAME, "filename")?;
        let input = VortexInput::open(&filename, false)?;
        let footer = input.file.footer();
        let fields = top_level_fields(footer.dtype());

        // Compressed size per top-level field from segment ownership
        let nodes = walk_layouts(footer.layout())?;
        let mut compressed = vec![0i64; fields.len()];
        for (segment_id, node_idx) in segment_owners(&nodes) {
            let node = &nodes[node_idx];
            let field_idx = match node.field_path.first() {
                Some(name) => fields.iter().position(|(n, _)| n == name),
                None if fields.len() == 1 && fields[0].0.is_empty() => Some(0),
                None => None,
            };
            if let (Some(i), Some(spec)) =
                (field_idx, footer.segment_map().get(segment_id as usize))
            {
                compressed[i] += i64::from(spec.length);
            }
        }

        let n = fields.len();
        let mut min = Vec::with_capacity(n);
        let mut max = Vec::with_capacity(n);
        let mut min_exact = Vec::with_capacity(n);
        let mut max_exact = Vec::with_capacity(n);
        let mut sum = Vec::with_capacity(n);
        let mut null_count = Vec::with_capacity(n);
        let mut nan_count = Vec::with_capacity(n);
        let mut uncompressed = Vec::with_capacity(n);
        let mut is_constant = Vec::with_capacity(n);
        let mut is_sorted = Vec::with_capacity(n);
        let mut is_strict_sorted = Vec::with_capacity(n);
        let file_stats = footer.statistics();

        for (idx, (_, field_dtype)) in fields.iter().enumerate() {
            let stats = file_stats
                .filter(|s| idx < s.stats_sets().len())
                .map(|s| s.get(idx));
            let Some((stats, stats_dtype)) = stats else {
                min.push(None);
                max.push(None);
                min_exact.push(None);
                max_exact.push(None);
                sum.push(None);
                null_count.push(None);
                nan_count.push(None);
                uncompressed.push(None);
                is_constant.push(None);
                is_sorted.push(None);
                is_strict_sorted.push(None);
                continue;
            };
            debug_assert_eq!(stats_dtype, field_dtype);
            let bound = |stat: Stat| -> Result<(Option<String>, Option<bool>)> {
                match stat_scalar(stats, stat, stats_dtype) {
                    Some((scalar, exact)) => Ok((input.scalar_to_string(scalar)?, Some(exact))),
                    None => Ok((None, None)),
                }
            };
            let (v, e) = bound(Stat::Min)?;
            min.push(v);
            min_exact.push(e);
            let (v, e) = bound(Stat::Max)?;
            max.push(v);
            max_exact.push(e);
            sum.push(bound(Stat::Sum)?.0);
            null_count.push(stat_u64(stats, Stat::NullCount, stats_dtype));
            nan_count.push(stat_u64(stats, Stat::NaNCount, stats_dtype));
            uncompressed.push(stat_u64(stats, Stat::UncompressedSizeInBytes, stats_dtype));
            is_constant.push(stat_bool(stats, Stat::IsConstant, stats_dtype));
            is_sorted.push(stat_bool(stats, Stat::IsSorted, stats_dtype));
            is_strict_sorted.push(stat_bool(stats, Stat::IsStrictSorted, stats_dtype));
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, false),
            Field::new("field_id", DataType::Int64, false),
            Field::new("field_name", DataType::Utf8, false),
            Field::new("dtype", DataType::Utf8, false),
            Field::new("compressed_size", DataType::Int64, false),
            Field::new("min", DataType::Utf8, true),
            Field::new("max", DataType::Utf8, true),
            Field::new("min_exact", DataType::Boolean, true),
            Field::new("max_exact", DataType::Boolean, true),
            Field::new("sum", DataType::Utf8, true),
            Field::new("null_count", DataType::Int64, true),
            Field::new("nan_count", DataType::Int64, true),
            Field::new("uncompressed_size", DataType::Int64, true),
            Field::new("is_constant", DataType::Boolean, true),
            Field::new("is_sorted", DataType::Boolean, true),
            Field::new("is_strict_sorted", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![filename; n])),
                Arc::new(Int64Array::from_iter_values(0..n as i64)),
                Arc::new(StringArray::from_iter_values(fields.iter().map(|(f, _)| f))),
                Arc::new(StringArray::from_iter_values(
                    fields.iter().map(|(_, d)| d.to_string()),
                )),
                Arc::new(Int64Array::from(compressed)),
                Arc::new(StringArray::from(min)),
                Arc::new(StringArray::from(max)),
                Arc::new(BooleanArray::from(min_exact)),
                Arc::new(BooleanArray::from(max_exact)),
                Arc::new(StringArray::from(sum)),
                Arc::new(Int64Array::from(null_count)),
                Arc::new(Int64Array::from(nan_count)),
                Arc::new(Int64Array::from(uncompressed)),
                Arc::new(BooleanArray::from(is_constant)),
                Arc::new(BooleanArray::from(is_sorted)),
                Arc::new(BooleanArray::from(is_strict_sorted)),
            ],
        )?;
        mem_table(schema, batch)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::*;
    use arrow::array::{Float64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Min, max, and null counts match the written data.
    #[tokio::test]
    async fn test_stats_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!("SELECT * FROM vortex_field_stats('{path}') ORDER BY field_id"),
        )
        .await;
        assert_eq!(
            column_strings(&batches, "field_name"),
            vec![
                Some("id".into()),
                Some("category".into()),
                Some("value".into())
            ]
        );
        let min = column_strings(&batches, "min");
        let max = column_strings(&batches, "max");
        assert_eq!(min[0].as_deref(), Some("0"));
        assert_eq!(max[0].as_deref(), Some("19999"));
        assert_eq!(min[1].as_deref(), Some("alpha"));
        assert_eq!(max[1].as_deref(), Some("gamma"));
        // Row 0 is null, so the smallest value is row 1 (0.5)
        assert_eq!(min[2].as_deref(), Some("0.5"));
        // Row 19999 is null (19999 = 7 * 2857), so the largest value is row 19998
        assert_eq!(max[2].as_deref(), Some("9999.0"));

        let nulls = column_i64(&batches, "null_count");
        assert_eq!(nulls[0], Some(0));
        assert_eq!(nulls[1], Some(0));
        // Every seventh row of 20000 is null: ceil(20000 / 7)
        assert_eq!(nulls[2], Some(2858));

        let sum = column_strings(&batches, "sum");
        assert_eq!(sum[0].as_deref(), Some("199990000"));

        for size in column_i64(&batches, "compressed_size") {
            assert!(size.unwrap() > 0);
        }
    }

    /// Long strings produce truncated, inexact min and max values.
    #[tokio::test]
    async fn test_truncated_string_stats() {
        let long_a = "a".repeat(200);
        let long_z = "z".repeat(200);
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                long_a.as_str(),
                long_z.as_str(),
            ]))],
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.vortex");
        write_vortex(&path, batch);

        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!(
                "SELECT min, max, min_exact, max_exact FROM vortex_field_stats('{}')",
                path.display()
            ),
        )
        .await;
        let min = column_strings(&batches, "min")[0].clone().unwrap();
        let max = column_strings(&batches, "max")[0].clone().unwrap();
        assert!(min.len() < 200 && long_a.starts_with(&min), "{min}");
        assert!(
            max.len() < 200 && max > long_z.clone().split_off(199),
            "{max}"
        );
        assert_eq!(
            column_strings(&batches, "min_exact")[0].as_deref(),
            Some("false")
        );
        assert_eq!(
            column_strings(&batches, "max_exact")[0].as_deref(),
            Some("false")
        );
    }

    /// NaN values are counted separately from nulls.
    #[tokio::test]
    async fn test_nan_count() {
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(f64::NAN),
                None,
                Some(f64::NAN),
                Some(f64::INFINITY),
            ]))],
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.vortex");
        write_vortex(&path, batch);
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!(
                "SELECT null_count, nan_count FROM vortex_field_stats('{}')",
                path.display()
            ),
        )
        .await;
        assert_eq!(column_i64(&batches, "null_count"), vec![Some(1)]);
        assert_eq!(column_i64(&batches, "nan_count"), vec![Some(2)]);
    }

    /// An all-null column has a null min and max.
    #[tokio::test]
    async fn test_all_null_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![None, None, None]))],
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.vortex");
        write_vortex(&path, batch);
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!(
                "SELECT min, max, null_count FROM vortex_field_stats('{}')",
                path.display()
            ),
        )
        .await;
        assert_eq!(column_strings(&batches, "min"), vec![None]);
        assert_eq!(column_strings(&batches, "max"), vec![None]);
        assert_eq!(column_i64(&batches, "null_count"), vec![Some(3)]);
    }
}

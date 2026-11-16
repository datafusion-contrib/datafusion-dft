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
    VortexInput, ROLE_DATA, ROLE_DICT_VALUES, ROLE_ZONE_MAP,
};
use crate::encodings::collect_encodings;
use arrow::array::{Array, AsArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::collections::BTreeSet;
use std::sync::Arc;

const NAME: &str = "vortex_compression";

/// Index of the top-level field that a field path belongs to
fn field_index(fields: &[(String, vortex::dtype::DType)], path: &[String]) -> Option<usize> {
    match path.first() {
        Some(name) => fields.iter().position(|(n, _)| n == name),
        None if fields.len() == 1 && fields[0].0.is_empty() => Some(0),
        None => None,
    }
}

/// `vortex_compression` table-valued function
///
/// Returns one row per top-level field with its compressed size on disk,
/// split by purpose, its uncompressed size, the compression ratio, and the
/// distinct array encodings used by its data.
///
/// - `compressed_size` is the sum of all segments of the field.
///   `data_size`, `zone_map_size`, `dict_values_size`, and `other_size`
///   (for example validity) split it by role.
/// - `uncompressed_size` is the in-memory size of the field as Arrow arrays.
/// - `compression_ratio` is `uncompressed_size / compressed_size`.
///
/// This function decompresses the whole file once to measure the
/// uncompressed size and decodes every flat segment to list the encodings,
/// so its cost is proportional to the file size.
///
/// Example:
/// ```sql
/// SELECT field_name, compressed_size, uncompressed_size, compression_ratio, encodings
/// FROM vortex_compression('file.vortex');
/// ```
#[derive(Debug)]
pub struct VortexCompressionFunc {}

impl TableFunctionImpl for VortexCompressionFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        check_max_args(exprs, 1, NAME)?;
        let filename = expr_to_string(exprs.first(), NAME, "filename")?;
        let input = VortexInput::open(&filename, false)?;
        let footer = input.file.footer();
        let fields = top_level_fields(footer.dtype());
        let n = fields.len();
        let nodes = walk_layouts(footer.layout())?;

        // Compressed bytes per field and role
        let mut data = vec![0i64; n];
        let mut zone_map = vec![0i64; n];
        let mut dict_values = vec![0i64; n];
        let mut other = vec![0i64; n];
        for (segment_id, node_idx) in segment_owners(&nodes) {
            let node = &nodes[node_idx];
            let (Some(i), Some(spec)) = (
                field_index(&fields, &node.field_path),
                footer.segment_map().get(segment_id as usize),
            ) else {
                continue;
            };
            let len = i64::from(spec.length);
            match node.role.as_str() {
                ROLE_DATA => data[i] += len,
                ROLE_ZONE_MAP => zone_map[i] += len,
                ROLE_DICT_VALUES => dict_values[i] += len,
                _ => other[i] += len,
            }
        }

        // Uncompressed bytes per field, measured as Arrow arrays
        let mut uncompressed = vec![0i64; n];
        let is_struct = footer.dtype().as_struct_fields_opt().is_some();
        input.scan_arrow(None, |chunk| {
            if is_struct {
                for (i, column) in chunk.as_struct().columns().iter().enumerate() {
                    uncompressed[i] += column.to_data().get_slice_memory_size()? as i64;
                }
            } else {
                uncompressed[0] += chunk.to_data().get_slice_memory_size()? as i64;
            }
            Ok(())
        })?;

        // Distinct encodings of the data arrays per field
        let mut encodings = vec![BTreeSet::new(); n];
        let rows = collect_encodings(&input, &nodes, None)?;
        for (row, encoding) in rows.encoding.iter().enumerate() {
            if rows.role[row] != ROLE_DATA && rows.role[row] != ROLE_DICT_VALUES {
                continue;
            }
            let path: Vec<String> = if rows.path[row].is_empty() {
                vec![]
            } else {
                rows.path[row].split('.').map(String::from).collect()
            };
            if let Some(i) = field_index(&fields, &path) {
                encodings[i].insert(encoding.clone());
            }
        }

        let compressed: Vec<i64> = (0..n)
            .map(|i| data[i] + zone_map[i] + dict_values[i] + other[i])
            .collect();
        let ratio: Vec<Option<f64>> = (0..n)
            .map(|i| (compressed[i] > 0).then(|| uncompressed[i] as f64 / compressed[i] as f64))
            .collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, false),
            Field::new("field_id", DataType::Int64, false),
            Field::new("field_name", DataType::Utf8, false),
            Field::new("dtype", DataType::Utf8, false),
            Field::new("num_rows", DataType::Int64, false),
            Field::new("compressed_size", DataType::Int64, false),
            Field::new("data_size", DataType::Int64, false),
            Field::new("zone_map_size", DataType::Int64, false),
            Field::new("dict_values_size", DataType::Int64, false),
            Field::new("other_size", DataType::Int64, false),
            Field::new("uncompressed_size", DataType::Int64, false),
            Field::new("compression_ratio", DataType::Float64, true),
            Field::new("encodings", DataType::Utf8, false),
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
                Arc::new(Int64Array::from(vec![footer.row_count() as i64; n])),
                Arc::new(Int64Array::from(compressed)),
                Arc::new(Int64Array::from(data)),
                Arc::new(Int64Array::from(zone_map)),
                Arc::new(Int64Array::from(dict_values)),
                Arc::new(Int64Array::from(other)),
                Arc::new(Int64Array::from(uncompressed)),
                Arc::new(Float64Array::from(ratio)),
                Arc::new(StringArray::from_iter_values(
                    encodings
                        .iter()
                        .map(|e| e.iter().cloned().collect::<Vec<_>>().join(", ")),
                )),
            ],
        )?;
        mem_table(schema, batch)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::*;

    /// Sizes add up and a sorted integer column compresses well.
    #[tokio::test]
    async fn test_compression() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!("SELECT * FROM vortex_compression('{path}') ORDER BY field_id"),
        )
        .await;
        assert_eq!(num_rows(&batches), 3);
        let compressed = column_i64(&batches, "compressed_size");
        let parts: Vec<i64> = [
            "data_size",
            "zone_map_size",
            "dict_values_size",
            "other_size",
        ]
        .iter()
        .map(|c| column_i64(&batches, c).iter().map(|v| v.unwrap()).sum())
        .collect();
        let total: i64 = compressed.iter().map(|v| v.unwrap()).sum();
        assert_eq!(parts.iter().sum::<i64>(), total);

        // Compressed sizes match the file-level data size
        let meta = run(
            &ctx,
            &format!("SELECT data_size FROM vortex_file_metadata('{path}')"),
        )
        .await;
        assert_eq!(column_i64(&meta, "data_size"), vec![Some(total)]);

        // id: 20000 i64 values are 160000 bytes uncompressed
        let uncompressed = column_i64(&batches, "uncompressed_size");
        assert_eq!(uncompressed[0], Some(160_000));
        let ratio: f64 = column_strings(&batches, "compression_ratio")[0]
            .as_deref()
            .unwrap()
            .parse()
            .unwrap();
        assert!(ratio > 2.0, "ratio {ratio}");

        for encodings in column_strings(&batches, "encodings") {
            assert!(!encodings.unwrap().is_empty());
        }
    }
}

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

use crate::common::{check_max_args, expr_to_string, mem_table, walk_layouts, VortexInput};
use arrow::array::{BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::sync::Arc;

const NAME: &str = "vortex_file_metadata";

/// `vortex_file_metadata` table-valued function
///
/// Returns a single summary row for a Vortex file: row count, file, footer,
/// and data sizes, segment and field counts, the root DType, and a summary of
/// the layout tree. Only the footer is read.
///
/// Example:
/// ```sql
/// SELECT * FROM vortex_file_metadata('file.vortex');
/// ```
#[derive(Debug)]
pub struct VortexFileMetadataFunc {}

impl TableFunctionImpl for VortexFileMetadataFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        check_max_args(exprs, 1, NAME)?;
        let filename = expr_to_string(exprs.first(), NAME, "filename")?;
        let input = VortexInput::open(&filename, false)?;
        let footer = input.file.footer();

        let data_size: u64 = footer
            .segment_map()
            .iter()
            .map(|s| u64::from(s.length))
            .sum();
        let segments_end = footer
            .segment_map()
            .iter()
            .chain(footer.metadata_segments().map(|(_, s)| s))
            .map(|s| s.offset + u64::from(s.length))
            .max()
            .unwrap_or(0);
        let footer_size = input.file_size.saturating_sub(segments_end);
        let num_fields = footer
            .dtype()
            .as_struct_fields_opt()
            .map(|f| f.names().len())
            .unwrap_or(1);

        let nodes = walk_layouts(footer.layout())?;
        let count = |id: &str| {
            nodes
                .iter()
                .filter(|n| n.layout.encoding_id().as_str() == id)
                .count() as i64
        };
        let layout_depth = nodes.iter().map(|n| n.depth).max().unwrap_or(0) as i64;

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, false),
            Field::new("num_rows", DataType::Int64, false),
            Field::new("file_size", DataType::Int64, false),
            Field::new("footer_size", DataType::Int64, false),
            Field::new("data_size", DataType::Int64, false),
            Field::new("num_segments", DataType::Int64, false),
            Field::new("num_fields", DataType::Int64, false),
            Field::new("dtype", DataType::Utf8, false),
            Field::new("root_layout_encoding", DataType::Utf8, false),
            Field::new("num_layouts", DataType::Int64, false),
            Field::new("layout_depth", DataType::Int64, false),
            Field::new("num_flat_layouts", DataType::Int64, false),
            Field::new("num_chunked_layouts", DataType::Int64, false),
            Field::new("num_zoned_layouts", DataType::Int64, false),
            Field::new("num_dict_layouts", DataType::Int64, false),
            Field::new("has_file_statistics", DataType::Boolean, false),
            Field::new("num_metadata_segments", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![filename])),
                Arc::new(Int64Array::from(vec![footer.row_count() as i64])),
                Arc::new(Int64Array::from(vec![input.file_size as i64])),
                Arc::new(Int64Array::from(vec![footer_size as i64])),
                Arc::new(Int64Array::from(vec![data_size as i64])),
                Arc::new(Int64Array::from(vec![footer.segment_map().len() as i64])),
                Arc::new(Int64Array::from(vec![num_fields as i64])),
                Arc::new(StringArray::from(vec![footer.dtype().to_string()])),
                Arc::new(StringArray::from(vec![footer
                    .layout()
                    .encoding_id()
                    .to_string()])),
                Arc::new(Int64Array::from(vec![nodes.len() as i64])),
                Arc::new(Int64Array::from(vec![layout_depth])),
                Arc::new(Int64Array::from(vec![count("vortex.flat")])),
                Arc::new(Int64Array::from(vec![count("vortex.chunked")])),
                Arc::new(Int64Array::from(vec![
                    count("vortex.zoned") + count("vortex.stats"),
                ])),
                Arc::new(Int64Array::from(vec![count("vortex.dict")])),
                Arc::new(BooleanArray::from(vec![footer.statistics().is_some()])),
                Arc::new(Int64Array::from(vec![
                    footer.metadata_segments().count() as i64
                ])),
            ],
        )?;
        mem_table(schema, batch)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::*;

    /// The summary row reports counts, sizes, and layout structure.
    #[tokio::test]
    async fn test_summary_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let file_size = std::fs::metadata(&path).unwrap().len() as i64;

        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!("SELECT * FROM vortex_file_metadata('{path}')"),
        )
        .await;
        assert_eq!(num_rows(&batches), 1);
        let get = |c: &str| column_i64(&batches, c)[0].unwrap();

        assert_eq!(get("num_rows"), STANDARD_ROWS as i64);
        assert_eq!(get("file_size"), file_size);
        assert_eq!(get("num_fields"), 3);
        assert!(get("num_segments") > 0);
        assert!(get("data_size") > 0);
        assert!(get("footer_size") > 0);
        // The remainder is the file header and segment alignment padding
        assert!(get("footer_size") + get("data_size") <= file_size);
        assert!(get("num_flat_layouts") > 0);
        assert!(get("num_zoned_layouts") >= 3);
        assert!(get("num_layouts") > get("num_flat_layouts"));
        assert_eq!(get("num_metadata_segments"), 0);
        assert_eq!(
            column_strings(&batches, "root_layout_encoding")[0].as_deref(),
            Some("vortex.struct")
        );
        assert_eq!(
            column_strings(&batches, "has_file_statistics")[0].as_deref(),
            Some("true")
        );
    }

    /// Metadata segments are counted.
    #[tokio::test]
    async fn test_metadata_segments_counted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.vortex");
        write_vortex_with_metadata(&path, standard_batch(), &[("a", b"1"), ("b", b"2")]);
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!(
                "SELECT num_metadata_segments FROM vortex_file_metadata('{}')",
                path.display()
            ),
        )
        .await;
        assert_eq!(column_i64(&batches, "num_metadata_segments"), vec![Some(2)]);
    }

    /// A non-Vortex file produces an error.
    #[tokio::test]
    async fn test_not_vortex() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.txt");
        std::fs::write(&path, "not a vortex file").unwrap();
        let ctx = make_ctx();
        let err = run_err(
            &ctx,
            &format!("SELECT * FROM vortex_file_metadata('{}')", path.display()),
        )
        .await;
        assert!(!err.is_empty());
    }

    /// An empty file produces an error.
    #[tokio::test]
    async fn test_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.vortex");
        std::fs::write(&path, b"").unwrap();
        let ctx = make_ctx();
        let err = run_err(
            &ctx,
            &format!("SELECT * FROM vortex_file_metadata('{}')", path.display()),
        )
        .await;
        assert!(!err.is_empty());
    }

    /// A missing file produces an error.
    #[tokio::test]
    async fn test_missing_file() {
        let ctx = make_ctx();
        let err = run_err(
            &ctx,
            "SELECT * FROM vortex_file_metadata('/does/not/exist.vortex')",
        )
        .await;
        assert!(err.contains("No such file"), "{err}");
    }

    /// Missing and extra arguments produce plan errors.
    #[tokio::test]
    async fn test_bad_arguments() {
        let ctx = make_ctx();
        let err = run_err(&ctx, "SELECT * FROM vortex_file_metadata()").await;
        assert!(err.contains("requires a string filename argument"), "{err}");
        let err = run_err(&ctx, "SELECT * FROM vortex_file_metadata(1)").await;
        assert!(err.contains("requires a string filename argument"), "{err}");
        let err = run_err(&ctx, "SELECT * FROM vortex_file_metadata('a', 'b')").await;
        assert!(err.contains("at most 1 argument"), "{err}");
    }
}

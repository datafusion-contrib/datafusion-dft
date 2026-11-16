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
use arrow::array::{BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use std::fmt::Write;
use std::sync::Arc;

const NAME: &str = "vortex_kv_metadata";

/// Format bytes as lowercase hex
fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// `vortex_kv_metadata` table-valued function
///
/// Returns one row per user-defined metadata segment of a Vortex file,
/// ordered by key. `value` is the segment as text when it is valid UTF-8,
/// and lowercase hex otherwise (`is_utf8` tells which). Only the footer and
/// the metadata segments are read.
///
/// Example:
/// ```sql
/// SELECT key, value FROM vortex_kv_metadata('file.vortex');
/// ```
#[derive(Debug)]
pub struct VortexKvMetadataFunc {}

impl TableFunctionImpl for VortexKvMetadataFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        check_max_args(exprs, 1, NAME)?;
        let filename = expr_to_string(exprs.first(), NAME, "filename")?;
        let input = VortexInput::open(&filename, true)?;

        let mut entries: Vec<(String, Vec<u8>)> = input
            .file
            .metadata_segments()
            .map(|(key, value)| (key.to_string(), value.as_slice().to_vec()))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let n = entries.len();

        let mut keys = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        let mut is_utf8 = Vec::with_capacity(n);
        let mut lengths = Vec::with_capacity(n);
        for (key, bytes) in entries {
            lengths.push(bytes.len() as i64);
            match String::from_utf8(bytes) {
                Ok(text) => {
                    values.push(text);
                    is_utf8.push(true);
                }
                Err(e) => {
                    values.push(to_hex(e.as_bytes()));
                    is_utf8.push(false);
                }
            }
            keys.push(key);
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("is_utf8", DataType::Boolean, false),
            Field::new("length", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![filename; n])),
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(values)),
                Arc::new(BooleanArray::from(is_utf8)),
                Arc::new(Int64Array::from(lengths)),
            ],
        )?;
        mem_table(schema, batch)
    }
}

#[cfg(test)]
mod tests {
    use super::to_hex;
    use crate::test_util::*;

    /// Text, binary, empty, and Unicode values are returned sorted by key.
    #[tokio::test]
    async fn test_kv_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.vortex");
        write_vortex_with_metadata(
            &path,
            standard_batch(),
            &[
                ("zeta", b"plain text"),
                ("alpha", &[0xff, 0x00, 0x10]),
                ("empty", b""),
                ("unicode", "caf\u{e9} \u{1f600}".as_bytes()),
            ],
        );
        let ctx = make_ctx();
        let batches = run(
            &ctx,
            &format!("SELECT * FROM vortex_kv_metadata('{}')", path.display()),
        )
        .await;
        assert_eq!(
            column_strings(&batches, "key"),
            vec![
                Some("alpha".into()),
                Some("empty".into()),
                Some("unicode".into()),
                Some("zeta".into())
            ]
        );
        assert_eq!(
            column_strings(&batches, "value"),
            vec![
                Some("ff0010".into()),
                Some("".into()),
                Some("caf\u{e9} \u{1f600}".into()),
                Some("plain text".into())
            ]
        );
        assert_eq!(
            column_strings(&batches, "is_utf8"),
            vec![
                Some("false".into()),
                Some("true".into()),
                Some("true".into()),
                Some("true".into())
            ]
        );
        assert_eq!(
            column_i64(&batches, "length"),
            vec![Some(3), Some(0), Some(10), Some(10)]
        );
    }

    /// A file without metadata segments returns no rows.
    #[tokio::test]
    async fn test_no_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_standard(dir.path(), "t.vortex");
        let ctx = make_ctx();
        let batches = run(&ctx, &format!("SELECT * FROM vortex_kv_metadata('{path}')")).await;
        assert_eq!(num_rows(&batches), 0);
    }

    #[test]
    fn test_to_hex() {
        assert_eq!(to_hex(&[]), "");
        assert_eq!(to_hex(&[0x00, 0x0f, 0xab, 0xff]), "000fabff");
    }
}

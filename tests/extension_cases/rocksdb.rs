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

//! Tests for datafusion-rocksdb integration

use datafusion_rocksdb::rocksdb::{Options, DB};

use crate::extension_cases::TestExecution;

/// Create a RocksDB database with a `default` and a `metrics` column family,
/// two flushed keys in each, so `live_files()` returns one SST file per
/// column family.
fn create_db(path: &std::path::Path) {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db = DB::open_cf(&opts, path, ["default", "metrics"]).unwrap();
    db.put(b"alpha", b"1").unwrap();
    db.put(b"beta", b"2").unwrap();
    let cf = db.cf_handle("metrics").unwrap();
    db.put_cf(cf, b"m1", b"10").unwrap();
    db.put_cf(cf, b"m2", b"20").unwrap();
    db.flush().unwrap();
    db.flush_cf(cf).unwrap();
}

/// Ensure the rocksdb metadata function is registered and returns the summary
#[tokio::test(flavor = "multi_thread")]
async fn test_rocksdb_metadata() {
    let execution = TestExecution::new().await;

    let dir = tempfile::tempdir().unwrap();
    create_db(dir.path());

    let sql = format!(
        "SELECT column_families, num_column_families, num_live_sst_files, estimated_num_keys FROM rocksdb_metadata('{}')",
        dir.path().display()
    );
    let actual = execution.run_and_format(&sql).await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +------------------+---------------------+--------------------+--------------------+
    - "| column_families  | num_column_families | num_live_sst_files | estimated_num_keys |"
    - +------------------+---------------------+--------------------+--------------------+
    - "| default, metrics | 2                   | 2                  | 4                  |"
    - +------------------+---------------------+--------------------+--------------------+
    "###);
}

/// Ensure the rocksdb sstables function is registered and returns one row per
/// live SST file
#[tokio::test(flavor = "multi_thread")]
async fn test_rocksdb_sstables() {
    let execution = TestExecution::new().await;

    let dir = tempfile::tempdir().unwrap();
    create_db(dir.path());

    let sql = format!(
        "SELECT column_family, level, num_entries, num_deletions, start_key_utf8, end_key_utf8 FROM rocksdb_sstables('{}') ORDER BY column_family",
        dir.path().display()
    );
    let actual = execution.run_and_format(&sql).await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------------+-------+-------------+---------------+----------------+--------------+
    - "| column_family | level | num_entries | num_deletions | start_key_utf8 | end_key_utf8 |"
    - +---------------+-------+-------------+---------------+----------------+--------------+
    - "| default       | 0     | 2           | 0             | alpha          | beta         |"
    - "| metrics       | 0     | 2           | 0             | m1             | m2           |"
    - +---------------+-------+-------------+---------------+----------------+--------------+
    "###);
}

/// Ensure the rocksdb column family metrics function is registered and
/// returns per-column-family properties
#[tokio::test(flavor = "multi_thread")]
async fn test_rocksdb_cf_metrics() {
    let execution = TestExecution::new().await;

    let dir = tempfile::tempdir().unwrap();
    create_db(dir.path());

    let sql = format!(
        "SELECT column_family, property, value FROM rocksdb_cf_metrics('{}') \
         WHERE property IN ('rocksdb.estimate-num-keys', 'rocksdb.num-files-at-level0') \
         ORDER BY column_family, property",
        dir.path().display()
    );
    let actual = execution.run_and_format(&sql).await;

    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------------+-----------------------------+-------+
    - "| column_family | property                    | value |"
    - +---------------+-----------------------------+-------+
    - "| default       | rocksdb.estimate-num-keys   | 2     |"
    - "| default       | rocksdb.num-files-at-level0 | 1     |"
    - "| metrics       | rocksdb.estimate-num-keys   | 2     |"
    - "| metrics       | rocksdb.num-files-at-level0 | 1     |"
    - +---------------+-----------------------------+-------+
    "###);
}

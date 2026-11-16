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

use crate::extension_cases::TestExecution;

#[tokio::test(flavor = "multi_thread")]
async fn test_vortex_extension_loads() {
    // This test verifies that the Vortex extension loads successfully
    // and is registered with the ExecutionContext
    let _test_exec = TestExecution::new().await;

    // If we get here without panicking, the extension loaded successfully
    // TODO: Add test data and query tests once we have sample .vortex files
    // Example usage would be:
    // let cwd = std::env::current_dir().unwrap();
    // let path = Url::from_file_path(cwd.join("data/vortex/sample_data.vortex")).unwrap();
    //
    // let test_exec = test_exec
    //     .with_setup(&format!(
    //         "CREATE EXTERNAL TABLE v STORED AS VORTEX LOCATION '{}';",
    //         path
    //     ))
    //     .await;
    //
    // let output = test_exec
    //     .run_and_format("SELECT * FROM v LIMIT 10")
    //     .await;
}

/// Write a Vortex file with 20000 rows through `COPY ... STORED AS VORTEX`
async fn write_vortex(path: &std::path::Path) -> TestExecution {
    TestExecution::new()
        .await
        .with_setup(&format!(
            "COPY (SELECT value AS id, \
                     CASE value % 3 WHEN 0 THEN 'alpha' WHEN 1 THEN 'beta' ELSE 'gamma' END AS category \
                   FROM generate_series(0, 19999)) \
             TO '{}' STORED AS VORTEX",
            path.display()
        ))
        .await
}

/// Every Vortex inspection function is registered and returns rows.
#[tokio::test(flavor = "multi_thread")]
async fn test_vortex_functions_registered() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.vortex");
    let exec = write_vortex(&path).await;
    let p = path.display();
    for sql in [
        format!("SELECT * FROM vortex_file_metadata('{p}')"),
        format!("SELECT * FROM vortex_schema('{p}')"),
        format!("SELECT * FROM vortex_field_stats('{p}')"),
        format!("SELECT * FROM vortex_layout('{p}')"),
        format!("SELECT * FROM vortex_zone_map('{p}')"),
        format!("SELECT * FROM vortex_segments('{p}')"),
        format!("SELECT * FROM vortex_encodings('{p}')"),
        format!("SELECT * FROM vortex_compression('{p}')"),
        format!("SELECT * FROM vortex_dictionary('{p}', 'category')"),
        format!("SELECT * FROM vortex_rows('{p}', 0, 10)"),
    ] {
        let batches = exec.run(&sql).await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(rows > 0, "no rows from {sql}");
    }
    let batches = exec
        .run(&format!("SELECT * FROM vortex_kv_metadata('{p}')"))
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

/// Schema, statistics, zone maps, and rows match the written data.
#[tokio::test(flavor = "multi_thread")]
async fn test_vortex_function_results() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.vortex");
    let exec = write_vortex(&path).await;
    let p = path.display();

    let actual = exec
        .run_and_format(&format!(
            "SELECT path, type_kind, dtype FROM vortex_schema('{p}') ORDER BY depth, path"
        ))
        .await;
    insta::assert_yaml_snapshot!(actual, @r###"
    - +----------+-----------+-------------------------+
    - "| path     | type_kind | dtype                   |"
    - +----------+-----------+-------------------------+
    - "|          | struct    | {id=i64, category=utf8} |"
    - "| category | utf8      | utf8                    |"
    - "| id       | primitive | i64                     |"
    - +----------+-----------+-------------------------+
    "###);

    let actual = exec
        .run_and_format(&format!(
            "SELECT field_name, min, max, null_count FROM vortex_field_stats('{p}') ORDER BY field_id"
        ))
        .await;
    insta::assert_yaml_snapshot!(actual, @r###"
    - +------------+-------+-------+------------+
    - "| field_name | min   | max   | null_count |"
    - +------------+-------+-------+------------+
    - "| id         | 0     | 19999 | 0          |"
    - "| category   | alpha | gamma | 0          |"
    - +------------+-------+-------+------------+
    "###);

    let actual = exec
        .run_and_format(&format!(
            "SELECT zone_id, first_row, num_rows, value FROM vortex_zone_map('{p}', 'id') \
             WHERE aggregate = 'max' ORDER BY zone_id"
        ))
        .await;
    insta::assert_yaml_snapshot!(actual, @r###"
    - +---------+-----------+----------+-------+
    - "| zone_id | first_row | num_rows | value |"
    - +---------+-----------+----------+-------+
    - "| 0       | 0         | 8192     | 8191  |"
    - "| 1       | 8192      | 8192     | 16383 |"
    - "| 2       | 16384     | 3616     | 19999 |"
    - +---------+-----------+----------+-------+
    "###);

    let actual = exec
        .run_and_format(&format!("SELECT * FROM vortex_rows('{p}', 9998, 10001)"))
        .await;
    insta::assert_yaml_snapshot!(actual, @r###"
    - +-------+----------+
    - "| id    | category |"
    - +-------+----------+
    - "| 9998  | gamma    |"
    - "| 9999  | alpha    |"
    - "| 10000 | beta     |"
    - +-------+----------+
    "###);
}

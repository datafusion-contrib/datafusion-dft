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

use std::io::Write;

use assert_cmd::Command;

use crate::{cli_cases::contains_str, config::TestConfigBuilder};

#[tokio::test(flavor = "multi_thread")]
async fn test_huggingface_single_repo() {
    let tempdir = tempfile::tempdir().unwrap();
    let ddl_path = tempdir.path().join("my_ddl.sql");
    let mut file = std::fs::File::create(ddl_path.clone()).unwrap();
    let ddl = "CREATE EXTERNAL TABLE hf STORED AS PARQUET LOCATION 'hf://HuggingFaceTB-finemath/finemath-3plus/'";
    file.write_all(ddl.as_bytes()).unwrap();
    file.flush().unwrap();
    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_ddl_path(ddl_path);
    config_builder.with_huggingface("dataset", "HuggingFaceTB/finemath", "main");
    let config = config_builder.build("my_config.toml");

    let assert = Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path)
        .arg("--run-ddl")
        .arg("-c")
        .arg("DESCRIBE hf")
        .assert()
        .success();

    let expected = r#"
+--------------------+-----------+-------------+
| column_name        | data_type | is_nullable |
+--------------------+-----------+-------------+
| url                | Utf8View  | YES         |
| fetch_time         | Int64     | YES         |
| content_mime_type  | Utf8View  | YES         |
| warc_filename      | Utf8View  | YES         |
| warc_record_offset | Int32     | YES         |
| warc_record_length | Int32     | YES         |
| text               | Utf8View  | YES         |
| token_count        | Int32     | YES         |
| char_count         | Int32     | YES         |
| metadata           | Utf8View  | YES         |
| score              | Float64   | YES         |
| int_score          | Int64     | YES         |
| crawl              | Utf8View  | YES         |
| snapshot_type      | Utf8View  | YES         |
| language           | Utf8View  | YES         |
| language_score     | Float64   | YES         |
+--------------------+-----------+-------------+
"#;

    assert.stdout(contains_str(expected));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_huggingface_multiple_repos() {
    let tempdir = tempfile::tempdir().unwrap();
    let ddl_path = tempdir.path().join("my_ddl.sql");
    let mut file = std::fs::File::create(ddl_path.clone()).unwrap();
    let ddl = "CREATE EXTERNAL TABLE hf1 STORED AS PARQUET LOCATION 'hf://HuggingFaceTB-finemath/finemath-3plus/';CREATE EXTERNAL TABLE hf2 STORED AS PARQUET LOCATION 'hf://deepmind-code_contests/data/';";
    file.write_all(ddl.as_bytes()).unwrap();
    file.flush().unwrap();
    let mut config_builder = TestConfigBuilder::default();
    config_builder.with_ddl_path(ddl_path);
    config_builder.with_huggingface("dataset", "HuggingFaceTB/finemath", "main");
    config_builder.with_huggingface("dataset", "deepmind/code_contests", "main");
    let config = config_builder.build("my_config.toml");

    let assert1 = Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path.clone())
        .arg("--run-ddl")
        .arg("-c")
        .arg("DESCRIBE hf1")
        .assert()
        .success();

    let expected = r#"
+--------------------+-----------+-------------+
| column_name        | data_type | is_nullable |
+--------------------+-----------+-------------+
| url                | Utf8View  | YES         |
| fetch_time         | Int64     | YES         |
| content_mime_type  | Utf8View  | YES         |
| warc_filename      | Utf8View  | YES         |
| warc_record_offset | Int32     | YES         |
| warc_record_length | Int32     | YES         |
| text               | Utf8View  | YES         |
| token_count        | Int32     | YES         |
| char_count         | Int32     | YES         |
| metadata           | Utf8View  | YES         |
| score              | Float64   | YES         |
| int_score          | Int64     | YES         |
| crawl              | Utf8View  | YES         |
| snapshot_type      | Utf8View  | YES         |
| language           | Utf8View  | YES         |
| language_score     | Float64   | YES         |
+--------------------+-----------+-------------+
"#;

    assert1.stdout(contains_str(expected));

    let assert2 = Command::cargo_bin("dft")
        .unwrap()
        .arg("--config")
        .arg(config.path)
        .arg("--run-ddl")
        .arg("-c")
        .arg("DESCRIBE hf2")
        .assert()
        .success();

    let expected = r#"
+---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+
| column_name               | data_type                                                                                                                                                                                                                                                                                                                                                                                                                                                     | is_nullable |
+---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+
| name                      | Utf8View                                                                                                                                                                                                                                                                                                                                                                                                                                                      | YES         |
| description               | Utf8View                                                                                                                                                                                                                                                                                                                                                                                                                                                      | YES         |
| public_tests              | Struct([Field { name: "input", data_type: List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "output", data_type: List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }])       | YES         |
| private_tests             | Struct([Field { name: "input", data_type: List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "output", data_type: List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }])       | YES         |
| generated_tests           | Struct([Field { name: "input", data_type: List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "output", data_type: List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }])       | YES         |
| source                    | Int64                                                                                                                                                                                                                                                                                                                                                                                                                                                         | YES         |
| difficulty                | Int64                                                                                                                                                                                                                                                                                                                                                                                                                                                         | YES         |
| solutions                 | Struct([Field { name: "language", data_type: List(Field { name: "item", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "solution", data_type: List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]) | YES         |
| incorrect_solutions       | Struct([Field { name: "language", data_type: List(Field { name: "item", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "solution", data_type: List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]) | YES         |
| cf_contest_id             | Int64                                                                                                                                                                                                                                                                                                                                                                                                                                                         | YES         |
| cf_index                  | Utf8View                                                                                                                                                                                                                                                                                                                                                                                                                                                      | YES         |
| cf_points                 | Float32                                                                                                                                                                                                                                                                                                                                                                                                                                                       | YES         |
| cf_rating                 | Int32                                                                                                                                                                                                                                                                                                                                                                                                                                                         | YES         |
| cf_tags                   | List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })                                                                                                                                                                                                                                                                                                                                               | YES         |
| is_description_translated | Boolean                                                                                                                                                                                                                                                                                                                                                                                                                                                       | YES         |
| untranslated_description  | Utf8View                                                                                                                                                                                                                                                                                                                                                                                                                                                      | YES         |
| time_limit                | Struct([Field { name: "seconds", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "nanos", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }])                                                                                                                                                                                                                          | YES         |
| memory_limit_bytes        | Int64                                                                                                                                                                                                                                                                                                                                                                                                                                                         | YES         |
| input_file                | Utf8View                                                                                                                                                                                                                                                                                                                                                                                                                                                      | YES         |
| output_file               | Utf8View                                                                                                                                                                                                                                                                                                                                                                                                                                                      | YES         |
+---------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+
"#;

    assert2.stdout(contains_str(expected));
}

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

mod basic;
mod config;

use assert_cmd::Command;
use predicates::str::ContainsPredicate;
use tempfile::NamedTempFile;

/// Creates a temporary file with the given SQL content
pub fn sql_in_file(sql: impl AsRef<str>) -> NamedTempFile {
    let file = NamedTempFile::new().unwrap();
    std::fs::write(file.path(), sql.as_ref()).unwrap();
    file
}

/// Returns a predicate that expects the given string to be contained in the
/// output
///
/// Whitespace is trimmed from the start and end of the string
pub fn contains_str(s: &str) -> ContainsPredicate {
    predicates::str::contains(s.trim())
}

/// Invokes `dft -f` with the given files and asserts that it exited
/// successfully and the output contains the given string
pub fn assert_output_contains(files: Vec<NamedTempFile>, expected_output: &str) {
    let mut cmd = Command::cargo_bin("dft").unwrap();
    for file in &files {
        cmd.arg("-f").arg(file.path());
    }

    let assert = cmd.assert().success();

    // Since temp files are deleted when they go out of scope ensure they are
    // dropped only after the command is run
    drop(files);

    assert.stdout(contains_str(expected_output));
}

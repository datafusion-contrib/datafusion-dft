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

use assert_cmd::Command;
use predicates::str::contains;

#[test]
fn repl_rejects_redirected_input() {
    Command::cargo_bin("dft")
        .unwrap()
        .arg("--repl")
        .assert()
        .failure()
        .stderr(contains("requires terminal input and output"));
}

#[test]
fn repl_rejects_subcommands_before_execution() {
    Command::cargo_bin("dft")
        .unwrap()
        .args(["--repl", "generate-tpch"])
        .assert()
        .failure()
        .stderr(contains("cannot be combined with a subcommand"));
}

#[cfg(not(feature = "tui"))]
#[test]
fn default_repl_rejects_batch_only_options() {
    Command::cargo_bin("dft")
        .unwrap()
        .arg("--bench")
        .assert()
        .failure()
        .stderr(contains("batch options cannot be used with the REPL"));
}

#[cfg(unix)]
#[test]
fn repl_terminal_session() {
    let mut command = std::process::Command::new("python3");
    command
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/repl_pty.py"))
        .arg(assert_cmd::cargo::cargo_bin("dft"));
    if !cfg!(feature = "tui") {
        command.arg("--default-repl");
    }
    let output = command
        .output()
        .expect("Python 3 is required for PTY tests");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

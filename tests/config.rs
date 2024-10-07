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

use std::{io::Write, path::PathBuf};

use tempfile::{tempdir, TempDir};

pub struct TestConfig {
    #[allow(dead_code)]
    dir: TempDir,
    pub path: PathBuf,
}

#[derive(Debug, Default)]
pub struct TestConfigBuilder {
    config_text: String,
}

impl TestConfigBuilder {
    pub fn build(self, name: &str) -> TestConfig {
        let tempdir = tempdir().unwrap();
        let path = tempdir.path().join(name);
        let mut file = std::fs::File::create(path.clone()).unwrap();
        file.write_all(self.config_text.as_bytes()).unwrap();
        file.flush().unwrap();
        TestConfig { dir: tempdir, path }
    }

    pub fn with_ddl_path(&mut self, ddl_path: PathBuf) -> &mut Self {
        self.config_text.push_str("[execution]\n");
        let param = format!("ddl_path = '{}'", ddl_path.display());
        self.config_text.push_str(&param);
        self
    }
}

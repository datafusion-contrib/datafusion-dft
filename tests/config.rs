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

#[derive(Debug)]
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
        let param = format!("ddl_path = '{}'\n", ddl_path.display());
        self.config_text.push_str(&param);
        self
    }

    #[cfg(feature = "s3")]
    #[allow(clippy::too_many_arguments)]
    pub fn with_s3_object_store(
        &mut self,
        store: &str,
        bucket_name: &str,
        object_store_url: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        allow_http: bool,
    ) -> &mut Self {
        self.config_text
            .push_str(&format!("[[execution.object_store.{}]]\n", store));
        self.config_text
            .push_str(&format!("bucket_name = '{}'\n", bucket_name));
        self.config_text
            .push_str(&format!("object_store_url = '{}'\n", object_store_url));
        self.config_text
            .push_str(&format!("aws_endpoint = '{}'\n", endpoint));
        self.config_text
            .push_str(&format!("aws_access_key_id = '{}'\n", access_key));
        self.config_text
            .push_str(&format!("aws_secret_access_key = '{}'\n", secret_key));
        self.config_text
            .push_str(&format!("aws_allow_http = {}\n", allow_http));
        self
    }

    pub fn with_benchmark_iterations(&mut self, iterations: u64) -> &mut Self {
        self.config_text.push_str(&format!(
            "[execution]\nbenchmark_iterations = {}\n",
            iterations
        ));
        self
    }

    #[cfg(feature = "flightsql")]
    pub fn with_flightsql_benchmark_iterations(&mut self, iterations: u64) -> &mut Self {
        self.config_text.push_str(&format!(
            "[flightsql]\nbenchmark_iterations = {}\n",
            iterations
        ));
        self
    }

    #[cfg(feature = "huggingface")]
    pub fn with_huggingface(
        &mut self,
        repo_type: &str,
        repo_id: &str,
        revision: &str,
    ) -> &mut Self {
        self.config_text
            .push_str("[[execution.object_store.huggingface]]\n");
        self.config_text
            .push_str(&format!("repo_type = '{repo_type}'\n"));
        self.config_text
            .push_str(&format!("repo_id = '{repo_id}'\n"));
        self.config_text
            .push_str(&format!("revision = '{revision}'\n"));
        self
    }

    #[cfg(feature = "udfs-wasm")]
    pub fn with_udfs_wasm(
        &mut self,
        module_path: &str,
        function_name: &str,
        input_types: &[&str],
        return_type: &str,
    ) -> &mut Self {
        // Begin the wasm UDF section.
        self.config_text.push_str("[execution.wasm_udf]\n");

        // Start the inline table for module_functions.
        self.config_text.push_str("module_functions = { ");

        // Write the key (module path) and start its array of function definitions.
        self.config_text
            .push_str(&format!("\"{}\" = [", module_path));

        // Start the function definition.
        self.config_text.push_str("{ ");

        // Write the function name.
        self.config_text
            .push_str(&format!("name = \"{}\", ", function_name));

        // Write the input_types array.
        self.config_text.push_str("input_types = [");
        for (i, ty) in input_types.iter().enumerate() {
            self.config_text.push_str(&format!("\"{}\"", ty));
            if i < input_types.len() - 1 {
                self.config_text.push_str(", ");
            }
        }
        self.config_text.push_str("], ");

        // Write the return type.
        self.config_text
            .push_str(&format!("return_type = \"{}\"", return_type));

        // Close the function definition.
        self.config_text.push_str(" }");

        // Close the array for the module key.
        self.config_text.push(']');

        // Close the inline table (no trailing comma here!).
        self.config_text.push_str(" }");

        // Add a final newline.
        self.config_text.push('\n');
        self
    }
}

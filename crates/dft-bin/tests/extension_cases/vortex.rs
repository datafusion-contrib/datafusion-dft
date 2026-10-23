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

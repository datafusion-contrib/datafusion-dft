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

//! Tests for the TUI (e.g. user application with keyboard commands)

use dft::app::state::initialize;
use dft::app::App;
use dft::cli::DftCli;

fn setup_app() -> App<'static> {
    let args = DftCli::default();
    let state = initialize(args.clone());
    let app = App::new(state, args);
    app
}

#[tokio::test]
async fn run_app_with_no_args() {
    let _app = setup_app();
}

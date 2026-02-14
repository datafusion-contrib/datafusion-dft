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

#![cfg(feature = "tui")]

mod ddl;
#[cfg(feature = "flightsql")]
mod flightsql_execution;
#[cfg(feature = "flightsql")]
mod flightsql_pagination;
mod keyboard_modifiers;
mod pagination;
mod quit;
mod sql_execution;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion_app::{extensions::DftSessionStateBuilder, local::ExecutionContext};
use datafusion_dft::{
    args::DftArgs,
    config::create_config,
    execution::AppExecution,
    tui::{state::AppState, App, AppEvent},
};
use tempfile::{tempdir, TempDir};

// mod tui_cases;

/// Fixture with an [`App`] instance and other temporary state
struct TestApp<'app> {
    /// Temporary directory for configuration files
    ///
    /// The directory is removed when the object is dropped so this
    /// field must remain alive while the app is running
    #[allow(dead_code)]
    config_path: TempDir,
    /// The [`App`] instance
    app: App<'app>,
}

impl TestApp<'_> {
    /// Create a new [`TestApp`] instance configured with a temporary directory
    async fn new() -> Self {
        let config_path = tempdir().unwrap();
        let config = create_config(config_path.path().to_path_buf());
        let state = AppState::new(config);
        let session_state =
            DftSessionStateBuilder::try_new(Some(state.config.tui.execution.clone()))
                .unwrap()
                .with_extensions()
                .await
                .unwrap()
                .build()
                .unwrap();
        let execution_ctx = ExecutionContext::try_new(
            &state.config.tui.execution,
            session_state,
            "dft",
            env!("CARGO_PKG_VERSION"),
        )
        .unwrap();
        let app_execution = AppExecution::new(execution_ctx);
        let args = DftArgs::default();
        let mut app = App::new(state, args, app_execution);
        app.enter(false).unwrap();
        Self { config_path, app }
    }

    /// Call app.event_handler with the given event
    pub fn handle_app_event(&mut self, event: AppEvent) -> color_eyre::Result<()> {
        self.app.handle_app_event(event)
    }

    /// Return the app state
    pub fn state(&self) -> &datafusion_dft::tui::state::AppState<'_> {
        self.app.state()
    }

    pub async fn wait_for_ddl(&mut self) {
        if let Some(handle) = self.app.ddl_task().take() {
            handle.await.unwrap();
        }
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let ctx = self.app.execution().session_ctx().clone();
        ctx.sql(sql).await.unwrap().collect().await
    }
}

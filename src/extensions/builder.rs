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

//! [`DftSessionStateBuilder`] for configuring DataFusion [`SessionState`]

use datafusion::catalog::TableProviderFactory;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Builds a DataFusion [`SessionState`] with any necessary configuration
///
/// Ideally we would use the DataFusion [`SessionStateBuilder`], but it doesn't
/// currently have all the needed APIs. Once we have a good handle on the needed
/// APIs we can upstream them to DataFusion.
///
/// List of things that would be nice to add upstream:
/// TODO: Implement Debug for SessionStateBuilder upstream
///  <https://github.com/apache/datafusion/issues/12555>
/// TODO: Implement some way to get access to the current RuntimeEnv (to register object stores)
///  <https://github.com/apache/datafusion/issues/12553>
/// TODO: Implement a way to add just a single TableProviderFactory
///  <https://github.com/apache/datafusion/issues/12552>
/// TODO: Make TableFactoryProvider implement Debug
///   <https://github.com/apache/datafusion/pull/12557>
/// TODO: rename RuntimeEnv::new() to RuntimeEnv::try_new() as it returns a Result:
///   <https://github.com/apache/datafusion/issues/12554>
//#[derive(Debug)]
pub struct DftSessionStateBuilder {
    session_config: SessionConfig,
    table_factories: Option<HashMap<String, Arc<dyn TableProviderFactory>>>,
    runtime_env: Option<Arc<RuntimeEnv>>,
}

impl Debug for DftSessionStateBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DftSessionStateBuilder")
            .field("session_config", &self.session_config)
            //.field("table_factories", &self.table_factories)
            .field(
                "table_factories",
                &"TODO TableFactoryDoes not implement Debug",
            )
            .field("runtime_env", &self.runtime_env)
            .finish()
    }
}

impl Default for DftSessionStateBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DftSessionStateBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        let session_config = SessionConfig::default().with_information_schema(true);
        // TODO why is batch size 1?
        // .with_batch_size(100)
        // .with_information_schema(true);

        Self {
            session_config,
            table_factories: None,
            runtime_env: None,
        }
    }

    /// Set the `batch_size` on the [`SessionConfig`]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.session_config = self.session_config.with_batch_size(batch_size);
        self
    }

    /// Add a table factory to the list of factories on this builder
    pub fn with_table_factory(
        mut self,
        name: &str,
        factory: Arc<dyn TableProviderFactory>,
    ) -> Self {
        if self.table_factories.is_none() {
            self.table_factories = Some(HashMap::from([(name.to_string(), factory)]));
        } else {
            self.table_factories
                .as_mut()
                .unwrap()
                .insert(name.to_string(), factory);
        }
        self
    }

    /// Return the current [`RuntimeEnv`], creating a default if it doesn't exist
    pub fn runtime_env(&mut self) -> &RuntimeEnv {
        if self.runtime_env.is_none() {
            self.runtime_env = Some(Arc::new(RuntimeEnv::default()));
        }
        self.runtime_env.as_ref().unwrap()
    }

    /// Build the [`SessionState`] from the specified configuration
    pub fn build(self) -> datafusion_common::Result<SessionState> {
        let Self {
            session_config,
            table_factories,
            runtime_env,
        } = self;

        let mut builder = SessionStateBuilder::new()
            .with_default_features()
            .with_config(session_config);

        if let Some(runtime_env) = runtime_env {
            builder = builder.with_runtime_env(runtime_env);
        }
        if let Some(table_factories) = table_factories {
            builder = builder.with_table_factories(table_factories);
        }

        Ok(builder.build())
    }
}

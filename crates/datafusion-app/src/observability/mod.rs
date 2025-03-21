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

use std::{sync::Arc, time::Duration};

use datafusion::catalog::{MemorySchemaProvider, SchemaProvider};

use crate::config::ObservabilityConfig;

#[derive(Clone, Debug)]
pub struct ObservabilityContext {
    schema: Arc<dyn SchemaProvider>,
    _config: ObservabilityConfig,
}

impl ObservabilityContext {
    pub fn new(config: ObservabilityConfig) -> Self {
        let schema = MemorySchemaProvider::new();
        Self {
            schema: Arc::new(schema),
            _config: config,
        }
    }

    pub fn catalog(&self) -> Arc<dyn SchemaProvider> {
        self.schema.clone()
    }

    pub fn record_request(&self, _sql: &str, _duration: Duration) {}
}

impl Default for ObservabilityContext {
    fn default() -> Self {
        Self {
            schema: Arc::new(MemorySchemaProvider::new()),
            _config: ObservabilityConfig::default(),
        }
    }
}

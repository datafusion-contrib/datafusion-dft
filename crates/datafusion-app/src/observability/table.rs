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

use std::{any::Any, collections::HashMap, sync::Arc};

use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    catalog::{Session, TableProvider},
    common::{Constraints, Result},
    datasource::TableType,
    logical_expr::dml::InsertOp,
    physical_plan::ExecutionPlan,
    prelude::Expr,
    scalar::ScalarValue,
};
use indexmap::IndexMap;
use parking_lot::RwLock;

type ObservabilityData = Arc<RwLock<IndexMap<String, HashMap<String, ScalarValue>>>>;

#[derive(Debug)]
pub struct ObservabilityTableConfig {
    primary_key: String,
}

/// Table for tracking observability information. Data is held in a IndexMap, which maintains
/// insertion order, while the app is running and is serialized on app shutdown.
#[derive(Debug)]
pub struct ObservabilityTable {
    schema: Arc<Schema>,
    constraints: Option<Constraints>,
    config: ObservabilityTableConfig,
    inner: ObservabilityData,
}

impl ObservabilityTable {
    pub fn try_new(
        schema: Arc<Schema>,
        constraints: Option<Constraints>,
        config: ObservabilityTableConfig,
    ) -> Result<Self> {
        let inner = Arc::new(RwLock::new(IndexMap::new()));
        Ok(Self {
            schema,
            constraints,
            config,
            inner,
        })
    }
}

impl TableProvider for ObservabilityTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.constraints.as_ref()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
    }

    // async fn insert_into(
    //     &self,
    //     _state: &dyn Session,
    //     input: Arc<dyn ExecutionPlan>,
    //     insert_op: InsertOp,
    // ) -> Result<Arc<dyn ExecutionPlan>> {
    // }
}

#[cfg(test)]
mod test {}

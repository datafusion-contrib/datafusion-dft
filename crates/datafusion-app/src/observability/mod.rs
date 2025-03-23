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

use std::{collections::HashMap, sync::Arc};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, TimeUnit},
    catalog::{MemorySchemaProvider, SchemaProvider},
    common::{DFSchema, Result},
    datasource::MemTable,
    logical_expr::{logical_plan::dml::InsertOp, LogicalPlan, Values},
    physical_plan::execute_stream,
    prelude::{cast, lit, SessionContext},
    sql::TableReference,
};
use log::error;
use tokio_stream::StreamExt;

use crate::config::ObservabilityConfig;

const REQUESTS_TABLE_NAME: &'static str = "requests";

#[derive(Clone, Debug)]
pub struct ObservabilityContext {
    catalog: String,
    schema: Arc<dyn SchemaProvider>,
    config: ObservabilityConfig,
}

impl ObservabilityContext {
    pub fn try_new(config: ObservabilityConfig, app_name: &str) -> Result<Self> {
        let schema = Self::try_create_observability_schema()?;
        Ok(Self {
            catalog: app_name.to_string(),
            schema,
            config,
        })
    }

    fn try_create_observability_schema() -> Result<Arc<dyn SchemaProvider>> {
        let obs_schema = MemorySchemaProvider::new();
        let req_schema = create_req_schema();
        let req_table = MemTable::try_new(Arc::new(req_schema), vec![vec![]])?;
        obs_schema.register_table(REQUESTS_TABLE_NAME.to_string(), Arc::new(req_table))?;
        Ok(Arc::new(obs_schema))
    }

    pub fn schema(&self) -> Arc<dyn SchemaProvider> {
        self.schema.clone()
    }

    pub async fn try_record_request(
        &self,
        ctx: &SessionContext,
        sql: &str,
        start_ms: i64,
        duration_ms: i64,
    ) -> Result<()> {
        let table_ref = TableReference::full(
            self.catalog.clone(),
            self.config.schema_name.clone(),
            REQUESTS_TABLE_NAME,
        );
        if let Ok(reqs) = ctx.table_provider(table_ref.clone()).await {
            let qualified_fields = req_fields()
                .into_iter()
                .map(|f| (Some(table_ref.clone()), Arc::new(f)))
                .collect();
            let schema = Arc::new(DFSchema::new_with_metadata(
                qualified_fields,
                HashMap::new(),
            )?);
            let values = Values {
                schema,
                values: vec![vec![
                    lit(sql),
                    cast(
                        lit(start_ms),
                        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    ),
                    lit(duration_ms),
                ]],
            };
            let logical_plan = LogicalPlan::Values(values);
            let state = ctx.state();
            let physical_plan = state.create_physical_plan(&logical_plan).await?;
            match reqs
                .insert_into(&state, physical_plan, InsertOp::Append)
                .await
            {
                Ok(res) => {
                    // Requires executing this stream to actually insert the request
                    let mut stream = execute_stream(res, ctx.task_ctx())?;
                    while let Some(_) = stream.next().await {}
                }
                Err(e) => {
                    error!("Error recording request: {}", e.to_string())
                }
            }
        } else {
            error!("Missing requests table")
        };
        Ok(())
    }
}

fn req_fields() -> Vec<Field> {
    vec![
        Field::new("sql", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("duration_ms", DataType::Int64, false),
    ]
}

fn create_req_schema() -> Schema {
    let fields = req_fields();
    let schema = Schema::new(fields);
    schema
}

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
    scalar::ScalarValue,
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

    /// Attempts to insert request details in `requests` table.  No verification is performed on
    /// the data - it is inserted exactly as is.
    pub async fn try_record_request(
        &self,
        ctx: &SessionContext,
        req: ObservabilityRequestDetails,
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
                    lit(ScalarValue::Utf8(req.request_id)),
                    lit(req.path),
                    lit(ScalarValue::Utf8(req.sql)),
                    cast(
                        lit(req.start_ms),
                        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    ),
                    lit(req.duration_ms),
                    lit(ScalarValue::UInt64(req.rows)),
                    lit(req.status),
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
                    // Requires executing this stream to actually insert the request. The plan
                    // returns the count of records inserted
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

/// Details that will be recorded in the configured observability request table
pub struct ObservabilityRequestDetails {
    pub request_id: Option<String>,
    pub path: String,
    pub sql: Option<String>,
    pub start_ms: i64,
    pub duration_ms: i64,
    pub rows: Option<u64>,
    pub status: u16,
}

fn req_fields() -> Vec<Field> {
    vec![
        Field::new("request_id", DataType::Utf8, true),
        Field::new("path", DataType::Utf8, false),
        Field::new("sql", DataType::Utf8, true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("duration_ms", DataType::Int64, false),
        Field::new("rows", DataType::UInt64, true),
        Field::new("status", DataType::UInt16, false),
    ]
}

fn create_req_schema() -> Schema {
    let fields = req_fields();
    let schema = Schema::new(fields);
    schema
}

#[cfg(test)]
mod test {
    use datafusion::{assert_batches_eq, execution::SessionStateBuilder};

    use crate::{
        config::ExecutionConfig, local::ExecutionContext,
        observability::ObservabilityRequestDetails,
    };

    #[tokio::test]
    async fn test_observability_schema_exists() {
        let config = ExecutionConfig::default();
        let state = SessionStateBuilder::new().build();
        let execution =
            ExecutionContext::try_new(&config, state, "dft", env!("CARGO_PKG_VERSION")).unwrap();

        execution
            .session_ctx()
            .catalog("dft")
            .unwrap()
            .schema("observability")
            .unwrap();
    }

    #[tokio::test]
    async fn test_observability_record_request() {
        let config = ExecutionConfig::default();
        let state = SessionStateBuilder::new().build();
        let execution =
            ExecutionContext::try_new(&config, state, "dft", env!("CARGO_PKG_VERSION")).unwrap();

        let ctx = execution.session_ctx();
        let req = ObservabilityRequestDetails {
            request_id: None,
            path: "/sql".to_string(),
            sql: Some("SELECT 1".to_string()),
            start_ms: 100,
            duration_ms: 200,
            rows: Some(1),
            status: 200,
        };

        execution
            .observability()
            .try_record_request(ctx, req)
            .await
            .unwrap();

        let batches = execution
            .session_ctx()
            .sql("SELECT * FROM dft.observability.requests")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+------+----------+--------------------------+-------------+------+--------+",
            "| path | sql      | timestamp                | duration_ms | rows | status |",
            "+------+----------+--------------------------+-------------+------+--------+",
            "| /sql | SELECT 1 | 1970-01-01T00:00:00.100Z | 200         | 1    | 200    |",
            "+------+----------+--------------------------+-------------+------+--------+",
        ];

        assert_batches_eq!(expected, &batches);
    }
}

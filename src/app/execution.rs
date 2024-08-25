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

#[cfg(feature = "deltalake")]
use std::sync::Arc;

use color_eyre::eyre::Result;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::*;
#[cfg(feature = "deltalake")]
use deltalake::delta_datafusion::DeltaTableFactory;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use super::config::DataFusionConfig;

pub struct ExecutionContext {
    pub session_ctx: SessionContext,
    pub config: DataFusionConfig,
    pub cancellation_token: CancellationToken,
}

impl ExecutionContext {
    pub fn new(config: DataFusionConfig) -> Self {
        let cfg = SessionConfig::default()
            .with_batch_size(1)
            .with_information_schema(true);

        let mut state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .build();

        #[cfg(feature = "deltalake")]
        {
            state
                .table_factories_mut()
                .insert("DELTATABLE".to_string(), Arc::new(DeltaTableFactory {}));
        }

        let session_ctx = SessionContext::new_with_state(state);
        let cancellation_token = CancellationToken::new();

        Self {
            config,
            session_ctx,
            cancellation_token,
        }
    }

    pub fn create_tables(&mut self) -> Result<()> {
        Ok(())
    }

    pub async fn execute_stream_sql(&mut self, query: &str) -> Result<()> {
        let df = self.session_ctx.sql(query).await.unwrap();
        let physical_plan = df.create_physical_plan().await.unwrap();
        // We use small batch size because web socket stream comes in small increments (each
        // message usually only has at most a few records).
        let stream_cfg = SessionConfig::default().with_batch_size(self.config.stream_batch_size);
        let stream_task_ctx = TaskContext::default().with_session_config(stream_cfg);
        let mut stream = execute_stream(physical_plan, stream_task_ctx.into()).unwrap();

        while let Some(maybe_batch) = stream.next().await {
            let batch = maybe_batch.unwrap();
            let d = pretty_format_batches(&[batch]).unwrap();
            println!("{}", d);
        }
        Ok(())
    }

    pub async fn show_catalog(&self) -> Result<()> {
        let tables = self.session_ctx.sql("SHOW tables").await?.collect().await?;
        let formatted = pretty_format_batches(&tables).unwrap();
        println!("{}", formatted);
        Ok(())
    }
}

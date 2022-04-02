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

use datafusion::execution::context::ExecutionContext;
use log::{debug, error};
use serde::Deserialize;
use std::fs::File;
use std::sync::Arc;

use crate::app::error::{DftError, Result};

#[cfg(feature = "bigtable")]
pub async fn register_bigtable(mut ctx: ExecutionContext) -> ExecutionContext {
    use arrow::datatypes::{DataType, Field};
    use datafusion_bigtable::datasource::BigtableDataSource;

    #[derive(Clone, Deserialize, Debug)]
    struct BigtableConfig {
        table_name: String,
        project: String,
        instance: String,
        table: String,
        column_family: String,
        table_partition_cols: Vec<String>,
        table_partition_separator: String,
        utf8_qualifiers: Vec<String>,
        i64_qualifiers: Vec<String>,
        only_read_latest: bool,
    }

    impl BigtableConfig {
        async fn try_to_table(self) -> Result<BigtableDataSource> {
            let mut qualifiers: Vec<Field> = self
                .utf8_qualifiers
                .iter()
                .map(|c| Field::new(c, DataType::Utf8, false))
                .collect();
            let mut i64_qualifiers: Vec<Field> = self
                .i64_qualifiers
                .iter()
                .map(|c| Field::new(c, DataType::Int64, false))
                .collect();
            qualifiers.append(&mut i64_qualifiers);

            BigtableDataSource::new(
                self.project,
                self.instance,
                self.table,
                self.column_family,
                self.table_partition_cols,
                self.table_partition_separator,
                qualifiers,
                self.only_read_latest,
            )
            .await
            .map_err(|e| DftError::DataFusionError(e))
        }
    }

    let home = dirs::home_dir();
    if let Some(p) = home {
        let bigtable_config_path = p.join(".datafusion/table_providers/bigtable.json");
        if bigtable_config_path.exists() {
            let cfgs: Vec<BigtableConfig> =
                serde_json::from_reader(File::open(bigtable_config_path).unwrap()).unwrap();
            debug!("BigTable Configs: {:?}", cfgs);
            for cfg in cfgs.into_iter() {
                let name = cfg.table_name.clone();
                let table = cfg.clone().try_to_table().await;
                if let Ok(t) = table {
                    ctx.register_table(name.as_str(), Arc::new(t)).unwrap();
                } else {
                    error!("Error loading Bigtable with config: {:?}", cfg);
                }
            }
        };
    }
    ctx
}

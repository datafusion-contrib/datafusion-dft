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

use datafusion::prelude::SessionContext;

#[cfg(feature = "glue")]
pub async fn register_glue(ctx: SessionContext) -> SessionContext {
    use datafusion_catalogprovider_glue::catalog_provider::glue::GlueCatalogProvider;
    use log::{error, info};
    use serde::Deserialize;
    use std::fs::File;
    use std::sync::Arc;

    #[derive(Deserialize, Debug)]
    struct GlueConfig {
        databases: Vec<String>,
    }

    async fn config_to_glue(cfg: GlueConfig) -> GlueCatalogProvider {
        info!("Creating Glue Catalog from: {:?}", cfg);
        let mut glue = GlueCatalogProvider::default().await;
        for db in cfg.databases {
            info!("Registering database {}", db);
            let register_results = glue.register_tables(db.as_str()).await.unwrap();
            for result in register_results {
                if result.is_err() {
                    // Only output tables which were not registered...
                    error!("{}", result.err().unwrap());
                }
            }
        };
        glue
    }

    let home = dirs::home_dir();
    if let Some(p) = home {
        let glue_config_path = p.join(".datafusion/catalog_providers/glue.json");
        let glue = if glue_config_path.exists() {
            let cfg: GlueConfig =
                serde_json::from_reader(File::open(glue_config_path).unwrap()).unwrap();
            let glue = config_to_glue(cfg).await;
            info!("Created GlueCatalogProvider from config");
            Arc::new(glue)
        } else {
            let mut glue = GlueCatalogProvider::default().await;
            let register_results = glue.register_all().await.unwrap();
            for result in register_results {
                if result.is_err() {
                    // Only output tables which were not registered...
                    error!("{}", result.err().unwrap());
                }
            }
            info!("Created GlueCatalogProvider from default AWS credentials");
            Arc::new(glue)
        };

        ctx.register_catalog("glue", glue);
        info!("Registered GlueCatalogProvider");
    }
    ctx
}

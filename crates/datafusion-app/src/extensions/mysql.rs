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

//! MySQL Integration: [MysqlExtension]

use std::{collections::HashMap, sync::Arc};
use crate::config::ExecutionConfig;
use crate::extensions::{DftSessionStateBuilder, Extension};
use datafusion_table_providers::{
    common::DatabaseCatalogProvider, mysql::MySQLTableFactory,
    sql::db_connection_pool::mysqlpool::MySQLConnectionPool, util::secrets::to_secret_map,
};
use log::info;

#[derive(Debug, Default)]
pub struct MysqlExtension {}

impl MysqlExtension {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Extension for MysqlExtension {
    async fn register(
        &self,
        config: ExecutionConfig,
        builder: &mut DftSessionStateBuilder,
    ) -> datafusion::common::Result<()> {
        let Some(database_config) = &config.database else {
            return Ok(());
        };

        let Some(mysql_configs) = &database_config.mysql else {
            return Ok(());
        };

        info!("MySQL configs exists");
        for mysql_config in mysql_configs {
            let mysql_params = to_secret_map(HashMap::from([
                (
                    "connection_string".to_string(),
                    format!("mysql://{}:{}@{}:{}/{}", mysql_config.user, mysql_config.password, mysql_config.host, mysql_config.port, mysql_config.database),
                ),
                ("sslmode".to_string(), mysql_config.sslmode.clone()),
            ]));
            let mysql_pool = Arc::new(
                MySQLConnectionPool::new(mysql_params)
                    .await
                    .expect("unable to create MySQL connection pool"),
            );
            let catalog = DatabaseCatalogProvider::try_new(mysql_pool).await.unwrap();
            builder.add_catalog_provider(&mysql_config.name, Arc::new(catalog));
        }

        Ok(())
    }
}

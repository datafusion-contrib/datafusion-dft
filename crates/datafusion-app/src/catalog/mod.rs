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

use std::{collections::HashMap, fs::File, path::Path, sync::Arc};

use datafusion::{
    arrow::{
        array::StringArray,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider},
    common::Result,
    datasource::MemTable,
    error::DataFusionError,
    scalar::ScalarValue,
    DATAFUSION_VERSION,
};
use indexmap::IndexMap;

use crate::config::ExecutionConfig;

type PreparedStatementsMap = IndexMap<String, HashMap<String, ScalarValue>>;

pub fn create_app_catalog(
    config: &ExecutionConfig,
    app_name: &str,
    app_version: &str,
) -> Result<Arc<dyn CatalogProvider>> {
    let catalog = MemoryCatalogProvider::new();
    let meta_schema = Arc::new(MemorySchemaProvider::new());
    catalog.register_schema("meta", Arc::<MemorySchemaProvider>::clone(&meta_schema))?;
    let versions_table = try_create_meta_versions_table(app_name, app_version)?;
    meta_schema.register_table("versions".to_string(), versions_table)?;
    #[cfg(feature = "flightsql")]
    {
        let flightsql_schema = Arc::new(MemorySchemaProvider::new());
        catalog.register_schema("flightsql", flightsql_schema)?;
        let db_path = config.db.path.to_file_path().map_err(|_| {
            DataFusionError::External("error converting DB path to file path".to_string().into())
        })?;
        let prepared_statements_file = db_path
            .join(app_name)
            .join("flightsql")
            .join("prepared_statements");
        let prepared_statements = if let Ok(true) = prepared_statements_file.try_exists() {
            let reader = File::open(prepared_statements_file)
                .map_err(|e| DataFusionError::External(e.to_string().into()))?;
            let vals: PreparedStatementsMap = serde_json::from_reader(reader)
                .map_err(|e| DataFusionError::External(e.to_string().into()))?;
        } else {
        };
    }
    Ok(Arc::new(catalog))
}

// fn create_flightsql_prepared_statements_table() -> Result<Arc<MemTable>> {
//     let fields = vec![
//         Field::new("datafusion", DataType::Utf8, false),
//         Field::new("datafusion-app", DataType::Utf8, false),
//     ];
//     let schema = Arc::new(Schema::new(fields));
//
//     let batches = RecordBatch::try_new(
//         Arc::<Schema>::clone(&schema),
//         vec![
//             Arc::new(app_version_arr),
//             Arc::new(datafusion_version_arr),
//             Arc::new(datafusion_app_version_arr),
//         ],
//     )?;
//
//     Ok(Arc::new(MemTable::try_new(schema, vec![vec![batches]])?))
// }

fn try_create_meta_versions_table(app_name: &str, app_version: &str) -> Result<Arc<MemTable>> {
    let fields = vec![
        Field::new(app_name, DataType::Utf8, false),
        Field::new("datafusion", DataType::Utf8, false),
        Field::new("datafusion-app", DataType::Utf8, false),
    ];
    let schema = Arc::new(Schema::new(fields));

    let app_version_arr = StringArray::from(vec![app_version]);
    let datafusion_version_arr = StringArray::from(vec![DATAFUSION_VERSION]);
    let datafusion_app_version_arr = StringArray::from(vec![env!("CARGO_PKG_VERSION")]);
    let batches = RecordBatch::try_new(
        Arc::<Schema>::clone(&schema),
        vec![
            Arc::new(app_version_arr),
            Arc::new(datafusion_version_arr),
            Arc::new(datafusion_app_version_arr),
        ],
    )?;

    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batches]])?))
}

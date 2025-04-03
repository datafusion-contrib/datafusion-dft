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

//! Benchmarks for MapTable using Criterion

use std::{collections::HashMap, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    prelude::{SessionConfig, SessionContext},
    scalar::ScalarValue,
};
use indexmap::IndexMap;
use parking_lot::RwLock;
use tokio::runtime::Runtime;

use datafusion_app::tables::map_table::{MapTable, MapTableConfig};

/// Set up a MapTable for benchmarking with specified number of rows
fn setup_map_table(num_rows: usize) -> SessionContext {
    let mut data: IndexMap<ScalarValue, HashMap<String, ScalarValue>> = IndexMap::new();
    
    // Create rows with sequential IDs and values
    for id in 1..=num_rows {
        let mut row: HashMap<String, ScalarValue> = HashMap::new();
        let val = format!("val{}", id);
        
        row.insert("id".to_string(), ScalarValue::Int32(Some(id as i32)));
        row.insert("val".to_string(), ScalarValue::Utf8(Some(val)));
        
        data.insert(ScalarValue::Int32(Some(id as i32)), row);
    }

    let fields = vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Utf8, false),
    ];
    
    let schema = Schema::new(fields);
    let config = MapTableConfig::new("test".to_string(), "id".to_string());
    let table = MapTable::try_new(
        Arc::new(schema),
        None,
        config,
        Some(Arc::new(RwLock::new(data))),
    )
    .unwrap();
    
    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("test", Arc::new(table)).unwrap();
    ctx
}

/// Benchmark MapTable with different row counts
pub fn map_table_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("MapTable");
    
    for size in [1000, 10000, 100000] {
        group.bench_function(format!("select_all_{}", size), |b| {
            b.iter(|| {
                let ctx = setup_map_table(size);
                runtime.block_on(async {
                    let df = ctx.sql("SELECT * FROM test").await.unwrap();
                    let _results = df.collect().await.unwrap();
                });
            });
        });
        
        group.bench_function(format!("select_filtered_{}", size), |b| {
            b.iter(|| {
                let ctx = setup_map_table(size);
                let filter_value = size / 2;
                runtime.block_on(async {
                    let query = format!("SELECT * FROM test WHERE id > {}", filter_value);
                    let df = ctx.sql(&query).await.unwrap();
                    let _results = df.collect().await.unwrap();
                });
            });
        });
    }
    
    group.finish();
}

criterion_group!(benches, map_table_benchmark);
criterion_main!(benches);
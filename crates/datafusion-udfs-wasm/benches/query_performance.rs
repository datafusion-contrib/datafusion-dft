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

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::{array::Int64Array, datatypes::DataType};
use datafusion::prelude::*;
use datafusion_udfs_wasm::{try_create_wasm_udf, WasmInputDataType, WasmUdfDetails};
use tokio::runtime::Runtime;

/// Generate test data with specified number of rows
async fn create_test_table(
    ctx: &SessionContext,
    row_count: usize,
) -> Result<(), datafusion::error::DataFusionError> {
    // Create arrays for our data
    let mut col1_data = Vec::with_capacity(row_count);
    let mut col2_data = Vec::with_capacity(row_count);

    for i in 0..row_count {
        col1_data.push(i as i64);
        col2_data.push((i + 1) as i64);
    }

    // Create Arrow arrays
    let col1_array = Int64Array::from(col1_data);
    let col2_array = Int64Array::from(col2_data);

    // Define schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("column1", DataType::Int64, false),
        Field::new("column2", DataType::Int64, false),
    ]));

    // Create RecordBatch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(col1_array), Arc::new(col2_array)],
    )?;

    // Register batch as table
    ctx.register_batch("test", batch)?;

    Ok(())
}

/// Register a row-based WASM UDF
fn register_row_udf(ctx: &SessionContext) -> Result<(), datafusion::error::DataFusionError> {
    let wasm_bytes =
        std::fs::read("test-wasm/basic_wasm_example.wasm").expect("Failed to read WASM file");
    let input_types = vec![DataType::Int64, DataType::Int64];
    let return_type = DataType::Int64;
    let udf_details = WasmUdfDetails::new(
        "wasm_add".to_string(),
        input_types,
        return_type,
        WasmInputDataType::Row,
    );
    let udf = try_create_wasm_udf(&wasm_bytes, udf_details)?;
    ctx.register_udf(udf);
    Ok(())
}

/// Register an Arrow IPC-based WASM UDF
fn register_arrow_ipc_udf(ctx: &SessionContext) -> Result<(), datafusion::error::DataFusionError> {
    let wasm_bytes =
        std::fs::read("test-wasm/wasm_examples.wasm").expect("Failed to read WASM file");
    let input_types = vec![DataType::Int64, DataType::Int64];
    let return_type = DataType::Int64;
    let udf_details = WasmUdfDetails::new(
        "arrow_func".to_string(),
        input_types,
        return_type,
        WasmInputDataType::ArrowIpc,
    );
    let udf = try_create_wasm_udf(&wasm_bytes, udf_details)?;
    ctx.register_udf(udf);
    Ok(())
}

/// Execute a query using the row-based WASM UDF
async fn execute_row_udf_query(
    ctx: &SessionContext,
) -> Result<Vec<RecordBatch>, datafusion::error::DataFusionError> {
    let query = "SELECT *, wasm_add(column1, column2) AS result FROM test";
    ctx.sql(query).await?.collect().await
}

/// Execute a query using the Arrow IPC-based WASM UDF
async fn execute_arrow_ipc_udf_query(
    ctx: &SessionContext,
) -> Result<Vec<RecordBatch>, datafusion::error::DataFusionError> {
    let query = "SELECT *, arrow_func(column1, column2) AS result FROM test";
    ctx.sql(query).await?.collect().await
}

/// Benchmark row-based WASM UDF with different dataset sizes
fn bench_row_udf(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_udf_query");

    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            // Create a shared runtime outside the benchmark loop
            let rt = Runtime::new().unwrap();

            b.iter(|| {
                rt.block_on(async {
                    let ctx = SessionContext::new();

                    // Create test data
                    create_test_table(&ctx, size).await.unwrap();

                    // Register UDF
                    register_row_udf(&ctx).unwrap();

                    // Execute query
                    execute_row_udf_query(&ctx).await.unwrap()
                })
            });
        });
    }

    group.finish();
}

/// Benchmark Arrow IPC-based WASM UDF with different dataset sizes
fn bench_arrow_ipc_udf(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow_ipc_udf_query");

    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            // Create a shared runtime outside the benchmark loop
            let rt = Runtime::new().unwrap();

            b.iter(|| {
                rt.block_on(async {
                    let ctx = SessionContext::new();

                    // Create test data
                    create_test_table(&ctx, size).await.unwrap();

                    // Register UDF
                    register_arrow_ipc_udf(&ctx).unwrap();

                    // Execute query
                    execute_arrow_ipc_udf_query(&ctx).await.unwrap()
                })
            });
        });
    }

    group.finish();
}

// Configure Criterion to run our benchmarks with optimized settings
fn bench_config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = bench_config();
    targets = bench_row_udf, bench_arrow_ipc_udf
}
criterion_main!(benches);

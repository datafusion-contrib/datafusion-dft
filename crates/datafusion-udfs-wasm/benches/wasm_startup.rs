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

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::arrow::datatypes::DataType;
use datafusion_udfs_wasm::{try_create_wasm_udf, WasmInputDataType, WasmUdfDetails};
use wasi_common::sync::WasiCtxBuilder;
use wasmtime::{Engine, Instance, Module, Store};

/// Benchmark wasmtime module loading
fn bench_module_loading(c: &mut Criterion) {
    let wasm_bytes = std::fs::read("test-wasm/basic_wasm_example.wasm").unwrap();

    // Benchmark module loading
    let mut group = c.benchmark_group("module_loading");
    group.bench_function("module_from_binary", |b| {
        b.iter(|| {
            let engine = Engine::default();
            let _module = Module::new(&engine, &wasm_bytes).unwrap();
        });
    });
    group.finish();
}

/// Benchmark engine + module + instance creation
fn bench_instance_creation(c: &mut Criterion) {
    let wasm_bytes = std::fs::read("test-wasm/basic_wasm_example.wasm").unwrap();

    // Benchmark instance creation with different configurations
    let mut group = c.benchmark_group("instance_creation");

    // Default no WASI
    group.bench_function("default_store", |b| {
        b.iter(|| {
            let engine = Engine::default();
            let module = Module::new(&engine, &wasm_bytes).unwrap();
            let mut store = Store::<()>::new(&engine, ());
            let _instance = Instance::new(&mut store, &module, &[]).unwrap();
        });
    });

    // With WASI context
    group.bench_function("wasi_context", |b| {
        b.iter(|| {
            let engine = Engine::default();
            let module = Module::new(&engine, &wasm_bytes).unwrap();
            let wasi = WasiCtxBuilder::new().inherit_stderr().build();
            let mut store = Store::new(&engine, wasi);
            let _instance = Instance::new(&mut store, &module, &[]).unwrap();
        });
    });

    group.finish();
}

/// Benchmark different WASM UDF creation types
fn bench_udf_creation(c: &mut Criterion) {
    let row_wasm_bytes = std::fs::read("test-wasm/basic_wasm_example.wasm").unwrap();
    let ipc_wasm_bytes = std::fs::read("test-wasm/wasm_examples.wasm").unwrap();

    let mut group = c.benchmark_group("udf_creation");

    // Row-based UDF
    let row_input_types = vec![DataType::Int64, DataType::Int64];
    let row_return_type = DataType::Int64;
    let row_udf_details = WasmUdfDetails::new(
        "wasm_add".to_string(),
        row_input_types,
        row_return_type,
        WasmInputDataType::Row,
    );

    group.bench_with_input(
        BenchmarkId::new("row_based_udf", "wasm_add"),
        &(&row_wasm_bytes, row_udf_details),
        |b, (wasm_bytes, udf_details)| {
            b.iter(|| {
                let udf_details_clone = WasmUdfDetails::new(
                    udf_details.name.clone(),
                    udf_details.input_types.clone(),
                    udf_details.return_type.clone(),
                    udf_details.input_data_type.clone(),
                );
                let _udf = try_create_wasm_udf(wasm_bytes, udf_details_clone).unwrap();
            });
        },
    );

    // ArrowIpc-based UDF
    let ipc_input_types = vec![DataType::Int64, DataType::Int64];
    let ipc_return_type = DataType::Int64;
    let ipc_udf_details = WasmUdfDetails::new(
        "arrow_func".to_string(),
        ipc_input_types,
        ipc_return_type,
        WasmInputDataType::ArrowIpc,
    );

    group.bench_with_input(
        BenchmarkId::new("arrow_ipc_udf", "arrow_func"),
        &(&ipc_wasm_bytes, ipc_udf_details),
        |b, (wasm_bytes, udf_details)| {
            b.iter(|| {
                let udf_details_clone = WasmUdfDetails::new(
                    udf_details.name.clone(),
                    udf_details.input_types.clone(),
                    udf_details.return_type.clone(),
                    udf_details.input_data_type.clone(),
                );
                let _udf = try_create_wasm_udf(wasm_bytes, udf_details_clone).unwrap();
            });
        },
    );

    group.finish();
}

/// Benchmark wasmtime engine creation
fn bench_engine_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_creation");

    group.bench_function("default_engine", |b| {
        b.iter(|| {
            let _engine = Engine::default();
        });
    });

    let mut config = wasmtime::Config::new();
    config.cranelift_opt_level(wasmtime::OptLevel::Speed);

    group.bench_function("optimized_engine", |b| {
        b.iter(|| {
            let _engine = Engine::new(&config).unwrap();
        });
    });

    group.finish();
}

/// Benchmark function extraction
fn bench_function_extraction(c: &mut Criterion) {
    let wasm_bytes = std::fs::read("test-wasm/basic_wasm_example.wasm").unwrap();

    let mut group = c.benchmark_group("function_extraction");

    group.bench_function("get_func", |b| {
        let engine = Engine::default();
        let module = Module::new(&engine, &wasm_bytes).unwrap();
        let mut store = Store::<()>::new(&engine, ());
        let instance = Instance::new(&mut store, &module, &[]).unwrap();

        b.iter(|| {
            let _func = instance.get_func(&mut store, "wasm_add");
        });
    });

    group.bench_function("get_typed_func", |b| {
        let engine = Engine::default();
        let module = Module::new(&engine, &wasm_bytes).unwrap();
        let mut store = Store::<()>::new(&engine, ());
        let instance = Instance::new(&mut store, &module, &[]).unwrap();

        b.iter(|| {
            let _func = instance
                .get_typed_func::<(i64, i64), i64>(&mut store, "wasm_add")
                .unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_engine_creation,
    bench_module_loading,
    bench_instance_creation,
    bench_udf_creation,
    bench_function_extraction
);
criterion_main!(benches);

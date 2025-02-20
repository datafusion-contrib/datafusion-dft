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

pub mod arrow;
pub mod native;

use std::sync::Arc;

use arrow::ipc::create_arrow_ipc_wasm_udf_impl;
use datafusion::{
    arrow::datatypes::DataType,
    common::{DataFusionError, Result},
    logical_expr::{ScalarUDF, Volatility},
    prelude::create_udf,
};
use log::info;
use native::row::create_row_wasm_udf_impl;
#[cfg(feature = "serde")]
use serde::Deserialize;
use wasi_common::WasiCtx;
use wasmtime::{Instance, Module, Store, TypedFunc};

#[cfg_attr(feature = "serde", derive(Deserialize))]
#[derive(Clone, Debug)]
pub enum WasmInputDataType {
    Row,
    ArrowIpc,
}

/// Details necessary to create a DataFusion `ScalarUDF`
pub struct WasmUdfDetails {
    name: String,
    input_data_type: WasmInputDataType,
    input_types: Vec<DataType>,
    return_type: DataType,
}
//
impl WasmUdfDetails {
    pub fn new(
        name: String,
        input_types: Vec<DataType>,
        return_type: DataType,
        input_data_type: WasmInputDataType,
    ) -> Self {
        Self {
            name,
            input_types,
            return_type,
            input_data_type,
        }
    }
}

fn create_wasm_udf(module_bytes: &[u8], udf_details: WasmUdfDetails) -> Result<ScalarUDF> {
    let WasmUdfDetails {
        name,
        input_types,
        return_type,
        input_data_type,
    } = udf_details;
    info!(
        "Registering WASM function {} with input {input_types:?} and return_type {return_type:?}",
        &name
    );

    // We need to call `create_udf` on each branch because each `impl Trait` creates a distinct
    // opaque type so the type returned from each branch is different.  We could probably create a
    // wrapper struct as a cleaner solution but using this for now.
    match input_data_type {
        WasmInputDataType::Row => {
            let udf_impl = create_row_wasm_udf_impl(
                module_bytes.to_owned(),
                name.clone(),
                input_types.clone(),
                return_type.clone(),
            );
            let udf = create_udf(
                &name,
                input_types,
                return_type,
                Volatility::Immutable,
                Arc::new(udf_impl),
            );
            Ok(udf)
        }
        WasmInputDataType::ArrowIpc => {
            let udf_impl = create_arrow_ipc_wasm_udf_impl(
                module_bytes.to_owned(),
                name.clone(),
                input_types.clone(),
                return_type.clone(),
            );
            let udf = create_udf(
                &name,
                input_types,
                return_type,
                Volatility::Immutable,
                Arc::new(udf_impl),
            );
            Ok(udf)
        }
    }
}

/// Attempts to create a `ScalarUDF` from the provided byte slice, which could be either a WASM
/// binary or text format, and function details (name and signature).
pub fn try_create_wasm_udf(module_bytes: &[u8], udf_details: WasmUdfDetails) -> Result<ScalarUDF> {
    let mut store = Store::<()>::default();
    let module = Module::new(store.engine(), module_bytes)
        .map_err(|_| DataFusionError::Execution("Unable to load WASM module".to_string()))?;
    let instance = Instance::new(&mut store, &module, &[])
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

    //  Check if the function exists in the WASM module before proceeding with the
    //  UDF creation
    instance
        .get_func(&mut store, &udf_details.name)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "WASM function {} is missing in module",
                &udf_details.name
            ))
        })?;

    let udf = create_wasm_udf(module_bytes, udf_details)?;
    Ok(udf)
}

pub fn try_get_wasm_module_exported_fn<Params, Results>(
    instance: &Instance,
    store: &mut Store<WasiCtx>,
    export_name: &str,
) -> Result<TypedFunc<Params, Results>>
where
    Params: wasmtime::WasmParams,
    Results: wasmtime::WasmResults,
{
    instance
        .get_typed_func::<Params, Results>(store, export_name)
        .map_err(|err| {
            DataFusionError::Internal(
                format!("Required export '{export_name:?}' could not be located in WASM module exports: {err:?}"))
        })
}

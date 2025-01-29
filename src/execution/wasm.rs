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

use color_eyre::{eyre::eyre, Result};
use datafusion::{
    arrow::{array::ArrayRef, datatypes::DataType},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility},
    prelude::create_udf,
};
use datafusion_common::{DataFusionError, Result as DFResult};
use log::{error, info};
use wasmtime::{Instance, Module, Store};

use crate::config::{WasmFuncDetails, WasmUdfConfig};

pub fn udf_signature_from_func_details(
    func_details: &WasmFuncDetails,
) -> Result<(Vec<DataType>, DataType)> {
    let input_types: Result<Vec<DataType>> = func_details
        .input_types
        .iter()
        .map(|s| {
            let t: DataType = s.as_str().try_into()?;
            Ok(t)
        })
        .collect();
    let return_type: DataType = func_details.return_type.as_str().try_into()?;
    Ok((input_types?, return_type))
}

fn validate_args(args: &[ColumnarValue], input_types: &[DataType]) -> DFResult<()> {
    // First check that the defined input_types and args have same number of columns
    if args.len() != input_types.len() {
        return Err(DataFusionError::Execution(
            "The number of arguments is incorrect".to_string(),
        ));
    }

    Ok(())
}

fn create_wasm_udf_impl(
    module_bytes: Vec<u8>,
    input_types: Vec<DataType>,
    return_type: DataType,
) -> impl Fn(&[ColumnarValue]) -> DFResult<ColumnarValue> {
    move |args: &[ColumnarValue]| {
        // First validate the arguments
        validate_args(args, &input_types)?;
        // Load the function again
        let mut store = Store::<()>::default();
        let module = Module::from_binary(store.engine(), &module_bytes)
            .map_err(|e| DataFusionError::Internal(format!("Error loading module: {e:?}")))?;
        let instance = Instance::new(&mut store, &module, &[])
            .map_err(|e| DataFusionError::Internal(format!("Error instantiating module: {e:?}")))?;

        let vals = ColumnarValue::values_to_arrays(args)?;
        let first = &vals[0];
        Ok(ColumnarValue::Array(Arc::clone(first)))
    }
}

pub fn create_wasm_udfs(wasm_udf_config: &WasmUdfConfig) -> Result<Vec<ScalarUDF>> {
    let mut created_udfs: Vec<ScalarUDF> = Vec::new();
    for (module_path, funcs) in &wasm_udf_config.module_functions {
        let mut store = Store::<()>::default();
        let module_bytes = std::fs::read(module_path)?;
        let module = Module::from_binary(store.engine(), &module_bytes).unwrap();
        for func_details in funcs {
            match udf_signature_from_func_details(func_details) {
                Ok((input_types, return_type)) => {
                    let instance =
                        Instance::new(&mut store, &module, &[]).map_err(|e| eyre!("{e}"))?;
                    //  Check if the function exists in the WASM module before proceeding with the
                    //  UDF creation
                    if instance.get_func(&mut store, &func_details.name).is_none() {
                        error!("WASM function {} is missing in module", &func_details.name);
                    } else {
                        let udf_impl = create_wasm_udf_impl(
                            module_bytes.to_owned(),
                            input_types.clone(),
                            return_type.clone(),
                        );
                        info!("Registering WASM function {} with input {input_types:?} and return_type {return_type:?}", &func_details.name);
                        let udf = create_udf(
                            &func_details.name,
                            input_types,
                            return_type,
                            Volatility::Immutable,
                            Arc::new(udf_impl),
                        );
                        created_udfs.push(udf)
                    }
                }
                Err(_) => {
                    error!("Error parsing WASM UDF signature for {}", func_details.name);
                }
            }
        }
    }
    Ok(created_udfs)
}

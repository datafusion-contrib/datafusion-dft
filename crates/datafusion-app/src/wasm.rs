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

use color_eyre::Result;
use datafusion::{arrow::datatypes::DataType, logical_expr::ScalarUDF};
use datafusion_udfs_wasm::{try_create_wasm_udf, WasmUdfDetails};
use log::error;

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

pub fn create_wasm_udfs(wasm_udf_config: &WasmUdfConfig) -> Result<Vec<ScalarUDF>> {
    let mut created_udfs: Vec<ScalarUDF> = Vec::new();
    for (module_path, funcs) in &wasm_udf_config.module_functions {
        let module_bytes = std::fs::read(module_path)?;
        for func_details in funcs {
            match udf_signature_from_func_details(func_details) {
                Ok((input_types, return_type)) => {
                    let udf_details = WasmUdfDetails::new(
                        func_details.name.clone(),
                        input_types,
                        return_type,
                        func_details.input_data_type.clone(),
                    );
                    let udf = try_create_wasm_udf(&module_bytes, udf_details)?;
                    created_udfs.push(udf)
                }
                Err(_) => {
                    error!("Error parsing WASM UDF signature for {}", func_details.name);
                }
            }
        }
    }
    Ok(created_udfs)
}

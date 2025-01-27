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
use datafusion::{arrow::datatypes::DataType, logical_expr::ScalarUDF, prelude::create_udf};
use wasmtime::Module;

use crate::config::WasmFuncDetails;

pub fn udf_signature_from_config(
    func_details: WasmFuncDetails,
) -> Result<(Vec<DataType>, DataType)> {
    let input_types: Result<Vec<DataType>> = func_details
        .input_types
        .as_str()
        .split(",")
        .map(|s| {
            let t: DataType = s.try_into()?;
            Ok(t)
        })
        .collect();
    let return_type: DataType = func_details.return_type.as_str().try_into()?;
    Ok((input_types?, return_type))
}

pub fn udf_from_wasm_module(module: &Module, func_name: &str) -> ScalarUDF {
    create_udf()
}

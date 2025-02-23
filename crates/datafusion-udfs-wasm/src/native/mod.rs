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

use datafusion::{
    arrow::datatypes::DataType,
    error::{DataFusionError, Result},
    logical_expr::ColumnarValue,
};

pub const VALID_ARROW_DTYPES_FOR_PRIMITIVE_WASM: [DataType; 4] = [
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
];

pub mod array;
pub mod row;

pub fn validate_func(args: &[ColumnarValue], input_types: &[DataType]) -> Result<()> {
    // First check that the defined input_types and args have same number of columns
    if args.len() != input_types.len() {
        return Err(DataFusionError::Execution(
            "The number of arguments is incorrect".to_string(),
        ));
    }

    // Then check that the args are a valid type
    for col in args {
        if !VALID_ARROW_DTYPES_FOR_PRIMITIVE_WASM.contains(&col.data_type()) {
            return Err(DataFusionError::Execution(
                "Invalid input type for function".to_string(),
            ));
        }
    }

    Ok(())
}

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

use crate::native;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, PrimitiveArray,
        },
        datatypes::{self, ArrowPrimitiveType, DataType},
    },
    common::{DataFusionError, Result},
    logical_expr::ColumnarValue,
};
use wasmtime::{Instance, Module, Store, Val};

fn get_arrow_value<T>(args: &[ArrayRef], row_ix: usize, col_ix: usize) -> Result<T::Native>
where
    T: ArrowPrimitiveType,
{
    args.get(col_ix)
        .unwrap()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Error casting column {col_ix:?} to array of primitive values"
            ))
        })
        .map(|arr| arr.value(row_ix))
}

/// Extract the relevant row and column indices and convert to WASM values that can be passed to
/// WASM func
fn create_wasm_func_params(vals: &[Arc<dyn Array>], row_idx: usize) -> Result<Vec<Val>> {
    (0..vals.len())
        .map(|col_idx| match vals[col_idx].data_type() {
            DataType::Int32 => {
                let arrow_val = get_arrow_value::<datatypes::Int32Type>(vals, row_idx, col_idx)?;
                Ok(Val::I32(arrow_val))
            }
            DataType::Int64 => {
                let arrow_val = get_arrow_value::<datatypes::Int64Type>(vals, row_idx, col_idx)?;
                Ok(Val::I64(arrow_val))
            }
            DataType::Float32 => {
                let arrow_val = get_arrow_value::<datatypes::Float32Type>(vals, row_idx, col_idx)?;
                Ok(Val::F32(arrow_val.to_bits()))
            }
            DataType::Float64 => {
                let arrow_val = get_arrow_value::<datatypes::Float64Type>(vals, row_idx, col_idx)?;
                Ok(Val::F64(arrow_val.to_bits()))
            }

            _ => Err(DataFusionError::Execution(
                "Unsupported column type for WASM scalar function".to_string(),
            )),
        })
        .collect()
}

/// Given a Vec of scalar results convert to the relevant Arrow Array type
fn convert_wasm_vals_to_arrow(vals: Vec<Val>, return_type: &DataType) -> Arc<dyn Array> {
    match return_type {
        DataType::Int32 => {
            Arc::new(vals.iter().map(|r| r.unwrap_i32()).collect::<Int32Array>()) as ArrayRef
        }
        DataType::Int64 => {
            Arc::new(vals.iter().map(|r| r.unwrap_i64()).collect::<Int64Array>()) as ArrayRef
        }
        DataType::Float32 => Arc::new(
            vals.iter()
                .map(|r| r.unwrap_f32())
                .collect::<Float32Array>(),
        ) as ArrayRef,
        DataType::Float64 => Arc::new(
            vals.iter()
                .map(|r| r.unwrap_f64())
                .collect::<Float64Array>(),
        ) as ArrayRef,
        _ => panic!("unexpected type"),
    }
}

// TODO: Confirm if this is called per row / batch / etc
// TODO: Benchmark time for loading module and for extracting function
pub fn create_row_wasm_udf_impl(
    module_bytes: Vec<u8>,
    func_name: String,
    input_types: Vec<DataType>,
    return_type: DataType,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> {
    move |args: &[ColumnarValue]| {
        // First validate the arguments
        native::validate_func(args, &input_types)?;
        // Load the function again
        let mut store = Store::<()>::default();
        // TODO: Figure out if we actually need to load the binary again here or if we can use the
        // one from `try_create_wasm_udf`
        let module = Module::from_binary(store.engine(), &module_bytes)
            .map_err(|e| DataFusionError::Internal(format!("Error loading module: {e:?}")))?;
        let instance = Instance::new(&mut store, &module, &[])
            .map_err(|e| DataFusionError::Internal(format!("Error instantiating module: {e:?}")))?;
        let func = instance.get_func(&mut store, &func_name).ok_or_else(|| {
            DataFusionError::Execution(format!("Unable to access function {func_name}"))
        })?;

        let vals = ColumnarValue::values_to_arrays(args)?;
        let val_count = vals.first().unwrap().len();
        let mut results: Vec<Val> = Vec::with_capacity(val_count);
        // Need to resize because `Vec::with_capacity` doesnt set the length
        results.resize(val_count, Val::null_extern_ref());
        for row_idx in 0..val_count {
            let params = create_wasm_func_params(&vals, row_idx)?;
            func.call(&mut store, &params, &mut results[row_idx..row_idx + 1])
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Error executing function {func_name:?}: {e:?}"
                    ))
                })?;
        }
        let return_vals = convert_wasm_vals_to_arrow(results, &return_type);
        Ok(ColumnarValue::Array(return_vals))
    }
}

#[cfg(test)]
mod tests {
    use crate::{try_create_wasm_udf, WasmInputDataType, WasmUdfDetails};

    use super::*;
    use datafusion::common::assert_batches_eq;
    use datafusion::prelude::*;

    #[test]
    fn descriptive_error_when_invalid_wasm() {
        let bytes = b"invalid";
        let input_types = vec![DataType::Int32];
        let return_type = DataType::Int32;
        let udf_details = WasmUdfDetails::new(
            "my_func".to_string(),
            input_types,
            return_type,
            WasmInputDataType::Row,
        );
        let res = try_create_wasm_udf(bytes, udf_details);
        if let Some(e) = res.err() {
            assert!(e.to_string().contains("Unable to load WASM module"));
        }
    }

    #[test]
    fn descriptive_error_when_missing_function_in_wasm() {
        let bytes = std::fs::read("test-wasm/basic_wasm_example.wasm").unwrap();
        let input_types = vec![DataType::Int32];
        let return_type = DataType::Int32;
        let udf_details = WasmUdfDetails::new(
            "missing_func".to_string(),
            input_types,
            return_type,
            WasmInputDataType::Row,
        );
        let res = try_create_wasm_udf(&bytes, udf_details);
        if let Some(e) = res.err() {
            assert!(e
                .to_string()
                .contains("WASM function missing_func is missing in module"));
        }
    }

    #[tokio::test]
    async fn udf_registers_and_computes_expected_result() {
        let bytes = std::fs::read("test-wasm/basic_wasm_example.wasm").unwrap();
        let input_types = vec![DataType::Int64, DataType::Int64];
        let return_type = DataType::Int64;
        let udf_details = WasmUdfDetails::new(
            "wasm_add".to_string(),
            input_types,
            return_type,
            WasmInputDataType::Row,
        );
        let udf = try_create_wasm_udf(&bytes, udf_details).unwrap();

        let ctx = SessionContext::new();
        ctx.register_udf(udf);

        let ddl = "CREATE TABLE test AS VALUES (1,2), (3,4);";
        ctx.sql(ddl).await.unwrap().collect().await.unwrap();

        let udf_sql = "SELECT *, wasm_add(column1, column2) FROM test";
        let res = ctx.sql(udf_sql).await.unwrap().collect().await.unwrap();

        let expected = vec![
            "+---------+---------+-------------------------------------+",
            "| column1 | column2 | wasm_add(test.column1,test.column2) |",
            "+---------+---------+-------------------------------------+",
            "| 1       | 2       | 3                                   |",
            "| 3       | 4       | 7                                   |",
            "+---------+---------+-------------------------------------+",
        ];
        assert_batches_eq!(&expected, &res);
    }
}

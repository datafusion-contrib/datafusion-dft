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

use std::{io::Cursor, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef},
        datatypes::DataType,
        ipc::reader::StreamReader,
    },
    common::{DataFusionError, Result},
    logical_expr::ColumnarValue,
};
use wasi_common::{sync::WasiCtxBuilder, WasiCtx};
use wasmtime::{Engine, Instance, Linker, Module, Store, TypedFunc};

use crate::try_get_wasm_module_exported_fn;

use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::error::{ArrowError, Result as ArrowResult};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;

/// Convert &[ColumnarValue] into an Arrow IPC (stream) buffer in memory.
pub fn columnar_values_to_ipc(columnar_values: &[ColumnarValue]) -> ArrowResult<Vec<u8>> {
    // 1. Determine the maximum row count (length) among array columns
    let mut max_length = 1;
    for cv in columnar_values {
        if let ColumnarValue::Array(ref arr) = cv {
            let arr_len = arr.len();
            if arr_len > max_length {
                max_length = arr_len;
            }
        }
    }

    let mut fields = Vec::with_capacity(columnar_values.len());
    let mut arrays = Vec::with_capacity(columnar_values.len());

    // 2. Convert each ColumnarValue into an Arrow Array of length == max_length
    for (i, cv) in columnar_values.iter().enumerate() {
        let field_name = format!("column_{i}");
        match cv {
            ColumnarValue::Array(ref arr) => {
                // If the array is already the same length as the max, use it directly.
                // Otherwise (if smaller), try to broadcast if it has length == 1.
                let arr_len = arr.len();
                if arr_len == max_length {
                    fields.push(Field::new(&field_name, arr.data_type().clone(), true));
                    arrays.push(arr.clone());
                } else if arr_len == 1 {
                    // Convert this single row into a scalar and then broadcast
                    let scalar = ScalarValue::try_from_array(arr.as_ref(), 0)?;
                    let broadcasted = scalar.to_array_of_size(max_length)?;
                    fields.push(Field::new(
                        &field_name,
                        broadcasted.data_type().clone(),
                        true,
                    ));
                    arrays.push(broadcasted);
                } else {
                    // If there's a mismatch that can't be easily fixed, return an error
                    return Err(ArrowError::ComputeError(format!(
                        "Inconsistent array length {} for column '{}' vs. max_length {}",
                        arr_len, field_name, max_length
                    )));
                }
            }
            ColumnarValue::Scalar(scalar) => {
                // For scalar values, broadcast them to match max_length
                let arr = scalar.to_array_of_size(max_length)?;
                fields.push(Field::new(&field_name, arr.data_type().clone(), true));
                arrays.push(arr);
            }
        }
    }

    // 3. Build a RecordBatch from these arrays
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays)?;

    // 4. Serialize the RecordBatch to Arrow IPC (stream format) in memory
    let mut ipc_buffer = Vec::new();
    {
        let mut stream_writer = StreamWriter::try_new(&mut ipc_buffer, &batch.schema())?;
        stream_writer.write(&batch)?;
        stream_writer.finish()?;
    }

    Ok(ipc_buffer)
}

/// Converts multiple [`RecordBatch`]es into a single `Vec<ColumnarValue>`.
///
/// This function assumes:
/// 1. All `RecordBatch`es share the exact same schema.
/// 2. You want to concatenate all batches for each column.
pub fn try_record_batches_to_columnar_values(
    batches: &[RecordBatch],
) -> Result<Vec<ColumnarValue>, DataFusionError> {
    if batches.is_empty() {
        // No batches => no columns
        return Ok(vec![]);
    }

    // Ensure all batches share the same schema
    let schema = batches[0].schema();
    for batch in batches {
        if batch.schema() != schema {
            return Err(DataFusionError::Execution(
                "Inconsistent schema across record batches".to_string(),
            ));
        }
    }

    // For each column index, gather arrays from all batches, then concat
    let num_cols = schema.fields().len();
    let mut column_values = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let column_arrays: Vec<ArrayRef> = batches
            .iter()
            .map(|batch| batch.column(col_idx).clone())
            .collect();

        let concatenated = if column_arrays.len() == 1 {
            // Only one batch => no need to concatenate
            column_arrays[0].clone()
        } else {
            let array_refs: Vec<&dyn Array> = column_arrays.iter().map(|a| a.as_ref()).collect();
            // Concatenate arrays across all batches for this column
            concat(&array_refs).map_err(|e| {
                DataFusionError::Execution(format!("Error concatenating arrays: {e}"))
            })?
        };

        column_values.push(ColumnarValue::Array(concatenated));
    }

    Ok(column_values)
}

pub fn create_arrow_ipc_wasm_udf_impl(
    module_bytes: Vec<u8>,
    func_name: String,
    input_types: Vec<DataType>,
    return_type: DataType,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> {
    move |args: &[ColumnarValue]| {
        let engine = Engine::default();
        let linker: Linker<WasiCtx> = Linker::new(&engine);
        // Create a WASI context and put it in a Store; all instances in the store
        // share this context. `WasiCtxBuilder` provides a number of ways to
        // configure what the target program will have access to.
        let wasi = WasiCtxBuilder::new().inherit_stderr().build();
        let mut store = Store::new(&engine, wasi);

        let module = Module::from_binary(store.engine(), &module_bytes)
            .map_err(|e| DataFusionError::Internal(format!("Error loading module: {e:?}")))?;
        let instance = Instance::new(&mut store, &module, &[])
            .map_err(|e| DataFusionError::Internal(format!("Error instantiating module: {e:?}")))?;

        let alloc: TypedFunc<i32, i32> =
            try_get_wasm_module_exported_fn(&instance, &mut store, "alloc")?;
        let dealloc: TypedFunc<(i32, i32), ()> =
            try_get_wasm_module_exported_fn(&instance, &mut store, "dealloc")?;

        let func: TypedFunc<(i32, i32), (i32, i32)> =
            try_get_wasm_module_exported_fn(&instance, &mut store, &func_name)?;

        let memory =
            instance
                .get_memory(&mut store, "memory")
                .ok_or(DataFusionError::Execution(
                    "Missing memory in module".to_string(),
                ))?;

        let ipc_bytes = columnar_values_to_ipc(args)?;
        let input_offset = alloc
            .call(&mut store, ipc_bytes.len() as i32)
            .map_err(|e| {
                DataFusionError::Execution(format!("Unable to allocate WASM memory: {}", e))
            })?;

        memory
            .write(&mut store, input_offset as usize, &ipc_bytes)
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Unable to write Arrow IPC to WASM memory: {}",
                    e
                ))
            })?;

        let (output_offset, output_len) = func
            .call(&mut store, (input_offset, ipc_bytes.len() as i32))
            .map_err(|e| {
                DataFusionError::Execution(format!("Error executing Arrow IPC WASM func: {}", e))
            })?;

        let output_len_usize = output_len as usize;
        let mut output_ipc_bytes = vec![0u8; output_len_usize];
        memory
            .read(&mut store, output_offset as usize, &mut output_ipc_bytes)
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Unable to read Arrow IPC from WASM memory: {}",
                    e
                ))
            })?;

        dealloc
            .call(&mut store, (input_offset, ipc_bytes.len() as i32))
            .map_err(|e| {
                DataFusionError::Execution(format!("Unable to deallocate input buffer: {}", e))
            })?;

        dealloc
            .call(&mut store, (output_offset, output_len))
            .map_err(|e| {
                DataFusionError::Execution(format!("Unable to deallocate output buffer: {}", e))
            })?;

        let reader = StreamReader::try_new(Cursor::new(output_ipc_bytes), None)?;
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch?);
        }

        let results = try_record_batches_to_columnar_values(&batches)?;
        let result = results.into_iter().next().ok_or_else(|| {
            DataFusionError::Execution("Error getting result columnar value".to_string())
        })?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::{try_create_wasm_udf, WasmInputDataType, WasmUdfDetails};

    use super::*;
    use datafusion::common::assert_batches_eq;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn udf_registers_and_computes_expected_result() {
        let bytes = std::fs::read("test-wasm/wasm_examples.wasm").unwrap();
        let input_types = vec![DataType::Int64, DataType::Int64];
        let return_type = DataType::Int64;
        let udf_details = WasmUdfDetails::new(
            "arrow_func".to_string(),
            input_types,
            return_type,
            WasmInputDataType::ArrowIpc,
        );
        let udf = try_create_wasm_udf(&bytes, udf_details).unwrap();

        let ctx = SessionContext::new();
        ctx.register_udf(udf);

        let ddl = "CREATE TABLE test AS VALUES (1,2), (3,4);";
        ctx.sql(ddl).await.unwrap().collect().await.unwrap();

        let udf_sql = "SELECT *, arrow_func(column1, column2) FROM test";
        let res = ctx.sql(udf_sql).await.unwrap().collect().await.unwrap();

        let expected = vec![
            "+---------+---------+-------------------------------------+",
            "| column1 | column2 | arrow_func(test.column1,test.column2) |",
            "+---------+---------+-------------------------------------+",
            "| 1       | 2       | 3                                   |",
            "| 3       | 4       | 7                                   |",
            "+---------+---------+-------------------------------------+",
        ];
        assert_batches_eq!(&expected, &res);
    }

    // use super::*;
    // use datafusion::common::assert_batches_eq;
    // use datafusion::prelude::*;

    // #[test]
    // fn descriptive_error_when_invalid_wasm() {
    //     let bytes = b"invalid";
    //     let input_types = vec![DataType::Int32];
    //     let return_type = DataType::Int32;
    //     let udf_details = WasmUdfDetails::new(
    //         "my_func".to_string(),
    //         input_types,
    //         return_type,
    //         WasmInputDataType::Row,
    //     );
    //     let res = try_create_wasm_udf(bytes, udf_details);
    //     if let Some(e) = res.err() {
    //         assert!(e.to_string().contains("Unable to load WASM module"));
    //     }
    // }

    // #[test]
    // fn descriptive_error_when_missing_function_in_wasm() {
    //     let bytes = std::fs::read("test-wasm/wasm_examples.wasm").unwrap();
    //     let input_types = vec![DataType::Int32];
    //     let return_type = DataType::Int32;
    //     let udf_details = WasmUdfDetails::new(
    //         "missing_func".to_string(),
    //         input_types,
    //         return_type,
    //         WasmInputDataType::Row,
    //     );
    //     let res = try_create_wasm_udf(&bytes, udf_details);
    //     if let Some(e) = res.err() {
    //         assert!(e
    //             .to_string()
    //             .contains("WASM function missing_func is missing in module"));
    //     }
    // }

    // #[tokio::test]
    // async fn udf_registers_and_computes_expected_result() {
    //     let bytes = std::fs::read("test-wasm/wasm_examples.wasm").unwrap();
    //     let input_types = vec![DataType::Int64, DataType::Int64];
    //     let return_type = DataType::Int64;
    //     let udf_details = WasmUdfDetails::new(
    //         "wasm_add".to_string(),
    //         input_types,
    //         return_type,
    //         WasmInputDataType::Row,
    //     );
    //     let udf = try_create_wasm_udf(&bytes, udf_details).unwrap();
    //
    //     let ctx = SessionContext::new();
    //     ctx.register_udf(udf);
    //
    //     let ddl = "CREATE TABLE test AS VALUES (1,2), (3,4);";
    //     ctx.sql(ddl).await.unwrap().collect().await.unwrap();
    //
    //     let udf_sql = "SELECT *, wasm_add(column1, column2) FROM test";
    //     let res = ctx.sql(udf_sql).await.unwrap().collect().await.unwrap();
    //
    //     let expected = vec![
    //         "+---------+---------+-------------------------------------+",
    //         "| column1 | column2 | wasm_add(test.column1,test.column2) |",
    //         "+---------+---------+-------------------------------------+",
    //         "| 1       | 2       | 3                                   |",
    //         "| 3       | 4       | 7                                   |",
    //         "+---------+---------+-------------------------------------+",
    //     ];
    //     assert_batches_eq!(&expected, &res);
    // }
}

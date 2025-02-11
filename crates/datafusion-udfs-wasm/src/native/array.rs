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

// use std::sync::Arc;
//
// use datafusion::{
//     arrow::{
//         array::{
//             Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, PrimitiveArray,
//         },
//         datatypes::{self, ArrowPrimitiveType, DataType, Int32Type},
//     },
//     common::{DataFusionError, Result},
//     logical_expr::ColumnarValue,
// };
// use wasi_common::{sync::WasiCtxBuilder, WasiCtx};
// use wasmtime::{Engine, Func, Instance, Linker, Memory, Module, Store, TypedFunc, Val};
//
// use super::validate_func;
// use crate::try_get_wasm_module_exported_fn;
//
// fn get_arrow_value<T>(args: &[ArrayRef], row_ix: usize, col_ix: usize) -> Result<T::Native>
// where
//     T: ArrowPrimitiveType,
// {
//     args.get(col_ix)
//         .unwrap()
//         .as_any()
//         .downcast_ref::<PrimitiveArray<T>>()
//         .ok_or_else(|| {
//             DataFusionError::Internal(format!(
//                 "Error casting column {col_ix:?} to array of primitive values"
//             ))
//         })
//         .map(|arr| arr.value(row_ix))
// }
//
// /// Allocates space in Wasm memory for a `PrimitiveArray<T>` (e.g. Int32Array),
// /// copies its contents, and returns `(offset, length_in_elements)`.
// ///
// /// - `alloc` is the exported Wasm "alloc" function
// /// - `memory` is the Wasm linear memory
// /// - `store` is the wasmtime store
// /// - `array` is the Arrow array to copy (already downcasted to PrimitiveArray<T>)
// ///
// /// The function handles nulls by writing some default (e.g. 0 for numbers) into the slot.
// /// You could handle null differently, e.g. also store a null bitmap in Wasm.
// // pub fn alloc_primitive_array<T: ArrowPrimitiveType>(
// //     alloc: &TypedFunc<i32, i32>,
// //     memory: &Memory,
// //     store: &mut Store<WasiCtx>,
// //     array: &PrimitiveArray<T>,
// // ) -> Result<(i32, i32), DataFusionError> {
// //     // Number of elements
// //     let len = array.len();
// //     // How many bytes total we need
// //     let elem_size = std::mem::size_of::<T::Native>();
// //     let total_bytes = (len * elem_size) as i32;
// //
// //     // Call the Wasm alloc function
// //     let alloc_offset = alloc
// //         .call(store, total_bytes)
// //         .map_err(|err| DataFusionError::Execution(format!("alloc failed: {err:?}")))?;
// //
// //     let offset_usize = alloc_offset as usize;
// //
// //     // Copy each element's bytes into Wasm memory
// //     let data = memory.data_mut(store);
// //     for (i, maybe_val) in array.iter().enumerate() {
// //         let val = maybe_val.unwrap_or_else(|| T::default_value());
// //         let start = offset_usize + i * elem_size;
// //         let bytes = val.to_le_bytes();
// //         data[start..(start + elem_size)].copy_from_slice(&bytes);
// //     }
// //
// //     // Return (offset, length_in_elements)
// //     Ok((offset, len as i32))
// // }
//
// pub fn create_array_wasm_udf_impl(
//     module_bytes: Vec<u8>,
//     func_name: String,
//     input_types: Vec<DataType>,
//     return_type: DataType,
// ) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> {
//     move |args: &[ColumnarValue]| {
//         validate_func(args, &input_types)?;
//         let engine = Engine::default();
//         let mut linker: Linker<WasiCtx> = Linker::new(&engine);
//         // Create a WASI context and put it in a Store; all instances in the store
//         // share this context. `WasiCtxBuilder` provides a number of ways to
//         // configure what the target program will have access to.
//         let wasi = WasiCtxBuilder::new().inherit_stderr().build();
//         let mut store = Store::new(&engine, wasi);
//
//         let module = Module::from_binary(store.engine(), &module_bytes)
//             .map_err(|e| DataFusionError::Internal(format!("Error loading module: {e:?}")))?;
//         let instance = Instance::new(&mut store, &module, &[])
//             .map_err(|e| DataFusionError::Internal(format!("Error instantiating module: {e:?}")))?;
//         let func = instance.get_func(&mut store, &func_name).ok_or_else(|| {
//             DataFusionError::Execution(format!("Unable to access function {func_name}"))
//         })?;
//
//         let alloc = try_get_wasm_module_exported_fn(&instance, &mut store, "alloc");
//         let dealloc = instance.get_func(&mut store, "dealloc").ok_or_else(|| {
//             DataFusionError::Execution(format!("Unable to access function {func_name}"))
//         })?;
//         let memory =
//             instance
//                 .get_memory(&mut store, "memory")
//                 .ok_or(DataFusionError::Execution(
//                     "Missing memory in module".to_string(),
//                 ))?;
//
//         let mut offsets_and_lengths = Vec::new();
//
//         for c in args {
//             match c {
//                 ColumnarValue::Scalar(s) => {}
//                 ColumnarValue::Array(array) => {
//                     if let Some(int_arr) = array.as_any().downcast_ref::<Int32Array>() {
//                         let (offset, len) = alloc_primitive_array::<Int32Type>(
//                             &alloc, &memory, &mut store, int_arr,
//                         )?;
//                         offsets_and_lengths.push((offset, len));
//                     } else {
//                         // Handle other arrays (Float64Array, StringArray, etc.)
//                         // For each primitive type, call `alloc_primitive_array`.
//                         // For non-primitive, you'll need a custom approach.
//                     }
//                 }
//             }
//         }
//
//         Ok(ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Null))
//     }
// }
//
// #[cfg(test)]
// mod tests {}

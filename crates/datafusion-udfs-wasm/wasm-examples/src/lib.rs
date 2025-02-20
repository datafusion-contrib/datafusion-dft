use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
// use std::{mem, os::raw::c_void};

/// Allocate memory into the module's linear memory
/// and return the offset to the start of the block.
///
/// From https://radu-matei.com/blog/practical-guide-to-wasm-memory/
#[no_mangle]
pub fn alloc(len: usize) -> *mut u8 {
    // create a new mutable buffer with capacity `len`
    let mut buf = Vec::with_capacity(len);
    // take a mutable pointer to the buffer
    let ptr = buf.as_mut_ptr();
    // take ownership of the memory block and
    // ensure that its destructor is not
    // called when the object goes out of scope
    // at the end of the function
    std::mem::forget(buf);
    // return the pointer so the runtime
    // can write data at this offset
    return ptr;
}

#[no_mangle]
pub fn dealloc(ptr: usize, offset: usize) {}

#[no_mangle]
pub fn wasm_add(left: i64, right: i64) -> i64 {
    left + right
}

#[no_mangle]
pub fn arrow_func(ptr: *mut u8, len: i32) -> (*mut u8, i32) {
    // 1. Read the input (Arrow IPC bytes) from WASM memory
    let input_data = unsafe { Vec::from_raw_parts(ptr, len as usize, len as usize) };

    // 2. Parse the Arrow IPC data into RecordBatches
    let c = Cursor::new(input_data);
    let mut reader =
        StreamReader::try_new(c, None).expect("Failed to create StreamReader from Arrow IPC input");

    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = reader.next() {
        batches.push(batch.expect("Failed to read RecordBatch from Arrow stream"));
    }

    // 3. Example transformation: just keep the first column from each batch
    //    (In arrow-rs, there's no direct `project(&[0])` method on `RecordBatch`,
    //     so we manually create a new batch with only the first column.)
    let projected: Vec<RecordBatch> = batches
        .iter()
        .map(|batch| {
            let schema = batch.schema();
            if schema.fields().is_empty() {
                // No columns to project; return original or an empty batch
                batch.clone()
            } else {
                let first_col = batch.column(0).clone();
                // Build a new one-column schema
                let new_schema = arrow::datatypes::Schema::new(vec![schema.field(0).clone()]);
                RecordBatch::try_new(std::sync::Arc::new(new_schema), vec![first_col])
                    .expect("Failed to create projected RecordBatch")
            }
        })
        .collect();

    // 4. Write the new batches back to Arrow IPC in-memory
    let mut out_buf = Vec::new();
    if !projected.is_empty() {
        let schema = projected[0].schema();
        let mut writer =
            StreamWriter::try_new(&mut out_buf, &schema).expect("Failed to create StreamWriter");

        for batch in &projected {
            writer.write(batch).expect("Failed to write batch");
        }
        writer.finish().expect("Failed to finish stream");
    }

    // out_buf now contains our resulting Arrow IPC bytes
    // let arrow_result_bytes = out_buf;

    // 5. Allocate memory inside the WASM module for the output
    let output_len = out_buf.len() as i32;
    let output_offset = alloc(output_len as usize);

    // 6. Copy the result bytes into WASM memory
    unsafe {
        let output_slice = std::slice::from_raw_parts_mut(output_offset, output_len as usize);
        output_slice.copy_from_slice(&out_buf);
    }

    // 7. Return (output_offset, output_len) so the host knows where to read
    (output_offset, output_len)
}

// #[no_mangle]
// pub extern "C" fn arrow_func(input_offset: i32, input_len: i32) -> (i32, i32) {
//     // 1. Read the input from Wasm memory
//     let input_data = unsafe {
//         // Pointer to the start of our static memory:
//         let base_ptr = WASM_MEMORY.as_ptr();
//         // Create a slice for the given offset/length
//         std::slice::from_raw_parts(base_ptr.add(input_offset as usize), input_len as usize)
//     };
//
//     let c = std::io::Cursor::new(input_data);
//
//     let reader = StreamReader::try_new(c, None).unwrap();
//
//     let mut batches = Vec::new();
//     while let Some(batch) = reader.next() {
//         batches.push(batch.unwrap());
//     }
//
//     let only_first_col = batches.iter().map(|b| b.project(&[0]));
//
//     let mut stream_writer =
//         StreamWriter::try_new(&mut WASM_MEMORY, &only_first_col[0].schema()).unwrap();
//     for batch in batches {
//         stream_writer.write(&batch).unwrap();
//     }
//     stream_writer.finish().unwrap();
//
//     // 3. Allocate memory for the output
//     let output_len = arrow_result_bytes.len() as i32;
//     let output_offset = alloc(output_len);
//
//     // 4. Write the output bytes back into Wasm memory
//     unsafe {
//         let base_ptr = WASM_MEMORY.as_mut_ptr();
//         let output_slice = std::slice::from_raw_parts_mut(
//             base_ptr.add(output_offset as usize),
//             output_len as usize,
//         );
//         output_slice.copy_from_slice(&arrow_result_bytes);
//     }
//
//     // 5. Return (output_offset, output_len).
//     //    In Wasmtime, you can return multiple values by returning a tuple.
//     //    If your environment doesn't support multi-value returns,
//     //    return a single i64 and pack the offset + length yourself.
//     (output_offset, output_len)
// }
//
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = wasm_add(2, 2);
        assert_eq!(result, 4);
    }
}

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;

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
    ptr
}

#[no_mangle]
pub fn dealloc(ptr: usize, offset: usize) {}

#[no_mangle]
pub fn wasm_add(left: i64, right: i64) -> i64 {
    left + right
}

#[no_mangle]
pub unsafe fn arrow_func(ptr: *mut u8, len: i32) -> u64 {
    // 1. Read the input (Arrow IPC bytes) from WASM memory
    let input_data = unsafe { Vec::from_raw_parts(ptr, len as usize, len as usize) };

    // 2. Parse the Arrow IPC data into RecordBatches
    let c = Cursor::new(input_data);
    let reader =
        StreamReader::try_new(c, None).expect("Failed to create StreamReader from Arrow IPC input");

    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch in reader {
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

    // 5. Allocate memory inside the WASM module for the output
    let output_len = out_buf.len() as i32;
    let output_offset = alloc(output_len as usize);

    // 6. Copy the result bytes into WASM memory
    unsafe {
        let output_slice = std::slice::from_raw_parts_mut(output_offset, output_len as usize);
        output_slice.copy_from_slice(&out_buf);
    }

    // 7. Return (output_offset, output_len) so the host knows where to read
    ((output_offset as u64) << 32) | (output_len as u64 & 0xFFFF_FFFF)
}

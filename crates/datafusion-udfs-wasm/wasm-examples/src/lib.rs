use std::{mem, os::raw::c_void};

#[no_mangle]
pub fn wasm_add(left: i64, right: i64) -> i64 {
    left + right
}

/// Allocate memory to be used by WASM host
#[no_mangle]
pub fn alloc(size: usize) -> *mut c_void {
    let mut buffer: Vec<u8> = Vec::with_capacity(size);
    let pointer = buffer.as_mut_ptr();
    // We must forget so that the buffer outlives the function and can continue to be used by WASM
    // module after `alloc` is called.
    mem::forget(buffer);

    pointer as *mut c_void
}

/// .
///
/// # Safety
///
/// As long as `pointer` was allocated from `Vec`, which it was above, then invariants are held.
#[no_mangle]
pub unsafe fn dealloc(pointer: *mut c_void, capacity: usize) {
    unsafe {
        let _ = Vec::from_raw_parts(pointer, 0, capacity);
    }
}

#[no_mangle]
pub fn wasm_vectorized_add() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = wasm_add(2, 2);
        assert_eq!(result, 4);
    }
}

#[no_mangle]
pub fn wasm_add(left: i64, right: i64) -> i64 {
    left + right
}

/// Export WASM LinearMemory to be used by WASM runtime
#[no_mangle]
pub static mut VECTORIZED_MEMORY: [u8; 0x100000] = [0; 0x100000]; // 1MB

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

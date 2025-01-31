#[no_mangle]
pub fn wasm_add(left: i64, right: i64) -> i64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = wasm_add(2, 2);
        assert_eq!(result, 4);
    }
}

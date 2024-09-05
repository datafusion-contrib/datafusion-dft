use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::error::ArrowError;
use std::sync::Arc;
use wasm_udfs::*;

// ```bash
// cargo install wasm-bindgen-cli
// ```

// ```bash
// cargo test --target wasm32-unknown-unknown
// ```

// expose function f1 as external function
// add required bindgen, and required serialization/deserialization
export_udf_function!(f1);
// function should return error
export_udf_function!(f_return_error);
// function should panic
// export_udf_function!(f_panic);
// function should return arrow error
export_udf_function!(f_return_arrow_error);

/// standard datafusion udf ... kind of
/// should return ArrayRef or ArrowError
fn f1(args: &[ArrayRef]) -> Result<ArrayRef, ArrowError> {
    assert_eq!(2, args.len());

    let base = args[0]
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("cast 0 failed");
    let exponent = args[1]
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("cast 1 failed");

    assert_eq!(exponent.len(), base.len());

    let array = base
        .iter()
        .zip(exponent.iter())
        .map(|(base, exponent)| match (base, exponent) {
            (Some(base), Some(exponent)) => Some(base.powf(exponent)),
            _ => None,
        })
        .collect::<Float64Array>();

    // TODO: do we need arc here?
    //       only reason to stay to keep api same
    //       like datafusion udf's
    Ok(Arc::new(array))
}
/// function returns String Error
fn f_return_error(_args: &[ArrayRef]) -> Result<ArrayRef, String> {
    Err("wasm function returned error".to_string())
}

/// function returns error
fn f_return_arrow_error(_args: &[ArrayRef]) -> Result<ArrayRef, ArrowError> {
    Err(ArrowError::DivideByZero)
}

// fn f_panic(_args: &[ArrayRef]) -> Result<ArrayRef, String> {
//     panic!("wasm function panicked")
// }

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array};

    use std::sync::Arc;

    #[wasm_bindgen_test::wasm_bindgen_test]
    fn test_f1() {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![2.1, 3.1, 4.1, 5.1]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
        let args = vec![a, b];
        let result = f1(&args).unwrap();

        assert_eq!(4, result.len())
    }
}

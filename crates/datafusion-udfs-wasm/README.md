# DataFusion UDFs WASM

Provides a [wasmtime](https://github.com/bytecodealliance/wasmtime) based implementation for WASM UDFs for DataFusion.

## Features

Currently there are two supported UDF input types.

1. `Row` => The exported WASM function is called once per row.  Only WASM native types (`i32`, `i64`, `f32`, `f64`) are supported.
2. `ArrowIpc` => The exported WASM module exposes linear memory that the host writes Arrow IPC bytes to and then the module writes the result Arrow IPC bytes for the host to read.

### Row

Simply specify the signature in `WasmUdfDetails` and it will be automatically checked.

### Arrow IPC

The exported WASM function that operates on Arrow IPC data must take two `i32` as input - the first being an offset into memory and the second the number of bytes for the Arrow IPC data.  The function must return an `i64` which is packed with the result offset as the last 32 bits and number of bytes of the result as the first 32 bits.  We do this because at the time of writing I was not able to get WASM to export functions that return more than one value. As I learn more about WASM this signature may change.

## Benchmarks

The crate includes Criterion benchmarks for measuring the startup time of various wasmtime components. These can help identify potential performance bottlenecks in the WASM UDF initialization process.

To run the benchmarks:

```bash
cargo bench --bench wasm_startup
```

For more details, see the [benchmarks README](benches/README.md).



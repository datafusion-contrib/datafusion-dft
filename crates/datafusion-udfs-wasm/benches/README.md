# WASM UDF Startup Time Benchmarks

This directory contains benchmarks focused on measuring the startup time for different aspects of WebAssembly (WASM) components in the DataFusion UDF system.

## Benchmarked Components

The benchmarks measure the following aspects:

1. **Engine Creation**: Time to create a Wasmtime engine with different configurations.
2. **Module Loading**: Time to load a WASM module from binary.
3. **Instance Creation**: Time to instantiate a WASM module with different store configurations.
4. **UDF Creation**: Time to create both row-based and Arrow IPC-based UDFs.
5. **Function Extraction**: Time to extract functions from a WASM module instance.

## Running the Benchmarks

To run all benchmarks:

```bash
cargo bench --bench wasm_startup
```

To run a specific benchmark group:

```bash
cargo bench --bench wasm_startup -- engine_creation
```

To run with a smaller sample size for quicker results:

```bash
cargo bench --bench wasm_startup -- --sample-size 10
```

## Interpreting Results

The benchmarks are designed to identify potential performance bottlenecks in the WASM UDF startup process. Key insights:

- **ArrowIpc UDFs vs. Row-based UDFs**: The benchmarks show a significant performance difference between these two UDF types.
- **Engine Creation**: Relatively fast operation, with different optimization levels having minimal impact.
- **Module Loading**: Can be a significant part of startup time.
- **Instance Creation**: Shows the overhead of adding WASI context to a module instance.
- **Function Extraction**: Very fast for both regular and typed function extraction.

These benchmarks can help identify areas for optimizing the WASM UDF startup time in DataFusion.
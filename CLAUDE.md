# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`dft` (datafusion-dft) is a batteries-included suite of DataFusion applications providing four interfaces: TUI, CLI, FlightSQL Server, and HTTP Server. All interfaces share a common execution engine built on Apache DataFusion and Apache Arrow.

## Building and Running

### Development Commands

```bash
# Build the project (default features: functions-parquet, s3)
cargo build

# Build with TUI support
cargo build --features=tui

# Build with all features
cargo build --all-features

# Run the TUI (requires tui feature)
cargo run --features=tui

# Run CLI with a query
cargo run -- -c "SELECT 1 + 2"

# Start HTTP server (requires http feature)
cargo run --features=http -- serve-http

# Start FlightSQL server (requires flightsql feature)
cargo run --features=flightsql -- serve-flightsql

# Generate TPC-H data
cargo run -- generate-tpch
```

### Benchmarking

Benchmarks measure query performance with detailed timing breakdowns:

```bash
# Serial benchmark (default, 10 iterations)
cargo run -- -c "SELECT 1" --bench

# Custom iteration count
cargo run -- -c "SELECT 1" --bench -n 100

# Concurrent benchmark (measures throughput under load)
cargo run -- -c "SELECT 1" --bench --concurrent

# With custom iterations and concurrency
cargo run -- -c "SELECT 1" --bench -n 100 --concurrent

# Save results to CSV
cargo run -- -c "SELECT 1" --bench --save results.csv

# Append to existing results
cargo run -- -c "SELECT 2" --bench --concurrent --save results.csv --append

# Warm up cache before benchmarking
cargo run -- -c "SELECT * FROM t" --bench --run-before "CREATE TABLE t AS VALUES (1)"
```

**Benchmark Modes:**
- **Serial** (default): Measures query performance in isolation
  - Shows pure query execution time without contention
  - Ideal for understanding baseline performance

- **Concurrent** (`--concurrent`): Measures performance under load
  - Runs iterations in parallel (concurrency = min(iterations, CPU cores))
  - Shows throughput (queries/second) with multiple clients
  - Reveals resource contention and bottlenecks
  - Higher mean/median times are expected due to concurrent load

**Output:**
- Timing breakdown: logical planning, physical planning, execution, total
- Statistics: min, max, mean, median for each phase
- CSV format includes `concurrency_mode` column (serial or concurrent(N))

**FlightSQL Benchmarks:**
```bash
# Benchmark FlightSQL server (requires --flightsql flag and server running)
cargo run -- -c "SELECT 1" --bench --flightsql --concurrent
```

### Testing

Tests are organized by feature and component:

```bash
# Run core database tests
cargo test db

# Run CLI tests
cargo test cli_cases

# Run TUI tests (requires tui feature)
cargo test --features=tui tui_cases

# Run feature-specific tests
cargo test --features=flightsql extension_cases::flightsql -- --test-threads=1
cargo test --features=s3 extension_cases::s3
cargo test --features=functions-json extension_cases::functions_json
cargo test --features=deltalake extension_cases::deltalake
cargo test --features="deltalake s3" extension_cases::deltalake::test_deltalake_s3  # Requires LocalStack
cargo test --features=udfs-wasm extension_cases::udfs_wasm
cargo test --features=vortex extension_cases::vortex
cargo test --features=vortex cli_cases::basic::test_output_vortex

# Run tests for specific crates
cargo test --manifest-path crates/datafusion-app/Cargo.toml --all-features
cargo test --manifest-path crates/datafusion-functions-parquet/Cargo.toml
cargo test --manifest-path crates/datafusion-udfs-wasm/Cargo.toml

# Run a single test
cargo test <test_name>
```

Note: FlightSQL tests require `--test-threads=1` because they spin up servers on the same port.

### Code Quality

```bash
# Format code
cargo fmt --all

# Check formatting (CI check)
cargo fmt --all -- --check

# Run clippy
cargo clippy --all-features --workspace -- -D warnings

# Check for unused dependencies
cargo machete

# Format TOML files
taplo format --check
```

## Architecture

### Crate Structure

The project is organized as a workspace with multiple crates:

- **Root crate (`datafusion-dft`)**: Main binary and application logic
  - `src/main.rs` - Entry point that routes to TUI, CLI, or servers
  - `src/tui/` - TUI implementation using ratatui
  - `src/cli/` - CLI implementation
  - `src/server/` - HTTP and FlightSQL server implementations
  - `src/config.rs` - Configuration management
  - `src/args.rs` - Command-line argument parsing

- **`crates/datafusion-app`**: Core execution engine (reusable library)
  - `src/local.rs` - ExecutionContext wrapping DataFusion SessionContext
  - `src/executor/` - Dedicated executors for CPU-intensive work (inspired by InfluxDB)
  - `src/catalog/` - Catalog management
  - `src/extensions/` - DataFusion extensions
  - `src/tables/` - Table provider implementations
  - `src/stats.rs` - Query execution statistics
  - `src/config.rs` - Execution configuration

- **`crates/datafusion-functions-parquet`**: Parquet-specific UDFs

- **`crates/datafusion-udfs-wasm`**: WASM-based UDF support

- **`crates/datafusion-auth`**: Authentication implementations

- **`crates/datafusion-ffi-table-providers`**: FFI table provider support

### Key Components

#### ExecutionContext
The `ExecutionContext` (in `crates/datafusion-app/src/local.rs`) is the core abstraction that wraps DataFusion's `SessionContext` with:
- Extension registration (UDFs, table formats, object stores)
- DDL file execution
- Dedicated executor for CPU-intensive work
- Query execution and statistics collection
- Observability integration

#### TUI Architecture
The TUI (in `src/tui/`) follows a state-based architecture:
- `src/tui/state/` - Application state management with tab-specific state
- `src/tui/ui/` - Rendering logic separated from state
- `src/tui/handlers/` - Event handling
- `src/tui/execution.rs` - Async query execution

Built with ratatui and crossterm.

#### Server Implementations
- **FlightSQL**: `src/server/flightsql/` - Arrow Flight SQL protocol server
- **HTTP**: `src/server/http/` - REST API using Axum

Both servers share the same `ExecutionContext` from `datafusion-app`.

### Feature Flags

The project uses extensive feature flags to keep binary size manageable:

- `tui` - Terminal user interface (ratatui-based)
- `s3` - S3 object store integration (default)
- `functions-parquet` - Parquet-specific functions (default)
- `functions-json` - JSON functions
- `deltalake` - Delta Lake table format support
- `vortex` - Vortex file format support
- `flightsql` - FlightSQL server and client
- `http` - HTTP server
- `huggingface` - HuggingFace dataset integration
- `udfs-wasm` - WASM UDF support
- `observability` - Metrics and tracing (required by servers)

When adding code that depends on a feature, use conditional compilation:
```rust
#[cfg(feature = "flightsql")]
use datafusion_app::flightsql;
```

### Configuration

Configuration files use TOML and are located in `~/.config/dft/`. Key config files:
- Main config: `~/.config/dft/config.toml`
- DDL file: `~/.config/dft/ddl.sql` (auto-loaded by TUI)

See `src/config.rs` and `crates/datafusion-app/src/config.rs` for configuration structure.

### Executor Pattern

The project uses a dedicated executor pattern (inspired by InfluxDB) for CPU-intensive work. This separates network I/O (on the main Tokio runtime) from CPU-bound query execution. See `crates/datafusion-app/src/executor/`.

The main runtime in `src/main.rs` uses a single-threaded Tokio runtime optimized for network I/O.

## Development Workflow

### Adding New Features

1. Update `Cargo.toml` feature flags if needed
2. Add the feature to CI test matrix in `.github/workflows/test.yml`
3. Implement feature in appropriate crate
4. Add tests in the `extension_cases` or `crate` test suites
5. Update documentation

### Testing Against LocalStack

Some tests (S3, TUI, CLI, Delta Lake + S3) require LocalStack for S3 testing. The CI workflow shows the setup:

```bash
# Start LocalStack
localstack start -d
awslocal s3api create-bucket --bucket test --acl public-read
awslocal s3 mv data/aggregate_test_100.csv s3://test/

# Run S3 tests
cargo test --features=s3 extension_cases::s3

# For Delta Lake + S3 tests, also sync the delta lake data
awslocal s3 sync data/deltalake/simple_table s3://test/deltalake/simple_table
cargo test --features="deltalake s3" extension_cases::deltalake::test_deltalake_s3
```

### Benchmarking

The project includes benchmarking support:

```bash
# Start HTTP server for benchmarking
just serve-http

# Run basic HTTP benchmark (requires oha tool)
just bench-http-basic

# Run custom benchmark
just bench-http-custom <file>
```

Criterion benchmarks are available in `crates/datafusion-app/benches/`.

## Common Patterns

### Adding a New UDF

1. Implement in `crates/datafusion-app/src/extensions/`
2. Register in the appropriate extension registration function
3. Add tests with SQL queries exercising the UDF

### Adding Table Provider Support

1. Implement in `crates/datafusion-app/src/tables/`
2. Register in catalog creation (`src/catalog/`)
3. Add integration tests

### Working with the TUI

The TUI uses a tab-based interface. Each tab has:
- State struct in `src/tui/state/tabs/`
- UI rendering in `src/tui/ui/tabs/`
- Event handlers in `src/tui/handlers/`

When modifying TUI code, ensure proper separation between state management and rendering.

## Important Notes

- The project is licensed under Apache 2.0
- Clippy lint `clone_on_ref_ptr` is set to "deny"
- The main Tokio runtime should only be used for network I/O (single-threaded)
- CPU-intensive query execution uses dedicated executors
- FlightSQL tests must run with `--test-threads=1` due to port conflicts
- All server implementations share the same execution engine

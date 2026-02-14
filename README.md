# dft - Batteries included DataFusion

## 🚧 DOCS UNDER CONSTRUCTION
Documentation is undergoing a significant revamp - the new documentation will be finalized as part of the v0.3 release in the late Spring or early Summer of 2025.

## Overview

`dft` is a batteries-included suite of [DataFusion](https://github.com/apache/arrow-datafusion) applications that provides:

- **Data Source Integration**: Query files from S3, local filesystems, or HuggingFace datasets
- **Table Format Support**: Native support for Delta Lake
- **Extensibility**: UDFs defined in WASM (and soon Python)
- **Helper Functions**: Built-in functions for JSON and Parquet data processing

The project offers four complementary interfaces:

1. **Text User Interface (TUI)**: An interactive SQL IDE with real-time query analysis, benchmarking, and catalog exploration (requires `tui` feature)
2. **Command Line Interface (CLI)**: A scriptable engine for executing queries from files or command line
3. **FlightSQL Server**: A standards-compliant SQL interface for programmatic access (requires `flightsql` feature)
4. **HTTP Server**: A REST API for SQL queries and catalog exploration (requires `http` feature)

All interfaces share the same execution engine, allowing you to develop locally with the TUI and then seamlessly deploy with the server implementations.

`dft` builds upon [`datafusion-cli`](https://datafusion.apache.org/user-guide/cli/overview.html) with enhanced interactivity, additional integrations, and ready-to-use server implementations.

## User Guide

### Installation

#### From crates.io (Recommended)
```sh
# Core CLI and server interfaces
cargo install datafusion-dft

# With TUI interface
cargo install datafusion-dft --features=tui

# For full functionality with all features (including TUI)
cargo install datafusion-dft --all-features
```

If you don't have Rust installed, follow the [installation instructions](https://www.rust-lang.org/tools/install).

#### Feature Flags
Common feature combinations:
```sh
# Core with S3 support
cargo install datafusion-dft --features=s3

# With TUI interface
cargo install datafusion-dft --features=tui

# TUI with S3 and data lake formats
cargo install datafusion-dft --features=tui,s3,deltalake

# Data lake formats
cargo install datafusion-dft --features=deltalake

# With JSON and Parquet functions
cargo install datafusion-dft --features=functions-json,functions-parquet
```

See the [Features documentation](docs/features.md) for all available features.

**Note**: The TUI (Text User Interface) is optional and requires the `tui` feature flag. The CLI, FlightSQL server, and HTTP server are always available.

### Running the apps

```sh
# Interactive TUI (requires `tui` feature)
dft

# CLI with direct query execution
dft -c "SELECT 1 + 2"

# CLI with file-based query
dft -f query.sql

# Benchmark a query (with stats)
dft -c "SELECT * FROM my_table" --bench

# Concurrent benchmark (measures throughput under load)
dft -c "SELECT * FROM my_table" --bench --concurrent

# Save benchmark results to CSV
dft -c "SELECT * FROM my_table" --bench --save results.csv

# Start FlightSQL Server (requires `flightsql` feature)
dft serve-flightsql

# Start HTTP Server (requires `http` feature)
dft serve-http

# Generate TPC-H data in the configured DB path
dft generate-tpch
```

### Benchmarking

`dft` includes built-in benchmarking to measure query performance with detailed timing breakdowns:

```sh
# Serial benchmark (default) - measures query performance in isolation
dft -c "SELECT * FROM my_table" --bench

# Concurrent benchmark - measures throughput under load
dft -c "SELECT * FROM my_table" --bench --concurrent

# Custom iteration count
dft -c "SELECT * FROM my_table" --bench -n 100

# Save results to CSV for analysis
dft -c "SELECT * FROM my_table" --bench --save results.csv

# Compare serial vs concurrent performance
dft -c "SELECT * FROM my_table" --bench --save results.csv
dft -c "SELECT * FROM my_table" --bench --concurrent --save results.csv --append
```

**Benchmark Output:**
- Timing breakdown by phase: logical planning, physical planning, execution
- Statistics: min, max, mean, median for each phase
- Row counts validation across all runs
- CSV export with `concurrency_mode` column for result comparison

**Serial vs Concurrent:**
- **Serial**: Pure query execution time without contention (baseline performance)
- **Concurrent**: Throughput measurement with parallel execution (reveals bottlenecks and contention)
- Concurrent mode uses adaptive concurrency: `min(iterations, CPU cores)`

### Setting Up Tables with DDL

`dft` can automatically load table definitions at startup, giving you a persistent "database-like" experience.

#### Using DDL Files

1. Create a DDL file (default: `~/.config/dft/ddl.sql`)
2. Add your table and view definitions:

```sql
-- S3 data source (requires s3 feature)
CREATE EXTERNAL TABLE users 
STORED AS NDJSON 
LOCATION 's3://bucket/users';

-- Parquet files
CREATE EXTERNAL TABLE transactions 
STORED AS PARQUET 
LOCATION 's3://bucket/transactions';

-- Local files
CREATE EXTERNAL TABLE listings 
STORED AS PARQUET 
LOCATION 'file://folder/listings';

-- Create views from tables
CREATE VIEW users_listings AS 
SELECT * FROM users 
LEFT JOIN listings USING (user_id);

-- Delta Lake table (requires deltalake feature)
CREATE EXTERNAL TABLE delta_table 
STORED AS DELTATABLE 
LOCATION 's3://bucket/delta_table';
```

#### Loading DDL

- **TUI** (requires `tui` feature): DDL is automatically loaded at startup
- **CLI**: Add `--run-ddl` flag to execute DDL before your query
- **Custom Path**: Configure a custom DDL path in your config file
  ```toml
  [execution]
  ddl_path = "/path/to/my/ddl.sql"
  ```

## Quick Reference

| Feature | Documentation |
|---------|---------------|
| **Core Features** | [Features Guide](docs/features.md) |
| **Database** | [Database Guide](docs/db.md) |
| **TUI Interface** | [TUI Guide](docs/tui.md) |
| **CLI Usage** | [CLI Guide](docs/cli.md) |
| **FlightSQL Server** | [FlightSQL Guide](docs/flightsql_server.md) |
| **HTTP Server** | [HTTP Guide](docs/http_server.md) |
| **Configuration Options** | [Config Reference](docs/config.md) |

# CLI Guide

`dft` also provides a scriptable Command Line Interface (CLI) for executing queries directly from files or the command line. It can connect to the same DataFusion engine behind the TUI, or optionally run your queries against a FlightSQL server.

## Basic Usage

```sh
# Run queries from a file
dft -f query.sql

# Run a query directly on the command line
dft -c "SELECT 1+2"
```

## FlightSQL Mode

Use `--flightsql` or `-q` to run commands or files against a FlightSQL server (instead of the default local SessionContext). You can override the default host for that single command with --host

```sh
dft -f query.sql --flightsql --host "http://127.0.0.1:50052"
```

### FlightSQL Commands

Additioanlly, you can use the `flightsql` subcommand to interact with different methods [exposed by a FlightSQL server](https://arrow.apache.org/docs/format/FlightSql.html) directly:

```sh
# Execute a SQL query
dft flightsql statement-query --sql "SELECT * FROM table"

# List all catalogs
dft flightsql get-catalogs

# List schemas in a catalog
dft flightsql get-db-schemas --catalog mycatalog
dft flightsql get-db-schemas --db-schema-filter-pattern "my%"

# List tables in a schema
dft flightsql get-tables --catalog mycatalog --db-schema-filter-pattern myschema
dft flightsql get-tables --table-name-filter-pattern "table" --table-types VIEW

# Get supported table types
dft flightsql get-table-types

# Get SQL capabilities and server information
dft flightsql get-sql-info
dft flightsql get-sql-info --info 1 --info 2  # Query specific info IDs

# Get data type information (XDBC/ODBC type metadata)
dft flightsql get-xdbc-type-info
dft flightsql get-xdbc-type-info --data-type 4  # Filter by specific SQL data type
```

## Headers

Additional HTTP headers can be attached to FlightSQL connections using `--header` or `--headers-file`. These are useful for passing authentication tokens, tenant identifiers, or any other metadata required by the server.

### Single headers (`--header`)

Pass one or more `Name: Value` pairs directly on the command line. The flag can be repeated:

```sh
dft -c "SELECT 1" --flightsql --header "x-api-key: secret"
dft -c "SELECT 1" --flightsql --header "x-tenant: acme" --header "x-api-key: secret"
```

### Headers file (`--headers-file`)

For many headers, or to avoid exposing secrets in shell history, store them in a file and point `--headers-file` at it:

```sh
dft -c "SELECT 1" --flightsql --headers-file ~/.config/dft/headers.txt
```

The file supports three formats (which can be mixed in the same file):

```
# Simple format
x-api-key: secret123
x-tenant: acme

# Curl config format
header = x-request-id: abc-123

# Curl -H flag format
-H "authorization: Bearer mytoken"
-H 'database: production'
```

Lines beginning with `#` and blank lines are ignored.

The headers file path can also be set in the config so it is always picked up without needing the flag each time:

```toml
[flightsql_client]
headers_file = "/home/user/.config/dft/headers.txt"
```

### Precedence

When headers are specified in multiple places they are merged in this order (later sources win):

1. Config file (`flightsql_client.headers`)
2. Headers file (config `headers_file` or `--headers-file`)
3. `--header` flags on the command line

## Auth

Basic Auth or Bearer Token can be set in your config, which is used by the client:

```toml
[flightsql_client.auth]
bearer_token = "MyToken"
basic_auth.username = "User"
basic_auth.password = "Pass"
```

## Benchmark Queries

You can benchmark queries by adding the `--bench` parameter. This will run the query a configurable number of times and output a breakdown of the query's execution time with summary statistics for each component (logical planning, physical planning, execution time, and total time).

### Benchmark Modes

**Serial Benchmark (default):**
Measures query performance in isolation, running iterations one after another. This shows the pure query execution time without any contention or resource sharing overhead.

**Concurrent Benchmark (`--concurrent`):**
Measures query performance under load by running iterations in parallel. This reveals:
- Throughput (queries per second) with multiple concurrent clients
- Resource contention and bottlenecks
- Performance degradation under concurrent load

Concurrent mode uses adaptive concurrency: `min(iterations, CPU cores)` to avoid overwhelming the system.

### Options

- **`--bench`**: Enable benchmarking mode
- **`--concurrent`**: Run iterations in parallel (for concurrent benchmarking)
- **`-n <count>`**: Number of iterations (default: 10, configured in config file)
- **`--run-before <query>`**: Run a setup query before benchmarking (useful for cache warming)
- **`--save <file>`**: Save results to CSV file
- **`--append`**: Append to existing results file instead of overwriting

### Examples

```sh
# Serial benchmark (default)
dft -c "SELECT * FROM my_table" --bench

# Concurrent benchmark
dft -c "SELECT * FROM my_table" --bench --concurrent

# Custom iteration count
dft -c "SELECT ..." --bench -n 100

# Concurrent with custom iterations
dft -c "SELECT ..." --bench -n 100 --concurrent

# Save benchmark results to CSV
dft -c "SELECT ..." --bench --save results.csv

# Append results (compare serial vs concurrent)
dft -c "SELECT ..." --bench --save results.csv
dft -c "SELECT ..." --bench --concurrent --save results.csv --append

# Run a setup query before benchmarking
dft -c "SELECT ..." --bench --run-before="CREATE TEMP TABLE my_temp AS SELECT ..."

# FlightSQL benchmark (concurrent)
dft -c "SELECT ..." --bench --concurrent --flightsql
```

### Output

Benchmark output includes:
- **Mode**: `serial` or `concurrent(N)` where N is the concurrency level
- **Timing breakdown**: Logical planning, physical planning, execution (min/max/mean/median)
- **Row counts**: Validation that all runs returned the same number of rows
- **CSV format**: Results include a `concurrency_mode` column for comparison

**Note**: Concurrent benchmarks typically show higher mean/median times due to resource contention - this is expected and reveals how the system performs under load.

## Analyze Queries

The output from `EXPLAIN ANALYZE` provides a wealth of information on a queries execution - however, the amount of information and connecting the dots can be difficult and manual.  Further, there is detail in the `MetricSet`'s of the underlying `ExecutionPlan`'s that is lost in the output.

To help with this the `--analyze` flag can used to generate a summary of the underlying `ExecutionPlan` `MetricSet`s.  The summary presents the information in a way that is hopefully easier to understand and easier to draw conclusions on a query's performance.

This feature is still in it's early stages and is expected to evolve.  Once it has gone through enough real world testing and it has been confirmed the metrics make sense documentation will be added on the exact calculations - until then the source will need to be inspected to see the calculations.

```sh
dft -c "SELECT ..." --analyze
```

## Generate TPC-H Data

Generate TPC-H data into your configured DB path

```sh
dft generate-tpch
```

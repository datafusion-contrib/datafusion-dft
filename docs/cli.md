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

**Important**: The analyze feature only supports a single SQL statement. If you provide multiple statements (e.g., separated by semicolons) or multiple files/commands, an error will be returned.

This feature is still in it's early stages and is expected to evolve.  Once it has gone through enough real world testing and it has been confirmed the metrics make sense documentation will be added on the exact calculations - until then the source will need to be inspected to see the calculations.

### Local Analyze

```sh
# Analyze a query locally
dft -c "SELECT * FROM table WHERE id > 100" --analyze

# Analyze from a file
dft -f query.sql --analyze
```

### FlightSQL Analyze

The `--analyze` flag also works with FlightSQL execution, providing identical output to local analyze:

```sh
# Analyze query on FlightSQL server
dft -c "SELECT * FROM table WHERE id > 100" --analyze --flightsql

# Analyze from a file via FlightSQL
dft -f query.sql --analyze --flightsql
```

**Requirements:**
- The Arrow Flight service must support the `"analyze_query"` custom action
- See the [Arrow Flight Analyze Protocol Specification](arrow_flight_analyze_protocol.md) for implementation details
- Servers without analyze support will return an "unimplemented" error

**How it works:**
1. Client sends a `do_action("analyze_query")` request with the SQL query
2. Server executes the query with metrics collection enabled
3. Server serializes execution statistics to Arrow IPC format (two batches: queries + metrics)
4. Client deserializes and reconstructs the full execution statistics
5. Output is formatted identically to local analyze

### Analyze Output

Both local and FlightSQL analyze produce identical output including:

- **Execution Summary**: Output rows/bytes, batch counts, selectivity ratios
- **Timing Breakdown**: Parsing, logical planning, physical planning, execution, total time
- **I/O Statistics**: Bytes scanned, file opening/scanning times
- **Parquet Metrics** (when applicable):
  - Row group pruning effectiveness (statistics, bloom filters, page index)
  - Per-row-group timing
- **Compute Statistics**: Per-operator elapsed compute time by partition
  - Breakdown by operator category (filter, sort, projection, join, aggregate)
  - Min/median/mean/max timing per operator

### Raw Metrics Mode

For debugging or custom analysis, use `--analyze-raw` to print the raw metrics table without formatting:

```sh
# Local raw metrics
dft -c "SELECT ..." --analyze-raw

# FlightSQL raw metrics
dft -c "SELECT ..." --analyze-raw --flightsql
```

This outputs two Arrow tables:
1. **Queries table**: Contains the query text
2. **Metrics table**: Flat table with columns (metric_name, value, value_type, operator_name, partition_id, operator_category)

## Generate TPC-H Data

Generate TPC-H data into your configured DB path

```sh
dft generate-tpch
```

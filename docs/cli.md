# CLI Guide

`dft` also provides a scriptable Command Line Interface (CLI) for executing queries directly from files or the command line. It can connect to the same DataFusion engine behind the TUI, or optionally run your queries against a FlightSQL server.

## Basic Usage

```sh
# Run queries from a file
dft -f query.sql

# Run a query directly on the command line
dft -c "SELECT 1+2"
```

## Persistent SQL prompt

Run `dft --repl` to start the inline SQL prompt.
Without the `tui` feature, `dft` starts this prompt automatically.
With the `tui` feature, `dft` continues to start the full TUI.

```sh
# Local DataFusion session
dft --repl

# FlightSQL session (requires the flightsql feature)
dft --repl -q --host http://localhost:50051
```

End each statement with `;` and press Enter to execute it.
Enter continues incomplete statements on a new line.
Alt+Enter inserts a new line without execution.
Pasted SQL remains editable until Enter.
Multiple complete statements execute in order.
An error stops the current submission and returns control to the prompt.
Any unfinished suffix remains in the editor.

Results appear above the editor in terminal scrollback.
Local tables, views, and session settings remain available between statements.
The prompt uses the CLI execution configuration, extensions, and database registrations.
`--run-ddl` executes the configured local initialization DDL once at startup.
`--json` selects line-delimited JSON results.
`--time` suppresses result rows and shows execution time and row counts.

| Control | Action |
| --- | --- |
| `\?` | Show help |
| `\q` | Exit |
| Ctrl+D with empty input | Exit |
| Ctrl+C | Clear input or stop execution |
| Up at the first editor line | Previous SQL submission |
| Down at the last editor line | Next submission or the original draft |
| Ctrl+R | Search previous submissions |
| Ctrl+R during search | Select the next older match |
| Enter during search | Copy the match into the editor |
| Escape during search | Close search |

History stores up to 1,000 submissions in `repl-history.toml` under the dft data directory.
The default directory is `~/.config/dft`.
`DATAFUSION_DFT_DATA` overrides this directory.
History contains SQL text, including literals.
History read or write errors produce a message and do not stop the session.
Concurrent sessions keep independent histories; the last save replaces the file.

The prompt requires terminal input and output.
Use `-c` or `-f` for scripts and redirected output.
The prompt does not accept batch export, concatenation, benchmarking, or analysis options.

### FlightSQL connections

The prompt reuses one FlightSQL client and channel by default.
Configure a new client and channel for each SQL statement when required:

```toml
[flightsql_client]
connection_mode = "per_statement" # Default: "persistent"
```

The existing configuration override also works:

```sh
dft --repl -q --set flightsql_client.connection_mode=per_statement
```

This setting applies to the inline prompt.
Both modes use the configured authentication, headers, and message limits.
Header precedence remains configuration, headers file, then command-line headers.
Each statement retains its connection until all result endpoints finish.
A persistent channel can reconnect its transport.
Server session state depends on the server implementation.
Failed statements are never automatically repeated.

SQL uses FlightSQL statement-query requests by default.
For servers that require statement-update requests for writes, use `\update`:

```sql
\update INSERT INTO events VALUES (1);
```

This command reports the affected row count.
It also supports DDL when the server accepts DDL through statement-update requests.
Ordinary SQL continues to support servers that execute DDL through statement-query requests.
Ctrl+C stops result consumption and drops the active execution future.
When query metadata is available, it also requests server cancellation.
The client tries `CancelFlightInfo`, then the older `CancelQuery` action when necessary.
Cancellation requests have a one-second timeout.
Server cancellation support determines whether remote work stops.

## Saving Results and Format Options

Use `--output` (or `-o`) to save the result of one command or SQL file. The
output format is inferred from the `.csv`, `.json`, `.parquet`, or (when
enabled) `.vortex` suffix.

Repeat `--format-option KEY=VALUE` to configure the selected writer. Option
names follow DataFusion's file format options; the optional `format.` prefix is
accepted. For example, Parquet options can be global or column-specific:

```sh
dft -c "SELECT id, name FROM users" -o users.parquet \
  --format-option compression=zstd\(5\) \
  --format-option encoding::id=delta_binary_packed \
  --format-option encoding::name=delta_length_byte_array
```

CSV writer options use the same mechanism:

```sh
dft -c "SELECT id, name FROM users" -o users.csv \
  --format-option delimiter='|' \
  --format-option has_header=false
```

Vortex writer options use a different set of keys. Column-specific keys use
`KEY::COLUMN=VALUE` and apply to top-level columns only:

```sh
dft -c "SELECT id, name FROM users" -o users.vortex \
  --format-option encoding::id=bitpacking \
  --format-option encoding::name=vortex.string.fsst \
  --format-option dictionary_enabled::name=false \
  --format-option statistics_enabled::id=false \
  --format-option row_block_size=4096 \
  --format-option data_block_target_bytes=none \
  --format-option compressor=compact
```

An encoding name can be a full scheme name (`vortex.string.fsst`), a
kind-qualified name (`string.fsst`), or a short name (`fsst`) when the column
type disambiguates it. Zone maps are written by default, including for columns
with an encoding override. Vortex selects encodings adaptively; `encoding`
restricts the candidate schemes for that column. Use
`dictionary_enabled::COLUMN=false` to skip dictionary encoding when you want a
single encoding.

`statistics_enabled=false` disables zone maps. It accepts a global value or a
`statistics_enabled::COLUMN` override. Zone maps are what DataFusion uses to
prune files and row blocks, so disabling them makes files smaller and writes
faster, and queries scan more rows.

Invalid options fail with a configuration error. Compressed CSV and JSON output
is not currently supported.

To list the available keys, defaults, and descriptions for a format:

```sh
dft --print-format-options parquet
dft --print-format-options csv
dft --print-format-options json
dft --print-format-options vortex
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

### Response headers (`--print-headers`)

To inspect the metadata returned by the FlightSQL server on each `DoGet` response, pass `--print-headers`. The response headers are printed before the query results and the response trailers (if the server sends any) after them:

```sh
dft -c "SELECT 1" --flightsql --print-headers
dft --print-headers flightsql statement-query --sql "SELECT 1"
```

```text
Response headers:
  content-type: application/grpc
  date: ...
+----------+
| Int64(1) |
+----------+
| 1        |
+----------+
Response trailers:
  grpc-status: 0
```

### Headers only (`--headers-only`)

For large results, `--headers-only` skips printing the results entirely and instead prints the headers, a summary of the response schema and total record count, and the trailers. It implies `--print-headers` and cannot be combined with `--output`, `--json`, or `--bench`:

```sh
dft -c "SELECT * FROM huge_table" --flightsql --headers-only
```

```text
Response headers:
  content-type: application/grpc
Schema:
  x: Int64
  y: Utf8
Records: 1234567
Response trailers:
  grpc-status: 0
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

This feature is still in it's early stages and is expected to evolve.  Once it has gone through enough real world testing and it has been confirmed the metrics make sense documentation will be added on the exact calculations - until then the source will need to be inspected to see the calculations.

```sh
dft -c "SELECT ..." --analyze
```

## Generate TPC-H Data

Generate TPC-H data into your configured DB path

```sh
dft generate-tpch
```

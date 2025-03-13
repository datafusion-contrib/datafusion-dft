# dft CLI

`dft` also provides a scriptable Command Line Interface (CLI) for executing queries directly from files or the command line. It can connect to the same DataFusion engine behind the TUI, or optionally run your queries against a FlightSQL server.

## Basic Usage

```sh
# Run queries from a file
dft -f query.sql

# Run a query directly on the command line
dft -c "SELECT 1+2"
```

## FlightSQL Mode

Use --flightsql to run against a FlightSQL server (instead of the default local SessionContext). You can override the default host for that single command with --host

```sh
dft -f query.sql --flightsql --host "http://127.0.0.1:50052"
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

You can benchmark queries by adding the `--bench` parameter.  This will run the query a configurable number of times and output a breakdown of the queries execution time with summary statistics for each component of the query (logical planning, physical planning, execution time, and total time).

Optionally you can use the `--run-before` param to run a query before the benchmark is run.  This is useful in cases where you want to hit a temp table or write a file to disk that your benchmark query will use.

To save benchmark results to a file use the `--save` parameter with a file path.  Further, you can use the `--append` parameter to append to the file instead of overwriting it.

The number of benchmark iterations is defined in your configuration (default is 10) and can be configured per benchmark run with `-n` parameter.


```sh
dft -c "SELECT * FROM my_table" --bench

# Run a configurable number of benchmark iterations
dft -c "SELECT ..." --bench -n 5

# Save benchmark results to a file
dft -c "SELECT ..." --bench --save results.csv

# Append benchmark results to existing file
dft -c "SELECT ..." --bench --save results.csv --append

# Run a setup query prior to running benchmark.  This can be useful to quickly iterate on various paramters
dft -c "SELECT ..." --bench --run-before="CREATE TEMP TABLE my_temp AS SELECT ..."
```

## Analyze Queries

The output from `EXPLAIN ANALYZE` provides a wealth of information on a queries execution - however, the amount of information and connecting the dots can be difficult and manual.  Further, there is detail in the `MetricSet`'s of the underlying `ExecutionPlan`'s that is lost in the output.

To help with this the `--analyze` flag can used to generate a summary of the underlying `ExecutionPlan` `MetricSet`s.  The summary presents the information in a way that is hopefully easier to understand and easier to draw conclusions on a query's performance.

This feature is still in it's early stages and is expected to evolve.  Once it has gone through enough real world testing and it has been confirmed the metrics make sense documentation will be added on the exact calculations - until then the source will need to be inspected to see the calculations.

```sh
dft -c "SELECT ..." --analyze
```

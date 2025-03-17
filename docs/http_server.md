# HTTP Server Guide

`dft` also provides an HTTP server that exposes a simple REST interface to DataFusion.

---

## Starting the Server

```sh
dft serve-http
```

## Endpoints

The current endpoints provided are:

`/sql` => Make POST requests with body `{ sql: string, flightsql?: bool }`
`/catalog` => View the catalog for the database, optionally accepts a `flightsql` query param
`/table/{CATALOG}/{SCHEMA}/{TABLE}` => Fetch records from the provided table, Optionally accepts a `flightsql` query param

## Auth

Require basic or bearer authentication to make requests.

```toml
[flightsql_server.auth]
bearer_token = "MyToken"
basic_auth.username = "User"
basic_auth.password = "Pass"
```

## Metrics

Prometheus metrics are automatically published.


```toml
[flightsql_server]
server_metrics_port = "0.0.0.0:9000"
```

## Benchmarking

Something useful that comes from having an HTTP server is that we can leverage an extensive ecosystem of the HTTP load generation tools to benchmark our performance.

To get started with benchmarking you will first want to install the necessary tools.

```sh
# Installs `oha` which is a CLI for HTTP benchmarking
just install-tools
```

Then you will want to start the `dft` HTTP server in one terminal window.

```sh
just serve-http
```

Finally, you can run a simple benchmark to make sure all the flows are working.

```sh
# Benchmarks running `SELECT 1`
just bench-http-basic
```

You can add your own benchmark files in the `bench/url_files` directory to cater the benchmarks to your needs.  An example file is provided that you can use as reference.

```sh
just bench-http-custom example_custom_bench.txt
```


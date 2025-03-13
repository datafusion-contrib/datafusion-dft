# dft HTTP Server

`dft` also provides an HTTP server that exposes a simple REST interface to DataFusion.

---

## Starting the Server

```sh
dft serve-http
```

## Endpoints

The current endpoints provided are:

`/sql` => Make POST requests with a SQL in the body
`/catalog` => View the catalog for the database
`/table/{CATALOG}/{SCHEMA}/{TABLE}` => Fetch records from the provided table.

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

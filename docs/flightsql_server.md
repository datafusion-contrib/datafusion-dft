# FlightSQL Server Guide

`dft` includes an **experimental FlightSQL server** that you can run to provide programmatic SQL access to DataFusion.

---

## Starting the Server

```sh
dft serve-flightsql
```

## Endpoints

TODO List endpoints

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

# FlightSQL Server Guide

`dft` includes a FlightSQL server that provides programmatic SQL access to DataFusion through a standards-compliant interface. This enables you to connect using language-specific FlightSQL clients and build applications on top of DataFusion.

## Starting the Server

```sh
# Start with default settings
dft serve-flightsql

# Start with your custom DDL loaded
dft serve-flightsql --run-ddl
```

## Supported Operations

The server implements the FlightSQL protocol, providing:

- SQL query execution
- Schema fetching (TODO)
- Prepared statements (TODO)
- Catalog browsing (TODO)

## Client Connections (TODO - Test this)

You can connect to the server using any FlightSQL-compatible client:

```python
TODO with https://docs.influxdata.com/influxdb3/clustered/reference/client-libraries/flight/python-flightsql-dbapi/
```

## Authentication

Configure Basic Auth or Bearer Token authentication:

```toml
[flightsql_server.auth]
# Option 1: Bearer token auth
bearer_token = "MyToken"

# Option 2: Basic auth
basic_auth.username = "User" 
basic_auth.password = "Pass"
```

## Metrics and Monitoring

Prometheus metrics are automatically published to help you monitor server performance:

```toml
[flightsql_server]
# Configure metrics port
server_metrics_port = "0.0.0.0:9000"
```

Available metrics include:
- Query execution time
- Active connections
- Errors by type
- Memory usage

## Configuration

Set connection and execution parameters in your config file:

```toml
[flightsql_server]
# Server address
connection_url = "http://localhost:50051"

# Configure execution parameters
[flightsql_server.execution.datafusion]
target_partitions = 8
```

See the [Config Reference](config.md) for all available options.

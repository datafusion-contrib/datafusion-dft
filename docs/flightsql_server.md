# FlightSQL Server Guide

`dft` includes a FlightSQL server that provides programmatic SQL access to DataFusion through a standards-compliant interface. This enables you to connect using language-specific FlightSQL clients and build applications on top of DataFusion.

## Starting the Server

```sh
# Start with default settings
dft serve-flightsql

# Start with custom port
dft serve-flightsql --port 8080

# Start with your custom DDL loaded
dft serve-flightsql --run-ddl
```

## Supported Operations

The server implements the FlightSQL protocol, providing:

- SQL query execution
- Schema fetching
- Prepared statements
- Catalog browsing
- Metadata retrieval

## Client Connections

You can connect to the server using any FlightSQL-compatible client:

```python
# Python example using pyarrow
import pyarrow.flight
from pyarrow.flight import FlightClient
import pyarrow.flight.sql as flight_sql

# Connect to the server
client = flight_sql.FlightSQLClient(
    FlightClient("grpc://localhost:50051")
)

# Execute a query
info = client.execute("SELECT * FROM my_table")
reader = client.do_get(info.endpoints[0].ticket)

# Read the results
for batch in reader:
    print(batch.to_pandas())
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

# Batch size for result streaming
flightsql_server_batch_size = 8092

# Configure execution parameters
[flightsql_server.execution]
target_partitions = 8
```

See the [Config Reference](config.md) for all available options.

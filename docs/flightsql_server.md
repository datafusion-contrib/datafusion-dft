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

### Core Query Execution
- **SQL query execution** - Execute SQL statements via `CommandStatementQuery`
- **Prepared statements** - Create, execute, and close prepared statements for improved performance
  - `ActionCreatePreparedStatement` - Parse and prepare SQL statements
  - `ActionClosePreparedStatement` - Release prepared statement resources
  - `CommandPreparedStatementQuery` - Execute prepared statements

### Metadata Discovery
- **Catalog browsing** - Discover database structure and metadata
  - `CommandGetCatalogs` - List available catalogs
  - `CommandGetDbSchemas` - List schemas with optional filtering
  - `CommandGetTables` - List tables with filtering by catalog, schema, name pattern, and type
  - `CommandGetTableTypes` - Get supported table types (TABLE, VIEW, etc.)

### Server Capabilities
- **SQL information** - Query server capabilities and version information via `CommandGetSqlInfo`
- **Type metadata** - Get XDBC/ODBC type information via `CommandGetXdbcTypeInfo` for understanding supported data types

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
# Configure metrics addr
server_metrics_addr = "0.0.0.0:9000"
```

Available metrics include:
- Query execution time (by endpoint)
  - `get_flight_info_latency_ms` - Flight info request latency
  - `do_get_fallback_latency_ms` - Data fetch latency
  - `get_flight_info_table_types_latency_ms` - Table types metadata latency
  - `get_flight_info_sql_info_latency_ms` - SQL info metadata latency
  - `get_flight_info_xdbc_type_info_latency_ms` - Type info metadata latency
  - `do_action_create_prepared_statement_latency_ms` - Prepared statement creation latency
  - `do_action_close_prepared_statement_latency_ms` - Prepared statement cleanup latency
  - `get_flight_info_prepared_statement_latency_ms` - Prepared statement flight info latency
  - `do_get_prepared_statement_latency_ms` - Prepared statement execution latency
- Active prepared statements (`prepared_statements_active` gauge)
- Request counts by endpoint
- Observability request details (when enabled) stored in `dft.observability_requests` table

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

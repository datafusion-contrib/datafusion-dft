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

### Custom Actions
- **Query Analysis** - Get detailed execution metrics via custom `"analyze_query"` action
  - See [Arrow Flight Analyze Protocol](arrow_flight_analyze_protocol.md) for the complete specification

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
  - `do_action_analyze_query_latency_ms` - Analyze query action latency
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

## Query Analysis Support

The `dft` FlightSQL server implements the [Arrow Flight Analyze Protocol](arrow_flight_analyze_protocol.md), which provides detailed query execution metrics through a custom action.

### Quick Start

The analyze protocol is automatically enabled when you start the FlightSQL server:

```sh
dft serve-flightsql
```

Clients can then use the `--analyze` flag:

```sh
# Analyze query via FlightSQL
dft -c "SELECT * FROM table WHERE id > 100" --analyze --flightsql

# View raw metrics table
dft -c "SELECT ..." --analyze-raw --flightsql
```

### What's Provided

The server collects and returns:

- **Execution timing**: Parsing, planning, execution, and total time
- **I/O statistics**: Bytes scanned, file operations, format-specific metrics
- **Parquet metrics**: Row group pruning, bloom filters, page index effectiveness
- **Compute breakdown**: Per-operator, per-partition CPU time by category (filter, sort, projection, join, aggregate)

### Implementation Details

The `dft` server implementation:

1. Handles the `"analyze_query"` action in `do_action_fallback()`
2. Executes queries using `ExecutionContext::analyze_query()`
3. Collects metrics from DataFusion execution plan `MetricSet`s
4. Serializes to two Arrow batches (queries + metrics) following the protocol spec
5. Returns FlightData stream with metrics encoded as rows

### For Other Implementers

If you're implementing an Arrow Flight service and want to support the analyze protocol, see the complete [Arrow Flight Analyze Protocol Specification](arrow_flight_analyze_protocol.md).

The protocol is designed to be:
- Implementation-agnostic (works with any query engine)
- Format-agnostic (supports Parquet, CSV, JSON, ORC, custom formats)
- Extensible (servers can add custom metrics)
- Language-agnostic (simple flat table format)

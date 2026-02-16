# Arrow Flight Analyze Protocol Specification

**Version**: 1.0
**Status**: Experimental

This document specifies a protocol extension for Apache Arrow Flight services to provide detailed query execution metrics. The protocol is designed to be implementation-agnostic and can be adopted by any Arrow Flight service.

## Overview

The Arrow Flight Analyze Protocol enables clients to retrieve detailed execution metrics for queries through a custom Arrow Flight action. This provides:

- Query execution timing breakdown (parsing, planning, execution)
- I/O statistics (bytes scanned, file operations)
- Format-specific metrics (Parquet pruning, CSV parsing, etc.)
- Per-operator compute time by partition
- Execution plan hierarchy for reconstructing query plan structure
- Extensible metric model for custom execution plan nodes

### Protocol Scope

This protocol is an **Apache Arrow Flight** extension, not specific to Flight SQL. While it naturally pairs with Flight SQL for SQL query analysis, any Arrow Flight service can implement the `analyze_query` action to provide execution metrics.

**Compatible Services**:
- Flight SQL servers (SQL queries)
- DataFrame API servers (DataFrame expressions)
- Custom query engines using Arrow Flight for transport

The examples in this specification use SQL for illustration, but the protocol works with any query representation that the Flight service supports.

## Action Specification

### Action Type

**Action Name**: `"analyze_query"`

**Purpose**: Execute a SQL query with metrics collection enabled and return detailed execution statistics.

### Request Format

**Request Body**: JSON-encoded query request structure

The request body should be a JSON object with the following structure:

```json
{
  "sql": "SELECT * FROM table WHERE id > 100"
}
```

**Current Fields**:
- `sql` (string, required): The SQL query to analyze. Must contain exactly one SQL statement. Multiple statements (e.g., separated by semicolons) are not supported and will result in an error.

**Future Extensibility**:
The protocol is designed to be extensible. Additional query representation fields may be supported in the future:
- `substrait` (bytes): Substrait query plan (binary or JSON)
- `logical_plan` (string): Serialized logical plan
- `physical_plan` (string): Serialized physical plan

Servers should ignore unknown fields and clients should only send one query representation field at a time.

**Example**:
```rust
use serde_json::json;

let request_body = json!({
    "sql": "SELECT * FROM table WHERE id > 100"
});

Action {
    r#type: "analyze_query".to_string(),
    body: serde_json::to_vec(&request_body)?.into()
}
```

**Request Encoding**: The JSON object should be serialized to UTF-8 bytes in the `Action.body` field.

### Response Format

The response is a stream of `arrow_flight::Result` messages. Each `Result.body` contains serialized `FlightData` messages.

**Note**: The client is responsible for correlating queries with metrics by retaining the original query string from the request. The server does not include the query in the response metadata.

#### Metrics Batch

**Purpose**: Single Arrow RecordBatch containing a flat table where each row represents a single metric

**Schema**:
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| metric_name | Utf8 | false | Namespaced metric name (e.g., "query.rows", "stage.parsing", "io.parquet.bytes_scanned") |
| value | UInt64 | false | Numeric value of the metric |
| value_type | Utf8 | false | Type of value: "duration_ns", "bytes", "count", or "ratio" |
| operator_name | Utf8 | true | Execution plan node name (e.g., "FilterExec", "ParquetExec") |
| partition_id | Int32 | true | Partition number for per-partition metrics |
| operator_category | Utf8 | true | Category: "filter", "sort", "projection", "join", "aggregate", "window", "distinct", "limit", "union", "io", "other" |
| operator_parent | Utf8 | true | Parent operator name in execution plan tree (NULL for root) |
| operator_index | Int32 | true | Child index under parent (0-based, NULL for root or query-level metrics) |

**Cardinality**: Variable (one row per metric)

### Execution Plan Hierarchy

The `operator_parent` and `operator_index` fields enable reconstruction of the execution plan DAG:

- **operator_parent = NULL**: Indicates the root operator of the execution plan
- **operator_index**: Position among siblings under the same parent (0-based)
- **Query-level metrics** (where `operator_name = NULL`) have both `operator_parent` and `operator_index` set to NULL

**Example**: For a plan like `ProjectionExec -> FilterExec -> ParquetExec` (where ProjectionExec is the root/final output):
- ProjectionExec: `operator_parent = NULL`, `operator_index = NULL` (root - produces final output)
- FilterExec: `operator_parent = "ProjectionExec"`, `operator_index = 0` (child of ProjectionExec)
- ParquetExec: `operator_parent = "FilterExec"`, `operator_index = 0` (child of FilterExec, leaf node)

## Metric Namespaces

Metric names use a hierarchical namespace structure to prevent collisions and provide clear semantic grouping:

**Format**: `{namespace}.{metric_name}`

**Standard Namespaces**:
- `query.*` - Query-level metrics (rows, batches, bytes)
- `stage.*` - Execution stage durations (parsing, logical_planning, physical_planning, execution, total)
- `io.parquet.*` - Parquet-specific I/O metrics (bytes_scanned, time_opening, time_scanning, output_rows, rg_pruned, bloom_pruned, etc.)
- `io.csv.*` - CSV-specific I/O metrics (bytes_scanned, time_opening, time_scanning, output_rows, rows_parsed, parse_errors)
- `io.json.*` - JSON-specific I/O metrics (bytes_scanned, time_opening, time_scanning, output_rows, invalid_rows, parse_errors)
- `io.orc.*` - ORC-specific I/O metrics (future: bytes_scanned, stripe_pruned, etc.)
- `io.arrow.*` - Arrow IPC-specific I/O metrics (future: dictionary_hits, etc.)
- `compute.*` - Compute metrics (elapsed_compute with operator breakdown)
- `index.*` - Index-related metrics (future: index_hits, index_scans)
- `distributed.*` - Distributed execution metrics (future: bytes_sent, rpc_calls)

**Important**: There is no generic `io.*` namespace. Each file format reports its own complete set of I/O metrics under its specific namespace (e.g., `io.parquet.*`, `io.csv.*`). This prevents mixing aggregated and raw data.

## Standard Metrics

### Query-Level Metrics

These metrics have `operator_name = NULL`, `partition_id = NULL`, `operator_category = NULL`, `operator_parent = NULL`, `operator_index = NULL`:

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `query.rows` | count | Total number of output rows |
| `query.batches` | count | Total number of output batches |
| `query.bytes` | bytes | Total output size in bytes |

### Duration Metrics

Timing breakdown for query execution phases. All have `operator_name = NULL`, `partition_id = NULL`, `operator_category = NULL`, `operator_parent = NULL`, `operator_index = NULL`:

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `stage.parsing` | duration_ns | Query parsing time in nanoseconds |
| `stage.logical_planning` | duration_ns | Logical plan creation time |
| `stage.physical_planning` | duration_ns | Physical plan creation time |
| `stage.execution` | duration_ns | Query execution time |
| `stage.total` | duration_ns | Total query time (sum of all phases) |

### Format-Specific I/O Metrics

Each file format reports its own complete set of I/O metrics under its namespace. Common I/O metrics that each format should provide:

- `operator_name`: The scan operator name (e.g., "ParquetExec", "CsvExec", "JsonExec")
- `operator_category = "io"`
- `partition_id = NULL` (unless per-partition I/O stats are available)
- `operator_parent`: Parent operator in the execution plan
- `operator_index`: Child index under parent

**Common I/O Metrics** (each format provides these under its own namespace):

| Metric Pattern | Value Type | Description |
|----------------|------------|-------------|
| `io.{format}.bytes_scanned` | bytes | Total bytes read from storage |
| `io.{format}.time_opening` | duration_ns | Time spent opening files |
| `io.{format}.time_scanning` | duration_ns | Time spent reading/scanning data |
| `io.{format}.output_rows` | count | Number of rows produced by scan operator |

#### Parquet Metrics

All Parquet metrics use `operator_name = "ParquetExec"` and `operator_category = "io"`:

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `io.parquet.bytes_scanned` | bytes | Total bytes read from Parquet files |
| `io.parquet.time_opening` | duration_ns | Time spent opening Parquet files |
| `io.parquet.time_scanning` | duration_ns | Time spent reading/scanning Parquet data |
| `io.parquet.output_rows` | count | Number of rows produced by ParquetExec |
| `io.parquet.rg_pruned` | count | Row groups pruned by statistics |
| `io.parquet.rg_matched` | count | Row groups matched (not pruned) by statistics |
| `io.parquet.bloom_pruned` | count | Row groups pruned by bloom filters |
| `io.parquet.bloom_matched` | count | Row groups matched by bloom filters |
| `io.parquet.page_index_pruned` | count | Row groups pruned by page index |
| `io.parquet.page_index_matched` | count | Row groups matched by page index |

#### CSV Metrics

Metrics for CSV format (`operator_name = "CsvExec"`, `operator_category = "io"`):

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `io.csv.bytes_scanned` | bytes | Total bytes read from CSV files |
| `io.csv.time_opening` | duration_ns | Time spent opening CSV files |
| `io.csv.time_scanning` | duration_ns | Time spent reading/scanning CSV data |
| `io.csv.output_rows` | count | Number of rows produced by CsvExec |
| `io.csv.rows_parsed` | count | Number of CSV rows successfully parsed |
| `io.csv.parse_errors` | count | Number of rows with parse errors |

#### JSON Metrics

Metrics for JSON format (`operator_name = "JsonExec"`, `operator_category = "io"`):

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `io.json.bytes_scanned` | bytes | Total bytes read from JSON files |
| `io.json.time_opening` | duration_ns | Time spent opening JSON files |
| `io.json.time_scanning` | duration_ns | Time spent reading/scanning JSON data |
| `io.json.output_rows` | count | Number of rows produced by JsonExec |
| `io.json.invalid_rows` | count | Invalid JSON records skipped |
| `io.json.parse_errors` | count | JSON parsing errors encountered |

#### Other Formats

Additional formats can define their own metrics under their namespace:
- **ORC**: `io.orc.bytes_scanned`, `io.orc.stripe_pruned`, `io.orc.stripe_matched`
- **Arrow IPC**: `io.arrow.bytes_scanned`, `io.arrow.dictionary_hits`, `io.arrow.dictionary_misses`
- **Custom formats**: Use `io.{format}.*` namespace for custom format metrics

### Compute Metrics

Metrics for CPU-intensive operators. The `compute.elapsed_compute` metric appears in two forms:

#### Aggregate Compute Time

Total compute time across all operators:
- `metric_name = "compute.elapsed_compute"`
- `operator_name = NULL`
- `partition_id = NULL`
- `operator_category = NULL`
- `operator_parent = NULL`
- `operator_index = NULL`

#### Per-Operator, Per-Partition Compute Time

Detailed breakdown by operator and partition:
- `metric_name = "compute.elapsed_compute"`
- `operator_name`: Name of the operator (e.g., "FilterExec", "ProjectionExec")
- `partition_id`: Partition number (0-indexed)
- `operator_category`: One of:
  - `"filter"` - Filter operations
  - `"sort"` - Sort and sort-preserving merge operations
  - `"projection"` - Projection operations
  - `"join"` - Join operations (hash, nested loop, sort-merge, etc.)
  - `"aggregate"` - Aggregation operations
  - `"window"` - Window function operations
  - `"distinct"` - Distinct/deduplication operations
  - `"limit"` - Limit and TopK operations
  - `"union"` - Union operations
  - `"other"` - Other compute operations
- `operator_parent`: Parent operator in execution plan
- `operator_index`: Child index under parent

**Note**: Category assignment for ambiguous operators (e.g., hash aggregate with filtering) is implementation-defined.

## Example Response

The client retains the original query: `"SELECT * FROM table WHERE id > 100"`

### Metrics Batch
```
metric_name                    value         value_type   operator_name    partition_id  operator_category  operator_parent  operator_index
------------------------------ ------------- ------------ ---------------- ------------- ------------------ ---------------- --------------
query.rows                     1000          count        NULL             NULL          NULL               NULL             NULL
query.batches                  10            count        NULL             NULL          NULL               NULL             NULL
query.bytes                    50000         bytes        NULL             NULL          NULL               NULL             NULL
stage.parsing                  12000000      duration_ns  NULL             NULL          NULL               NULL             NULL
stage.logical_planning         45000000      duration_ns  NULL             NULL          NULL               NULL             NULL
stage.physical_planning        78000000      duration_ns  NULL             NULL          NULL               NULL             NULL
stage.execution                234000000     duration_ns  NULL             NULL          NULL               NULL             NULL
stage.total                    369000000     duration_ns  NULL             NULL          NULL               NULL             NULL
io.parquet.bytes_scanned       1000000       bytes        ParquetExec      NULL          io                 FilterExec       0
io.parquet.time_opening        50000000      duration_ns  ParquetExec      NULL          io                 FilterExec       0
io.parquet.time_scanning       150000000     duration_ns  ParquetExec      NULL          io                 FilterExec       0
io.parquet.output_rows         10000         count        ParquetExec      NULL          io                 FilterExec       0
io.parquet.rg_pruned           16            count        ParquetExec      NULL          io                 FilterExec       0
io.parquet.rg_matched          4             count        ParquetExec      NULL          io                 FilterExec       0
io.parquet.bloom_pruned        12            count        ParquetExec      NULL          io                 FilterExec       0
io.parquet.bloom_matched       8             count        ParquetExec      NULL          io                 FilterExec       0
compute.elapsed_compute        12345678      duration_ns  NULL             NULL          NULL               NULL             NULL
compute.elapsed_compute        1200          duration_ns  ProjectionExec   0             projection         NULL             NULL
compute.elapsed_compute        1100          duration_ns  ProjectionExec   1             projection         NULL             NULL
compute.elapsed_compute        1050          duration_ns  ProjectionExec   2             projection         NULL             NULL
compute.elapsed_compute        1000          duration_ns  ProjectionExec   3             projection         NULL             NULL
compute.elapsed_compute        1500          duration_ns  FilterExec       0             filter             ProjectionExec   0
compute.elapsed_compute        1400          duration_ns  FilterExec       1             filter             ProjectionExec   0
compute.elapsed_compute        1300          duration_ns  FilterExec       2             filter             ProjectionExec   0
compute.elapsed_compute        1200          duration_ns  FilterExec       3             filter             ProjectionExec   0
```

## Implementation Guide

### Server Implementation

To implement this protocol in an Arrow Flight service:

1. **Register Action Handler**
   - Implement `do_action` or `do_action_fallback` to recognize action type `"analyze_query"`

2. **Parse Request**
   - Decode `Action.body` as UTF-8 string to extract SQL query

3. **Execute Query**
   - Run query with execution plan metrics collection enabled
   - Ensure all operators record metrics in their `MetricSet`

4. **Collect Metrics**
   - Traverse execution plan to extract metrics from each operator
   - Convert metrics to rows following the schema above
   - Emit one row per metric value

5. **Build Response**
   - Create metrics RecordBatch with 8-field schema
   - Use namespaced metric names (e.g., `query.rows`, `stage.parsing`, `io.parquet.bytes_scanned`)
   - Populate `operator_parent` and `operator_index` fields for execution plan hierarchy
   - Encode as FlightData using `batches_to_flight_data()` or equivalent
   - Serialize each FlightData to bytes (protobuf encoding)
   - Wrap each serialized FlightData in `arrow_flight::Result { body: bytes }`
   - Stream Result messages to client

**Pseudo-code**:
```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct AnalyzeQueryRequest {
    sql: Option<String>,
    // Future fields:
    // substrait: Option<Vec<u8>>,
    // logical_plan: Option<String>,
    // physical_plan: Option<String>,
}

async fn do_action_fallback(&self, request: Request<Action>) -> Result<Response<Stream>> {
    let action = request.into_inner();

    if action.r#type == "analyze_query" {
        // 1. Parse JSON request
        let request: AnalyzeQueryRequest = serde_json::from_slice(&action.body)?;

        // 2. Extract SQL query (only supported format for now)
        let query = request.sql.ok_or_else(||
            Status::invalid_argument("sql field is required")
        )?;

        // 3. Execute with metrics
        let stats = self.analyze_query(&query).await?;

        // 4. Convert to metrics batch (8-field schema with namespaced metrics)
        let metrics_batch = stats.to_metrics_table()?;

        // 5. Encode as FlightData (no metadata needed)
        let flight_data = batches_to_flight_data(&metrics_batch.schema(), vec![metrics_batch])?;

        // 6. Serialize and wrap in Result messages
        // Note: Query is NOT included in response; clients must retain the original request
        let results: Vec<arrow_flight::Result> = flight_data
            .into_iter()
            .map(|fd| arrow_flight::Result { body: fd.encode_to_vec().into() })
            .collect();

        // 7. Return stream
        Ok(Response::new(stream::iter(results.into_iter().map(Ok))))
    } else {
        Err(Status::unimplemented("Unknown action"))
    }
}
```

### Client Implementation

To consume this protocol:

1. **Send Request**
   ```rust
   use serde_json::json;

   let query = "SELECT * FROM table WHERE id > 100";

   // Construct JSON request
   let request_body = json!({
       "sql": query
   });

   let action = Action {
       r#type: "analyze_query".to_string(),
       body: serde_json::to_vec(&request_body)?.into(),
   };
   let stream = client.do_action(action).await?;
   ```

2. **Receive Stream**
   - Collect `arrow_flight::Result` messages from stream

3. **Decode FlightData**
   ```rust
   let mut flight_data_vec = Vec::new();

   for result in result_messages {
       let flight_data = FlightData::decode(result.body.as_ref())?;
       flight_data_vec.push(flight_data);
   }
   ```

4. **Convert to RecordBatch**
   ```rust
   let batches = flight_data_to_batches(&flight_data_vec)?;
   let metrics_batch = batches[0].clone();
   ```

5. **Reconstruct Statistics**
   - Use the original query string retained by the client
   - Parse metrics batch (8-field schema) to reconstruct execution statistics
   - Extract `operator_parent` and `operator_index` to reconstruct execution plan hierarchy if needed

### Error Handling

**Server Behavior**:
Any error during request parsing, query execution, metrics collection, or response serialization results in complete failure. No partial metrics are returned.

**Error Codes**:
- `Status::unimplemented` - Server doesn't support analyze protocol
- `Status::invalid_argument` - Invalid SQL, malformed request, or multiple SQL statements provided
- `Status::internal` - Query execution, metrics collection, or serialization failure

**Client Handling**:
- Handle `unimplemented` gracefully with clear user message
- Retry transient errors as appropriate
- Validate response format (expect at least one batch with 8-field schema)
- Any error response means no metrics were collected

## Extensibility

### Design Principles

The protocol is designed to be:

1. **Format-Agnostic**: Any file format (Parquet, CSV, JSON, ORC, Avro, etc.) can add metrics using naming conventions
2. **Execution-Agnostic**: Custom execution plan nodes can emit metrics in standard categories
3. **Forward-Compatible**: Unknown metrics are safely ignored by clients
4. **Language-Agnostic**: Simple flat table format works in any programming language
5. **Type-Safe**: Proper Arrow types prevent parsing ambiguities

### Adding Custom Metrics

Servers can add custom metrics as long as they follow the 8-field schema:

**Custom Format Metrics**:
```
metric_name: "io.{format}.{metric_name}"
operator_name: "{Format}Exec"
operator_category: "io"
operator_parent: (parent operator name)
operator_index: (child index)
```

**Custom Compute Operators**:
- Choose the closest standard `operator_category` (filter, sort, projection, join, aggregate, window, distinct, limit, union, other)
- Use `compute.elapsed_compute` metric name with operator name and partition ID
- Populate `operator_parent` and `operator_index` for hierarchy

**Custom Query-Level Metrics**:
- Add new namespaced metric names with appropriate value types
- Use NULL for operator_name, partition_id, operator_category, operator_parent, and operator_index

### Client Metric Handling

Clients should handle metrics according to these principles:

1. **Display all metrics** with recognized `operator_category` values, even if `metric_name` is unknown
   - This enables forward compatibility with new server metrics
   - Allows debugging of custom or experimental operators

2. **Categorize and group** metrics by operator_category for presentation
   - Group "io" metrics together, "compute" metrics together, etc.

3. **Optionally validate** metric names against a known set
   - Strict mode: Warn or error on unknown metric_name
   - Permissive mode (default): Display all metrics
   - Configuration option: `unknown_metrics_policy: "allow" | "warn" | "error"`

4. **Gracefully handle** metrics with unknown operator_category
   - Display in "other" or "unknown" section
   - Log for debugging

5. **Validate required metrics** exist:
   - Query-level: `query.rows`, `query.batches`, `query.bytes`
   - Stage durations: `stage.parsing`, `stage.logical_planning`, `stage.physical_planning`, `stage.execution`, `stage.total`

6. **Do not fail** on missing optional metrics (format-specific, compute per-partition, etc.)

## References

- [Apache Arrow Flight SQL Protocol](https://arrow.apache.org/docs/format/FlightSql.html)
- [Apache Arrow IPC Format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
- [DataFusion Execution Plans](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html)

## License

This specification is provided under the Apache License 2.0, consistent with the Apache Arrow project.

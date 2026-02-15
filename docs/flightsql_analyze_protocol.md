# FlightSQL Analyze Protocol Specification

**Version**: 1.0
**Status**: Experimental

This document specifies a protocol extension for Apache Arrow Flight SQL servers to provide detailed query execution metrics. The protocol is designed to be implementation-agnostic and can be adopted by any FlightSQL server.

## Overview

The FlightSQL Analyze Protocol enables clients to retrieve detailed execution metrics for SQL queries through a custom Flight SQL action. This provides:

- Query execution timing breakdown (parsing, planning, execution)
- I/O statistics (bytes scanned, file operations)
- Format-specific metrics (Parquet pruning, CSV parsing, etc.)
- Per-operator compute time by partition
- Extensible metric model for custom execution plan nodes

## Action Specification

### Action Type

**Action Name**: `"analyze_query"`

**Purpose**: Execute a SQL query with metrics collection enabled and return detailed execution statistics.

### Request Format

**Request Body**: UTF-8 encoded SQL query string

**Important**: The SQL query must contain exactly one SQL statement. Multiple statements (e.g., separated by semicolons) are not supported and will result in an error.

**Example**:
```rust
Action {
    r#type: "analyze_query".to_string(),
    body: "SELECT * FROM table WHERE id > 100".as_bytes().to_vec().into()
}
```

**Request Encoding**: The SQL query string should be encoded as UTF-8 bytes in the `Action.body` field.

### Response Format

The response is a stream of `arrow_flight::Result` messages. Each `Result.body` contains serialized `FlightData` messages.

#### Response Metadata

The first `FlightData` message (schema message) contains the query text in its metadata:

**Metadata Key**: `"sql_query"`
**Metadata Value**: UTF-8 encoded SQL query string

This allows the client to correlate the metrics with the original query without requiring a separate record batch.

#### Metrics Batch

**Purpose**: Single Arrow RecordBatch containing a flat table where each row represents a single metric

**Schema**:
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| metric_name | Utf8 | false | Name of the metric (see standard names below) |
| value | UInt64 | false | Numeric value of the metric |
| value_type | Utf8 | false | Type of value: "duration_ns", "bytes", "count", or "ratio" |
| operator_name | Utf8 | true | Execution plan node name (e.g., "FilterExec", "ParquetExec") |
| partition_id | Int32 | true | Partition number for per-partition metrics |
| operator_category | Utf8 | true | Category: "filter", "sort", "projection", "join", "aggregate", "io", "other" |

**Cardinality**: Variable (one row per metric)

## Standard Metrics

### Query-Level Metrics

These metrics have `operator_name = NULL`, `partition_id = NULL`, `operator_category = NULL`:

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `rows` | count | Total number of output rows |
| `batches` | count | Total number of output batches |
| `bytes` | bytes | Total output size in bytes |

### Duration Metrics

Timing breakdown for query execution phases. All have `operator_name = NULL`, `partition_id = NULL`, `operator_category = NULL`:

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `parsing` | duration_ns | SQL parsing time in nanoseconds |
| `logical_planning` | duration_ns | Logical plan creation time |
| `physical_planning` | duration_ns | Physical plan creation time |
| `execution` | duration_ns | Query execution time |
| `total` | duration_ns | Total query time (sum of all phases) |

### Generic I/O Metrics

These metrics apply to all file format readers. They should include:
- `operator_name`: The scan operator name (e.g., "ParquetExec", "CsvExec", "JsonExec")
- `operator_category = "io"`
- `partition_id = NULL` (unless per-partition I/O stats are available)

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `bytes_scanned` | bytes | Total bytes read from storage |
| `time_opening` | duration_ns | Time spent opening files |
| `time_scanning` | duration_ns | Time spent reading/scanning data |
| `output_rows` | count | Number of rows produced by scan operator |

### Format-Specific I/O Metrics

Metrics specific to particular file formats should use a naming convention: `{format}_{metric_name}`

#### Parquet Metrics

All Parquet metrics use `operator_name = "ParquetExec"` and `operator_category = "io"`:

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `parquet_rg_pruned` | count | Row groups pruned by statistics |
| `parquet_rg_matched` | count | Row groups matched (not pruned) by statistics |
| `parquet_bloom_pruned` | count | Row groups pruned by bloom filters |
| `parquet_bloom_matched` | count | Row groups matched by bloom filters |
| `parquet_page_index_pruned` | count | Row groups pruned by page index |
| `parquet_page_index_matched` | count | Row groups matched by page index |

#### CSV Metrics (Example)

Example metrics for CSV format (`operator_name = "CsvExec"`, `operator_category = "io"`):

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `csv_rows_parsed` | count | Number of CSV rows successfully parsed |
| `csv_parse_errors` | count | Number of rows with parse errors |

#### JSON Metrics (Example)

Example metrics for JSON format (`operator_name = "JsonExec"`, `operator_category = "io"`):

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `json_invalid_rows` | count | Invalid JSON records skipped |
| `json_parse_errors` | count | JSON parsing errors encountered |

#### Other Formats

Additional formats can define their own metrics following the `{format}_{metric_name}` convention:
- **ORC**: `orc_stripe_pruned`, `orc_stripe_matched`
- **Arrow IPC**: `arrow_dictionary_hits`, `arrow_dictionary_misses`
- **Custom formats**: Any format-specific naming

### Compute Metrics

Metrics for CPU-intensive operators. The `elapsed_compute` metric appears in two forms:

#### Aggregate Compute Time

Total compute time across all operators:
- `metric_name = "elapsed_compute"`
- `operator_name = NULL`
- `partition_id = NULL`
- `operator_category = NULL`

#### Per-Operator, Per-Partition Compute Time

Detailed breakdown by operator and partition:
- `metric_name = "elapsed_compute"`
- `operator_name`: Name of the operator (e.g., "FilterExec", "ProjectionExec")
- `partition_id`: Partition number (0-indexed)
- `operator_category`: One of:
  - `"filter"` - Filter operations
  - `"sort"` - Sort and sort-preserving merge operations
  - `"projection"` - Projection operations
  - `"join"` - Join operations (hash, nested loop, sort-merge, etc.)
  - `"aggregate"` - Aggregation operations
  - `"other"` - Other compute operations

## Example Response

### Response Metadata
```
Metadata in schema FlightData message:
  sql_query: "SELECT * FROM table WHERE id > 100"
```

### Metrics Batch
```
metric_name              value         value_type   operator_name    partition_id  operator_category
------------------------ ------------- ------------ ---------------- ------------- ------------------
rows                     1000          count        NULL             NULL          NULL
batches                  10            count        NULL             NULL          NULL
bytes                    50000         bytes        NULL             NULL          NULL
parsing                  12000000      duration_ns  NULL             NULL          NULL
logical_planning         45000000      duration_ns  NULL             NULL          NULL
physical_planning        78000000      duration_ns  NULL             NULL          NULL
execution                234000000     duration_ns  NULL             NULL          NULL
total                    369000000     duration_ns  NULL             NULL          NULL
bytes_scanned            1000000       bytes        ParquetExec      NULL          io
time_opening             50000000      duration_ns  ParquetExec      NULL          io
time_scanning            150000000     duration_ns  ParquetExec      NULL          io
output_rows              10000         count        ParquetExec      NULL          io
parquet_rg_pruned        16            count        ParquetExec      NULL          io
parquet_rg_matched       4             count        ParquetExec      NULL          io
parquet_bloom_pruned     12            count        ParquetExec      NULL          io
parquet_bloom_matched    8             count        ParquetExec      NULL          io
elapsed_compute          12345678      duration_ns  NULL             NULL          NULL
elapsed_compute          1500          duration_ns  FilterExec       0             filter
elapsed_compute          1400          duration_ns  FilterExec       1             filter
elapsed_compute          1300          duration_ns  FilterExec       2             filter
elapsed_compute          1200          duration_ns  FilterExec       3             filter
elapsed_compute          1200          duration_ns  ProjectionExec   0             projection
elapsed_compute          1100          duration_ns  ProjectionExec   1             projection
elapsed_compute          1050          duration_ns  ProjectionExec   2             projection
elapsed_compute          1000          duration_ns  ProjectionExec   3             projection
```

## Implementation Guide

### Server Implementation

To implement this protocol in a FlightSQL server:

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
   - Create metrics RecordBatch
   - Encode as FlightData using `batches_to_flight_data()` or equivalent
   - Add SQL query to the schema FlightData message metadata with key `"sql_query"`
   - Serialize each FlightData to bytes (protobuf encoding)
   - Wrap each serialized FlightData in `arrow_flight::Result { body: bytes }`
   - Stream Result messages to client

**Pseudo-code**:
```rust
async fn do_action_fallback(&self, request: Request<Action>) -> Result<Response<Stream>> {
    let action = request.into_inner();

    if action.r#type == "analyze_query" {
        // 1. Extract SQL
        let sql = String::from_utf8(action.body.to_vec())?;

        // 2. Execute with metrics
        let stats = self.analyze_query(&sql).await?;

        // 3. Convert to metrics batch
        let metrics_batch = stats.to_metrics_table()?;

        // 4. Encode as FlightData with SQL in metadata
        let mut flight_data = batches_to_flight_data(&metrics_batch.schema(), vec![metrics_batch])?;

        // Add SQL query to schema message metadata
        if let Some(schema_msg) = flight_data.first_mut() {
            schema_msg.app_metadata = sql.as_bytes().to_vec().into();
        }

        // 5. Serialize and wrap in Result messages
        let results: Vec<arrow_flight::Result> = flight_data
            .into_iter()
            .map(|fd| arrow_flight::Result { body: fd.encode_to_vec().into() })
            .collect();

        // 6. Return stream
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
   let action = Action {
       r#type: "analyze_query".to_string(),
       body: sql.as_bytes().to_vec().into(),
   };
   let stream = client.do_action(action).await?;
   ```

2. **Receive Stream**
   - Collect `arrow_flight::Result` messages from stream

3. **Decode FlightData and Extract Metadata**
   ```rust
   let mut flight_data_vec = Vec::new();
   let mut sql_query = None;

   for result in result_messages {
       let flight_data = FlightData::decode(result.body.as_ref())?;

       // Extract SQL from first message (schema) metadata
       if sql_query.is_none() && !flight_data.app_metadata.is_empty() {
           sql_query = Some(String::from_utf8(flight_data.app_metadata.to_vec())?);
       }

       flight_data_vec.push(flight_data);
   }
   ```

4. **Convert to RecordBatch**
   ```rust
   let batches = flight_data_to_batches(&flight_data_vec)?;
   let metrics_batch = batches[0].clone();
   let query_text = sql_query.expect("SQL query not found in metadata");
   ```

5. **Reconstruct Statistics**
   - Use query text from metadata
   - Parse metrics batch to reconstruct execution statistics

### Error Handling

**Server Errors**:
- `Status::unimplemented` - Server doesn't support analyze protocol
- `Status::invalid_argument` - Invalid SQL, malformed request, or multiple SQL statements provided
- `Status::internal` - Query execution or serialization failure

**Client Handling**:
- Gracefully handle `unimplemented` with clear user message
- Retry transient errors as appropriate
- Validate response format (expect metadata with `sql_query` and at least one batch)
- Handle missing metadata gracefully (older protocol versions)

## Extensibility

### Design Principles

The protocol is designed to be:

1. **Format-Agnostic**: Any file format (Parquet, CSV, JSON, ORC, Avro, etc.) can add metrics using naming conventions
2. **Execution-Agnostic**: Custom execution plan nodes can emit metrics in standard categories
3. **Forward-Compatible**: Unknown metrics are safely ignored by clients
4. **Language-Agnostic**: Simple flat table format works in any programming language
5. **Type-Safe**: Proper Arrow types prevent parsing ambiguities

### Adding Custom Metrics

Servers can add custom metrics as long as they follow the schema:

**Custom Format Metrics**:
```
metric_name: "{format}_{metric_name}"
operator_name: "{Format}Exec"
operator_category: "io"
```

**Custom Compute Operators**:
- Choose the closest standard `operator_category` (filter, sort, projection, join, aggregate, other)
- Use `elapsed_compute` metric name with operator name and partition ID

**Custom Query-Level Metrics**:
- Add new metric names with appropriate value types
- Use NULL for operator_name, partition_id, and operator_category

### Client Compatibility

Clients should:
- Parse only metrics they recognize
- Ignore unknown metric names gracefully
- Log or track unknown metrics for debugging
- Not fail on missing optional metrics
- Validate required metrics (rows, batches, bytes, durations)

## Version History

**Version 1.0** (2025-02):
- Initial specification
- Standard metric definitions
- Parquet, CSV, JSON format examples
- Compute metrics with operator categories

## References

- [Apache Arrow Flight SQL Protocol](https://arrow.apache.org/docs/format/FlightSql.html)
- [Apache Arrow IPC Format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
- [DataFusion Execution Plans](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html)

## License

This specification is provided under the Apache License 2.0, consistent with the Apache Arrow project.

# Arrow Flight Analyze Protocol Specification

**Version**: 0.1
**Status**: Experimental

This document specifies a protocol extension for Apache Arrow Flight services to provide detailed query execution metrics. The protocol is modeled on Apache DataFusion's execution metrics and is intended to generalize to other engines; see [Relationship to DataFusion](#relationship-to-datafusion).

## Overview

The Arrow Flight Analyze Protocol enables clients to retrieve detailed execution metrics for queries through a custom Arrow Flight action. This provides:

- Query execution timing breakdown (parsing, planning, execution)
- I/O statistics (bytes scanned, file operations)
- Format-specific metrics (Parquet pruning, etc.)
- Per-operator compute time by partition
- Execution plan hierarchy, via stable node ids, for reconstructing query plan structure
- Extensible metric model for custom execution plan nodes

### Protocol Scope

This protocol is an **Apache Arrow Flight** extension, not specific to Flight SQL. While it naturally pairs with Flight SQL for SQL query analysis, any Arrow Flight service can implement the `analyze_query` action to provide execution metrics.

The examples in this specification use SQL for illustration, but the protocol works with any query representation that the Flight service supports.

### Related Work

The protocol composes ideas from existing systems rather than inventing new ones; the gap it fills is that **Arrow Flight has no standardized, in-band, machine-readable way to return execution telemetry**:

- **`EXPLAIN ANALYZE`** (Postgres, DataFusion, DuckDB, ...): rich per-operator metrics, but delivered as engine-specific text or JSON out of band from the transport. Postgres's `EXPLAIN (ANALYZE, FORMAT JSON)` is the closest precedent for structured output; this protocol replaces the format-specific document with a flat Arrow relation.
- **Trino/Presto query stats and Spark's SQL metrics APIs**: execution metrics over a separate REST channel, with stable plan-node ids identifying operators. The node-id approach here follows that precedent.
- **ClickHouse `system.query_log`**: flat metric rows queryable as a table — the same "metrics are data" shape this protocol uses, but post-hoc rather than in-band.
- **OpenMetrics/Prometheus**: the namespaced metric-name convention (`io.parquet.bytes_scanned`) follows their naming discipline. The `value_type` field is a deliberately minimal unit model; adopters needing richer semantics (temporality, exemplars) should look at the OpenTelemetry metrics data model, which this protocol does not attempt to replicate.
- **Substrait**: a future request/response field may carry Substrait plans; Substrait plan-relation ids would then be the natural cross-engine operator identity, complementing the per-execution `node_id` used here.
- **Distributed DataFusion engines** ([DataFusion Distributed](https://datafusion-contrib.github.io/datafusion-distributed/), [Apache DataFusion Ballista](https://github.com/apache/datafusion-ballista), and production Arrow Flight remote-execution systems): a distributed plan is still one plan tree, with network-boundary operators between stages; after execution each worker's per-node metrics are folded back into the coordinator's copy of the plan before display. The [`distributed.*` namespace](#distributed-execution-metrics) and the unified-tree rule follow that model. Notably, systems that ship per-node metrics back today correlate them with the coordinator's tree *positionally* (DFS pre-order) — Ballista's executors report each task's metric sets to the scheduler keyed by pre-order node position within a stage, and its `GetJobMetrics` RPC (per-job metrics retained on the scheduler after completion) is a working precedent for the two-phase analyze in [Future Work](#future-work). The `node_id`/`parent_node_id` columns make exactly that correlation explicit on the wire.

### Relationship to DataFusion

The reference implementation is built on Apache DataFusion, and the standard metric set below is derived from DataFusion's `MetricSet` values. Non-DataFusion implementers should treat the metric tables as a **DataFusion mapping** of the abstract categories (query, stage, io, compute): implement the metrics that have equivalents in their engine, keep the namespaces and `value_type` conventions, and omit the rest. Clients are required to tolerate missing optional metrics (see [Client Metric Handling](#client-metric-handling)).

## Action Specification

### Action Type

**Action Name**: `"analyze_query"`

**Purpose**: Execute a SQL query with metrics collection enabled and return detailed execution statistics.

### Request Format

**Request Body**: JSON-encoded query request structure. JSON is used for the request because bodies are small, debuggability matters more than throughput here, and it avoids coupling the extension to a protobuf schema registry. (Flight SQL precedent would be protobuf `Any`; adopters that need it can layer that in a future version.)

The request body should be a JSON object with the following structure:

```json
{
  "sql": "SELECT * FROM table WHERE id > 100",
  "protocol_version": "0.1"
}
```

**Current Fields**:
- `sql` (string, required): The SQL query to analyze. Must contain exactly one SQL statement. Multiple statements (e.g., separated by semicolons) are not supported and will result in an error.
- `protocol_version` (string, optional): The protocol version the client implements. Servers MUST reject a request whose major version they do not support with `invalid_argument`. When absent, the server assumes the client speaks the server's version.

**Future Extensibility**:
The protocol is designed to be extensible. Additional query representation fields may be supported in the future:
- `substrait` (bytes): Substrait query plan (binary or JSON)
- `logical_plan` (string): Serialized logical plan
- `physical_plan` (string): Serialized physical plan

Servers should ignore unknown fields and clients should only send one query representation field at a time.

**Request Encoding**: The JSON object should be serialized to UTF-8 bytes in the `Action.body` field.

### Response Format

The response is a stream of `arrow_flight::Result` messages. Each `Result.body` contains one serialized `FlightData` message (protobuf encoding). Concatenated, the `FlightData` messages form a standard Arrow IPC stream: one schema message followed by **one or more** record batch messages (and dictionary batches if needed). Clients MUST NOT assume the metrics table arrives as a single batch; servers MAY split large tables.

**Response Metadata** (Arrow schema metadata on the metrics batch schema):

| Key | Required | Description |
|-----|----------|-------------|
| `analyze.protocol_version` | yes | Protocol version the server implements (e.g., `"0.1"`) |
| `analyze.query_id` | recommended | Opaque correlation id for this request (e.g., a UUID). Lets clients issuing concurrent analyze calls match responses to requests. |

**Note**: The query text is NOT echoed in the response. The client is responsible for retaining the original query and correlating it with the response (using `analyze.query_id` when concurrent requests are in flight).

#### Metrics Batch

**Purpose**: A flat Arrow table where each row represents a single metric observation.

**Schema**:
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| metric_name | Utf8 | false | Namespaced metric name (e.g., "query.rows", "stage.parsing", "io.parquet.bytes_scanned") |
| value | UInt64 | false | Numeric value of the metric |
| value_type | Utf8 | false | Unit of value: "duration_ns", "bytes", or "count" |
| operator_name | Utf8 | true | Execution plan node display name (e.g., "FilterExec", "DataSourceExec"). A label, NOT an identifier — plans routinely contain multiple nodes with the same name. |
| partition_id | Int32 | true | Partition rank for per-partition metrics (see Compute Metrics) |
| operator_category | Utf8 | true | Category: "filter", "sort", "projection", "join", "aggregate", "window", "distinct", "limit", "union", "exchange", "io", "other" — see [Operator Categories](#operator-categories) |
| node_id | Int32 | true | Stable id of the plan node this metric belongs to (NULL for query/stage-level metrics) |
| parent_node_id | Int32 | true | `node_id` of the node's parent (NULL for the root node and for query/stage-level metrics) |

**Canonical row key**: `(metric_name, node_id, partition_id)`. The `operator_name`, `operator_category`, and the namespace prefix of `metric_name` are denormalized presentation hints; they carry no identity.

**Value types**: Durations are always nanoseconds (`duration_ns`); sizes are bytes; everything else is a plain count. There is deliberately no fractional/ratio value type in v0.1 — ratios (selectivity, pruning effectiveness) are derived by clients from count metrics.

**Cardinality**: One row per metric, per node, per partition. For large plans this is `O(operators × partitions × metrics)`; see [Operational Considerations](#operational-considerations).

### Execution Plan Hierarchy

Every node in the execution plan is assigned a stable integer `node_id` by **pre-order traversal, with the root as 0**. The `(node_id, parent_node_id)` pairs on operator-level metric rows fully describe the plan tree, even when the same operator type appears multiple times (e.g., partial and final `AggregateExec`, repeated `RepartitionExec` nodes, self-joins).

NULL rules:
- **Query/stage-level rows** (`operator_name = NULL`): both `node_id` and `parent_node_id` are NULL.
- **Root operator rows**: `node_id = 0`, `parent_node_id = NULL`.
- **All other operator rows**: both fields are non-NULL.

**Example**: For a plan `ProjectionExec -> FilterExec -> DataSourceExec` (root first):
- ProjectionExec: `node_id = 0`, `parent_node_id = NULL`
- FilterExec: `node_id = 1`, `parent_node_id = 0`
- DataSourceExec: `node_id = 2`, `parent_node_id = 1`

## Operator Categories

The `operator_category` column groups plan nodes by their function so clients can organize metrics without recognizing every engine-specific operator name. Categories describe what the **physical operator** does, not the logical intent of the query — e.g. an engine that plans `SELECT DISTINCT` as a grouped aggregation reports those nodes as `aggregate`, not `distinct`.

| Category | Meaning | Representative operators (DataFusion) |
|----------|---------|----------------------------------------|
| `io` | Scan/data-source nodes reading external data. Report format-specific metrics under `io.{format}.*` and are excluded from the compute breakdown (see [Format-Specific I/O Metrics](#format-specific-io-metrics)) | `DataSourceExec` |
| `projection` | Column selection and expression evaluation | `ProjectionExec` |
| `filter` | Row-level predicate evaluation | `FilterExec` |
| `sort` | Ordering, including order-preserving merges and top-N variants | `SortExec` (incl. TopK), `SortPreservingMergeExec` |
| `aggregate` | Grouped or scalar aggregation, at any phase (partial/final) | `AggregateExec` |
| `join` | All join algorithms, including cross joins | `HashJoinExec`, `SortMergeJoinExec`, `NestedLoopJoinExec`, `CrossJoinExec`, `SymmetricHashJoinExec` |
| `window` | Window-function evaluation | `WindowAggExec`, `BoundedWindowAggExec` |
| `distinct` | Dedicated deduplication operators (engines without one report deduplication under the operator that implements it, typically `aggregate`) | — |
| `limit` | Row-count truncation | `GlobalLimitExec`, `LocalLimitExec` |
| `union` | Concatenating/interleaving multiple inputs without join semantics | `UnionExec`, `InterleaveExec` |
| `exchange` | Network-boundary data movement between workers in a distributed plan (see [Distributed Execution Metrics](#distributed-execution-metrics)). **Not** for process-local repartitioning | `NetworkShuffleExec`, `ShuffleWriterExec`/`ShuffleReaderExec`, `RemoteExec`-style nodes |
| `other` | Everything else: local repartition/coalesce, empty relations, unnest, table functions, custom nodes | `RepartitionExec`, `CoalesceBatchesExec`, `UnnestExec` |

**Classification rules**:
- Classify by the operator's **dominant function** — the work its metrics measure. A hybrid operator goes to the category of its dominant cost (e.g. a fused sort-with-limit is `sort`).
- Category assignment for ambiguous operators is implementation-defined; `other` is always valid, and clients MUST handle it (and unknown categories) gracefully per [Client Metric Handling](#client-metric-handling).
- The category is a denormalized presentation hint carried on each of the node's metric rows; all rows for one `node_id` MUST carry the same category.

## Metric Namespaces

Metric names use a hierarchical namespace structure to prevent collisions and provide clear semantic grouping:

**Format**: `{namespace}.{metric_name}`

**Standard Namespaces**:
- `query.*` - Query-level metrics (rows, batches, bytes)
- `stage.*` - Execution stage durations (parsing, logical_planning, physical_planning, execution, total)
- `io.parquet.*` - Parquet-specific I/O metrics
- `io.csv.*` - CSV-specific I/O metrics
- `io.json.*` - JSON-specific I/O metrics
- `io.arrow.*` - Arrow IPC-specific I/O metrics
- `compute.*` - Compute metrics (elapsed_compute with operator breakdown)
- `index.*` - Reserved for index-related metrics (future: index_hits, index_scans)
- `distributed.*` - Distributed execution metrics (see [Distributed Execution Metrics](#distributed-execution-metrics))

**Important**: There is no generic `io.*` namespace. Each file format reports its own complete set of I/O metrics under its specific namespace (e.g., `io.parquet.*`, `io.csv.*`). This prevents mixing aggregated and raw data. Only namespaced metric names are valid on the wire.

## Standard Metrics

### Query-Level Metrics

These metrics have `operator_name = NULL`, `partition_id = NULL`, `operator_category = NULL`, `node_id = NULL`, `parent_node_id = NULL`:

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `query.rows` | count | Total number of output rows |
| `query.batches` | count | Total number of output batches |
| `query.bytes` | bytes | Total in-memory Arrow size of the output batches (as reported by `RecordBatch::get_array_memory_size` in the reference implementation). This is NOT a serialized/wire size; servers implementing this metric MUST use the in-memory definition. |

### Duration Metrics

Timing breakdown for query execution phases. All have `operator_name = NULL`, `partition_id = NULL`, `operator_category = NULL`, `node_id = NULL`, `parent_node_id = NULL`:

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `stage.parsing` | duration_ns | Query parsing time in nanoseconds |
| `stage.logical_planning` | duration_ns | Logical plan creation time |
| `stage.physical_planning` | duration_ns | Physical plan creation time |
| `stage.execution` | duration_ns | Query execution wall-clock time |
| `stage.total` | duration_ns | Total wall-clock time for the request |

**Timing semantics**: `stage.*` metrics are wall-clock durations of non-overlapping request phases; they are additive and sum to approximately `stage.total`. `compute.elapsed_compute` and `io.*.time_*` metrics are per-operator CPU/IO time summed across partitions that execute **concurrently**; under pipelined, parallel execution they routinely exceed `stage.execution` and MUST NOT be treated as additive with the stage timers or with each other.

### Format-Specific I/O Metrics

Each I/O metric row carries:
- `operator_name`: The scan operator's display name (in DataFusion 51+, file scans are `"DataSourceExec"`)
- `operator_category = "io"`
- `partition_id = NULL` (values are summed across partitions)
- `node_id` / `parent_node_id`: identity of the scan node

A query with multiple scans (joins, unions) produces a separate set of I/O rows per scan node, distinguished by `node_id`. A mixed-format query reports each scan under its own format namespace.

**Common I/O Metrics** (each format provides these under its own namespace):

| Metric Pattern | Value Type | Description |
|----------------|------------|-------------|
| `io.{format}.bytes_scanned` | bytes | Total bytes read from storage (where available) |
| `io.{format}.time_opening` | duration_ns | Time spent opening files |
| `io.{format}.time_scanning` | duration_ns | Time spent reading/scanning data |

#### Parquet Metrics

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `io.parquet.bytes_scanned` | bytes | Total bytes read from Parquet files |
| `io.parquet.time_opening` | duration_ns | Time spent opening Parquet files |
| `io.parquet.time_scanning` | duration_ns | Time spent reading/scanning Parquet data |
| `io.parquet.output_rows` | count | Number of rows produced by the scan node |
| `io.parquet.rg_pruned` | count | Row groups pruned by statistics |
| `io.parquet.rg_matched` | count | Row groups matched (not pruned) by statistics |
| `io.parquet.bloom_pruned` | count | Row groups pruned by bloom filters |
| `io.parquet.bloom_matched` | count | Row groups matched by bloom filters |
| `io.parquet.page_index_pruned` | count | Rows pruned by the page index |
| `io.parquet.page_index_matched` | count | Rows matched by the page index |

#### CSV / JSON / Arrow IPC Metrics

The reference implementation emits the common I/O metrics (`time_opening`, `time_scanning`, and `bytes_scanned` where the engine reports it) under `io.csv.*`, `io.json.*`, and `io.arrow.*`. Format-specific names such as `io.csv.rows_parsed`, `io.csv.parse_errors`, `io.json.invalid_rows`, or `io.arrow.dictionary_hits` are **reserved** for future use; they are listed here so implementations do not repurpose them, but v0.1 does not define or emit them.

#### Other Formats

Additional formats can define their own metrics under their namespace (e.g., `io.orc.stripe_pruned`). Use `io.{format}.*` for custom format metrics.

### Compute Metrics

Metrics for CPU-intensive operators. The `compute.elapsed_compute` metric appears in two forms:

#### Aggregate Compute Time

Total compute time across all (non-scan) operators:
- `metric_name = "compute.elapsed_compute"`
- All other columns NULL

#### Per-Operator, Per-Partition Compute Time

Detailed breakdown by operator and partition:
- `metric_name = "compute.elapsed_compute"`
- `operator_name`: Display name of the operator (e.g., "FilterExec")
- `partition_id`: **Rank** of the value among the node's partitions when sorted ascending (0-based). It preserves the per-partition distribution for skew analysis but is not guaranteed to be the physical partition number.
- `operator_category`: One of `filter`, `sort`, `projection`, `join`, `aggregate`, `window`, `distinct`, `limit`, `union`, `exchange`, `other` (see [Operator Categories](#operator-categories))
- `node_id` / `parent_node_id`: identity of the operator node

**Note**: Scan nodes report under `io` and are excluded from the compute breakdown and from the aggregate compute total. Exchange (network boundary) nodes report their local CPU work here under category `exchange`; their network-side metrics are reported separately under `distributed.*`.

### Distributed Execution Metrics

Distributed engines execute parts of the plan on remote workers, connected by network-boundary **exchange operators** — shuffle/coalesce nodes in a stage/task scheduler (e.g. DataFusion Distributed's `NetworkShuffleExec` and `NetworkCoalesceExec`), or a `RemoteExec`-style node in a single-hop plan-fragment shipper. The `distributed.*` namespace carries the network-side metrics of those boundaries. The reference implementation is a single-node engine and does not emit these metrics; the definitions below are derived from surveyed distributed DataFusion systems so that distributed adopters map onto real metric sets. Servers implementing distributed execution SHOULD include `"distributed"` in the `namespaces` list of `analyze_query_capabilities`.

**One table, one tree.** A distributed query still produces a single metrics table over the **unified** plan: servers MUST fold each worker's sub-plan metrics back into the coordinator's plan tree before emitting, and assign `node_id` by pre-order traversal of that unified tree. In stage-based engines (Ballista-style), stages are stitched back together at their exchange boundaries (e.g. a shuffle reader leaf reconnects to the producer stage's shuffle writer root). Remote operators are therefore indistinguishable from local ones in the response, apart from the exchange nodes between them.

**Trees, not DAGs.** Stage graphs can be DAGs: a broadcast stage may feed several consumers (e.g. a broadcast join build side read by N exchange nodes). Servers MUST emit each producer node exactly once — attach the shared subtree under exactly one of its consumer exchange nodes (implementation-defined; e.g. the first in pre-order) so its metrics are not double-counted. The `distributed.consumers` metric on the subtree's root records the fan-out; an explicit plan-edge representation is future work.

**Adaptive execution.** Engines that re-plan stages mid-flight (adaptive query execution: runtime join selection, stage elimination, stage re-execution) MUST emit the plan as it **finally executed**. Node ids are assigned after the query completes, over that final plan; retried stages report the winning attempt's metrics only.

**Aggregation across tasks.** When multiple tasks (workers) execute the same stage in parallel, each plan node's metric values are aggregated across tasks before emission — sums for counts, bytes, and durations — mirroring how I/O metrics are summed across partitions. Per-partition `compute.elapsed_compute` rows rank values over the union of all tasks' partitions. Engines that cannot attribute values to individual partitions exactly (a known hazard for subtrees beneath partition-coalescing operators in task-parallel engines) SHOULD emit an aggregated row (`partition_id = NULL`) rather than approximate or inflated per-partition rows. A per-task or per-worker breakdown requires a task identity the v0.1 schema does not carry; see [Future Work](#future-work).

#### Exchange-Node Metrics

Each exchange operator reports with `operator_category = "exchange"`, `partition_id = NULL`, and its own `node_id`/`parent_node_id`. A boundary may be represented by a single receiving operator (fragment shippers, network shuffle readers) or by a sender/receiver pair (a shuffle writer at a producer stage's root plus a shuffle reader leaf in the consumer); both sides are `exchange` nodes and each reports the metrics for its own side.

**Receiver-side metrics**:

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `distributed.bytes_transferred` | bytes | Serialized bytes received across the boundary (wire size — NOT the in-memory size used by `query.bytes`) |
| `distributed.messages` | count | Data messages (e.g. Arrow Flight `FlightData`) received |
| `distributed.rpc_calls` | count | RPCs issued to serve the boundary |
| `distributed.rpc_retries` | count | Failed attempts that were retried (connection acquisition, endpoint failover, fetch retries) |
| `distributed.tasks` | count | Remote tasks feeding this boundary |
| `distributed.time_to_first_batch` | duration_ns | Time from issuing the request to receiving the first record batch |
| `distributed.time_transferring` | duration_ns | Time spent receiving data across the boundary |
| `distributed.network_latency_sum` | duration_ns | Sum of per-message network latencies (clients derive the average using `_count`) |
| `distributed.network_latency_count` | count | Number of latency observations contributing to `_sum` |

**Sender-side metrics** (on writer-style exchange nodes, where they exist):

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `distributed.bytes_written` | bytes | Serialized bytes written for downstream consumers (e.g. shuffle files) |
| `distributed.time_writing` | duration_ns | Time spent serializing and writing exchange output |
| `distributed.consumers` | count | Downstream consumers of this node's output (> 1 when a broadcast stage feeds several exchanges; see "Trees, not DAGs") |

Byte accounting is deliberately split: engines often source the two sides differently (received bytes from the reader's metrics, written bytes from shuffle file statistics), and with broadcast or partial fetches they are not equal, so there is no single "transferred" number spanning both sides.

`distributed.network_latency_min`, `_max`, `_p50`, `_p95`, and `_p99` are **reserved** for pre-aggregated latency distribution values (each a `duration_ns` computed server-side, e.g. from a quantile sketch); v0.1 does not define or emit them, consistent with the no-server-computed-ratios rule (`_sum`/`_count` are the required pair). `distributed.time_queued` is likewise **reserved** for time spent waiting on admission control or backpressure permits before fetching — engines with such governors MUST NOT fold that wait into `distributed.network_latency_sum`.

Exchange operators MAY additionally report `compute.elapsed_compute` (category `exchange`) for their local CPU work, like any other operator.

#### Query-Level Distributed Metrics

These rows have all identity columns NULL (like other query-level metrics):

| Metric Name | Value Type | Description |
|-------------|------------|-------------|
| `distributed.stages` | count | Number of distributed stages in the plan |
| `distributed.tasks` | count | Total tasks scheduled across all stages |
| `distributed.plan_bytes_sent` | bytes | Serialized plan fragments/tasks shipped to workers |
| `distributed.time_distributing_plan` | duration_ns | Time spent serializing and shipping plan fragments to workers |

`distributed.tasks` follows the `compute.elapsed_compute` precedent: the query-level row is the total, exchange-node rows are per-boundary.

**Timing semantics.** All `distributed.*` durations are wall-clock observations at a boundary that overlap the execution of upstream stages and each other (`time_to_first_batch` overlaps `time_transferring`); like the `compute.*` and `io.*` timers, they MUST NOT be treated as additive with the `stage.*` timers or with each other.

## Example Response

The client retains the original query: `"SELECT * FROM table WHERE id > 100"`

Plan: `ProjectionExec (0) -> FilterExec (1) -> DataSourceExec (2)`

```
metric_name                    value         value_type   operator_name    partition_id  operator_category  node_id  parent_node_id
------------------------------ ------------- ------------ ---------------- ------------- ------------------ -------- --------------
query.rows                     1000          count        NULL             NULL          NULL               NULL     NULL
query.batches                  10            count        NULL             NULL          NULL               NULL     NULL
query.bytes                    50000         bytes        NULL             NULL          NULL               NULL     NULL
stage.parsing                  12000000      duration_ns  NULL             NULL          NULL               NULL     NULL
stage.logical_planning         45000000      duration_ns  NULL             NULL          NULL               NULL     NULL
stage.physical_planning        78000000      duration_ns  NULL             NULL          NULL               NULL     NULL
stage.execution                234000000     duration_ns  NULL             NULL          NULL               NULL     NULL
stage.total                    369000000     duration_ns  NULL             NULL          NULL               NULL     NULL
io.parquet.bytes_scanned       1000000       bytes        DataSourceExec   NULL          io                 2        1
io.parquet.time_opening        50000000      duration_ns  DataSourceExec   NULL          io                 2        1
io.parquet.time_scanning       150000000     duration_ns  DataSourceExec   NULL          io                 2        1
io.parquet.output_rows         10000         count        DataSourceExec   NULL          io                 2        1
io.parquet.rg_pruned           16            count        DataSourceExec   NULL          io                 2        1
io.parquet.rg_matched          4             count        DataSourceExec   NULL          io                 2        1
compute.elapsed_compute        12345678      duration_ns  NULL             NULL          NULL               NULL     NULL
compute.elapsed_compute        1200          duration_ns  ProjectionExec   0             projection         0        NULL
compute.elapsed_compute        1250          duration_ns  ProjectionExec   1             projection         0        NULL
compute.elapsed_compute        1400          duration_ns  FilterExec       0             filter             1        0
compute.elapsed_compute        1500          duration_ns  FilterExec       1             filter             1        0
```

## Capability Discovery

**Action Name**: `"analyze_query_capabilities"`

Servers implementing `analyze_query` SHOULD also implement this action so clients can probe support and version before issuing (potentially expensive) analyze calls. The response is a single `arrow_flight::Result` whose body is a UTF-8 JSON object:

```json
{
  "protocol_version": "0.1",
  "request_formats": ["sql"],
  "namespaces": ["query", "stage", "compute", "io.parquet", "io.csv", "io.arrow", "io.json"]
}
```

Clients MUST ignore unknown fields. Servers not implementing the action return `unimplemented`, which clients should treat the same as an `unimplemented` response to `analyze_query` itself.

## Implementation Guide

### Server Implementation

To implement this protocol in an Arrow Flight service:

1. **Register Action Handler**
   - Implement `do_action` or `do_action_fallback` to recognize action type `"analyze_query"`

2. **Parse and Validate Request**
   - Decode `Action.body` as JSON
   - Reject unsupported `protocol_version` major versions with `invalid_argument`

3. **Execute Query**
   - Run the query to completion with execution plan metrics collection enabled

4. **Collect Metrics**
   - In a distributed engine, first gather each worker's per-node metrics and fold them into the coordinator's plan tree (see [Distributed Execution Metrics](#distributed-execution-metrics)); all subsequent steps operate on the unified tree
   - Assign each plan node a `node_id` by pre-order traversal (root = 0)
   - Traverse the plan extracting metrics from each operator; emit one row per metric value with the node's `node_id`/`parent_node_id`

5. **Build Response**
   - Create the metrics RecordBatch with the 8-field schema
   - Set `analyze.protocol_version` (and ideally `analyze.query_id`) in the schema metadata
   - Encode as FlightData using `batches_to_flight_data()` or equivalent
   - Serialize each FlightData to bytes (protobuf encoding) and wrap in `arrow_flight::Result { body }`
   - Stream Result messages to the client

### Client Implementation

To consume this protocol:

1. **Send Request**: JSON body with `sql` and `protocol_version`, action type `"analyze_query"`.
2. **Receive Stream**: collect all `arrow_flight::Result` messages.
3. **Decode**: decode each `Result.body` as a `FlightData` protobuf message; feed the sequence to an IPC decoder (e.g., `flight_data_to_batches`). Handle any number of data batches — concatenate them into one table.
4. **Validate**: check `analyze.protocol_version` in the schema metadata for major-version compatibility.
5. **Reconstruct**: parse the metrics table; rebuild the plan tree from `(node_id, parent_node_id)`; group per-partition compute rows by `node_id` (never by `operator_name` — names are not identities).

### Error Handling

**Server Behavior**:
Any error during request parsing, query execution, metrics collection, or response serialization results in complete failure. No partial metrics are returned.

**Error Codes**:
- `Status::unimplemented` - Server doesn't support the analyze protocol
- `Status::invalid_argument` - Invalid SQL, malformed request, unsupported protocol version, or multiple SQL statements provided
- `Status::internal` - Query execution, metrics collection, or serialization failure

**Client Handling**:
- Handle `unimplemented` gracefully with a clear user message
- Retry transient errors as appropriate — but note that a retried analyze re-executes the query (see below)
- Any error response means no metrics were collected

## Operational Considerations

**Re-execution cost.** `analyze_query` executes the query to completion and discards the results. Analyzing a query therefore costs a full extra execution, and the analyzed run is a *different execution* from the one the user experienced (caches may be warm, data may have changed). There is currently no fused "results plus metrics" mode; a two-phase design (execute normally, then fetch metrics for a statement id) and a streaming `analyze_query_live` variant are candidate future extensions.

**Resource controls.** Because the action runs an arbitrary query server-side, deployments should apply the same admission control, timeouts, and cost caps to `analyze_query` as to regular query execution. The protocol itself defines no limits.

**Payload size.** The metrics table is `O(operators × partitions × metrics)` rows. A scan with thousands of partitions produces a correspondingly large batch. Servers MAY split the table across multiple record batches (clients must handle this); future versions may add sampling or aggregation options.

**Distributed collection.** In a distributed engine the response cannot be built until every worker's metrics have arrived at the coordinator, adding a synchronization step after the result stream completes (surveyed systems use a dedicated metrics channel, piggyback metrics on the final data message, or retain per-job metrics on the scheduler for post-hoc retrieval). Per [Error Handling](#error-handling), if any worker's metrics cannot be retrieved the action fails — no partial metrics are returned. This also applies to *silent* loss inside the engine's own metric transport: surveyed systems have failure modes where a single non-serializable metric value drops an entire task's metric set, or where only successfully completed stages report at all — an `analyze_query` implementation MUST surface such gaps as `internal` errors rather than emit a silently incomplete table. Engines should also note that result caches must be bypassed for `analyze_query`: a cached response carries no live metrics.

**Authorization and audit.** The SQL travels inside an `Action` body. Proxies or middleware that authorize/audit only `DoGet`/`GetFlightInfo` tickets will not see it — route `do_action` through the same authorization and audit path as query execution. Also note that analyze output discloses plan internals (operator structure, partition counts, pruning effectiveness); deployments may want to gate the action separately from query execution.

## Extensibility

### Design Principles

The protocol is designed to be:

1. **Format-Agnostic**: Any file format (Parquet, CSV, JSON, ORC, Avro, etc.) can add metrics using the namespace conventions
2. **Execution-Agnostic**: Custom execution plan nodes can emit metrics in standard categories
3. **Forward-Compatible**: Clients display unknown metrics that carry a recognized category rather than rejecting them
4. **Language-Agnostic**: A flat Arrow table works in any language with an Arrow implementation
5. **Type-Safe**: Proper Arrow types prevent parsing ambiguities

### Adding Custom Metrics

Servers can add custom metrics as long as they follow the 8-field schema:

**Custom Format Metrics**:
```
metric_name: "io.{format}.{metric_name}"
operator_name: (scan node display name)
operator_category: "io"
node_id / parent_node_id: (scan node identity)
```

**Custom Compute Operators**:
- Choose the closest standard `operator_category` per the classification rules in [Operator Categories](#operator-categories)
- Use the `compute.elapsed_compute` metric name with operator name, partition rank, and node identity

**Custom Query-Level Metrics**:
- Add new namespaced metric names with appropriate value types
- Use NULL for operator_name, partition_id, operator_category, node_id, and parent_node_id

### Client Metric Handling

Clients should handle metrics according to these principles:

1. **Display all metrics** with recognized `operator_category` values, even if `metric_name` is unknown
   - This enables forward compatibility with new server metrics
   - Allows debugging of custom or experimental operators

2. **Categorize and group** metrics by operator_category for presentation

3. **Optionally validate** metric names against a known set
   - Strict mode: Warn or error on unknown metric_name
   - Permissive mode (default): Display all metrics
   - Configuration option: `unknown_metrics_policy: "allow" | "warn" | "error"`

4. **Gracefully handle** metrics with unknown operator_category
   - Display in an "other"/"unknown" section; log for debugging

5. **Validate required metrics** exist:
   - Query-level: `query.rows`, `query.batches`, `query.bytes`
   - Stage durations: `stage.parsing`, `stage.logical_planning`, `stage.physical_planning`, `stage.execution`, `stage.total`

6. **Do not fail** on missing optional metrics (format-specific, compute per-partition, etc.)

## Future Work

Tracked candidates for later protocol versions, roughly in priority order:

- Two-phase analyze (execute normally → fetch metrics by statement id) and/or a results-then-metrics mode, to avoid double execution
- `analyze_query_live`: streaming incremental metrics for long-running queries
- Per-operator/per-partition start and end timestamps for timeline (flamegraph) reconstruction
- Partition skew summary metrics
- A fractional value column (e.g., Float64) if server-computed ratios prove necessary
- Substrait request support, with Substrait plan-relation ids as cross-engine operator identity
- Response served via `Ticket` + standard `DoGet` instead of `Result`-wrapped FlightData, gaining standard streaming/dictionary handling
- `index.*` namespace definitions
- Per-task/per-worker breakdown of distributed metrics (DataFusion Distributed's "PerTask" format). Note that in surveyed systems worker identity is a set of strings (service, cluster, host/URL) rather than an integer, so this likely needs a nullable Utf8 identity column rather than reusing `partition_id`
- A text value column for non-numeric per-node diagnostics (e.g. a remote worker's end-of-execution dynamic-filter snapshot, which surveyed systems transport alongside numeric metrics)
- An explicit plan-edge representation for DAG-shaped distributed plans (broadcast stages feeding multiple consumers), replacing the emit-once/`distributed.consumers` convention
- A `memory.*` namespace for spill and memory metrics (`spilled_bytes`, `spilled_rows`, `spill_count`, peak memory) — first-class in DataFusion's metric model and in Ballista's metric transport, but not yet mapped by this protocol
- Config/resource context (memory limits, target partitions) in response metadata
- A UDTF form (`SELECT * FROM analyze('...')`) so metrics can be queried directly

## References

- [Apache Arrow Flight SQL Protocol](https://arrow.apache.org/docs/format/FlightSql.html)
- [Apache Arrow IPC Format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
- [DataFusion Execution Plans](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html)
- [DataFusion Distributed](https://datafusion-contrib.github.io/datafusion-distributed/)
- [Apache DataFusion Ballista](https://github.com/apache/datafusion-ballista)
- [OpenTelemetry Metrics Data Model](https://opentelemetry.io/docs/specs/otel/metrics/data-model/)
- [Substrait](https://substrait.io/)

## License

This specification is provided under the Apache License 2.0, consistent with the Apache Arrow project.

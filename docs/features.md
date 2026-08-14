# Features

## Internal Optional Features (Workspace Features)

`dft` incubates several optional features in it's `crates` directory.  This provides us with the ability to quickly iterate on new features and test them in the main application while at the same time making it easy to export them to their own crates when they are ready.

### Parquet Functions (`--features=functions-parquet`)

Includes functions from [datafusion-function-parquet] for querying Parquet files in DataFusion in `dft`.  For example:

```sql
SELECT * FROM parquet_metadata('my_parquet_file.parquet')
```

Or for reading the key-value metadata from a file's footer:

```sql
SELECT * FROM parquet_kv_metadata('my_parquet_file.parquet')
```

The Arrow schema embedded in the `ARROW:schema` key-value metadata entry (written by Arrow-based writers) can be decoded and printed with one row per field:

```sql
SELECT * FROM parquet_arrow_schema('my_parquet_file.parquet')
```

The data from specific row groups can be read by passing either a single row group index or an inclusive start and end index:

```sql
SELECT * FROM parquet_row_groups('my_parquet_file.parquet', 1)
SELECT * FROM parquet_row_groups('my_parquet_file.parquet', 1, 3)
```

Bloom filter details (presence, location, and size per column chunk) can be inspected with:

```sql
SELECT * FROM parquet_bloom_filter('my_parquet_file.parquet')
```

And a column's bloom filter can be probed for a value, returning one row per row group. A `false` in `might_contain` guarantees the value is absent from that row group, while `true` means it might be present (subject to the filter's false positive probability):

```sql
SELECT * FROM parquet_bloom_filter_check('my_parquet_file.parquet', 'user_id', 'abc123')
```

The dictionary for a column can be read with one row per dictionary entry per row group (row groups where the column has no dictionary page produce no rows):

```sql
SELECT * FROM parquet_dictionary('my_parquet_file.parquet', 'user_id')
```

Compression details can be inspected with one row per column chunk per row group, including the codec, encodings, uncompressed (pre-compression) and compressed (post-compression) sizes, and the compression efficiency as the percentage of space saved:

```sql
SELECT * FROM parquet_compression('my_parquet_file.parquet')
```

The physical page layout can be inspected with one row per page (dictionary and data pages), including the page type, encoding, value counts, and sizes. An optional column name restricts the output to a single column. The `offset`, `compressed_page_size`, and `first_row_index` columns come from the offset index and are null for files written without a page index:

```sql
SELECT * FROM parquet_pages('my_parquet_file.parquet')
SELECT * FROM parquet_pages('my_parquet_file.parquet', 'user_id')
```

The raw Parquet schema tree (as opposed to the embedded Arrow schema shown by `parquet_arrow_schema`) can be inspected with one row per schema node in depth-first order, including each node's repetition, physical/logical/converted type, field id, and, for leaf columns, the maximum definition and repetition levels:

```sql
SELECT * FROM parquet_schema('my_parquet_file.parquet')
```

And a single-row file summary (writer and format version, row/row group/column counts, file and footer sizes, total compressed and uncompressed data sizes, and whether the file has column statistics, a page index, or bloom filters) can be read with:

```sql
SELECT * FROM parquet_file_metadata('my_parquet_file.parquet')
```

### Arrow Functions (`--features=functions-arrow`)

Includes functions from [datafusion-functions-arrow] for inspecting Arrow IPC files (the Arrow "file format", also used by Feather V2) in `dft`.

The file's schema can be inspected with one row per top-level field:

```sql
SELECT * FROM arrow_schema('my_arrow_file.arrow')
```

The custom key-value metadata stored in the file's footer can be read with:

```sql
SELECT * FROM arrow_metadata('my_arrow_file.arrow')
```

Details on each record batch block (file offset, metadata and body sizes, row count, and compression codec) can be inspected with:

```sql
SELECT * FROM arrow_batches('my_arrow_file.arrow')
```

The data from specific record batches can be read by passing either a single batch index or an inclusive start and end index (the indexes match the `batch_index` column of `arrow_batches`):

```sql
SELECT * FROM arrow_batch('my_arrow_file.arrow', 1)
SELECT * FROM arrow_batch('my_arrow_file.arrow', 1, 3)
```

Details on each dictionary block (dictionary id, delta flag, offset, sizes, and entry count) can be inspected with:

```sql
SELECT * FROM arrow_dictionaries('my_arrow_file.arrow')
```

And a single-row file summary (IPC metadata version, batch and dictionary counts, total rows, and total body bytes) can be read with:

```sql
SELECT * FROM arrow_file_metadata('my_arrow_file.arrow')
```

### WASM UDF Functions (`--features=udfs-wasm`)

Adds the ability to register WASM UDFs. Currently two different input types are supported:

1. Row => WASM native types only (`Int32`, `Int64`, `Float32`, or `Float64`) and the UDF is called once per row.
2. ArrowIpc => The input `ColumnarValue`'s are serialized to Arrow's IPC format and written to the WASM module's linear memory.

More details can be found in [datafusion-udfs-wasm](https://github.com/datafusion-contrib/datafusion-dft/tree/main/crates/datafusion-udfs-wasm).

```toml
[[execution]]
module_functions = {
    "/path/to/wasm" = [
        {
            name = "funcName1",
            input_data_type = "Row",
            input_types = [
                "Int32",
                "Int64",
            ],
            return_type = "Int32"
        },
        {
            name = "funcName2",
            input_data_type = "ArrowIpc",
            input_types = [
                "Float32",
                "Float64",
            ],
            return_type = "Int32"
        }

    ]
}
```

### WebSocket (`--features=websocket`)

Adds a `websocket` table function that connects to a WebSocket endpoint and streams received messages as rows with the schema (`received_at` Timestamp, `message` Utf8).  The first argument is the connection URL (`ws://` or `wss://`) and any remaining arguments are messages sent after the connection is established (for example, subscription messages).

```sql
SELECT * FROM websocket('wss://stream.example.com/ws', '{"op":"subscribe","channel":"trades"}') LIMIT 10
```

The source is unbounded: without a `LIMIT` (or an aggregation that can complete) the query streams until the server closes the connection.  Note the CLI `--concat` flag collects all batches and therefore should not be used with unbounded queries.

### Net (`--features=net`)

Adds table functions from [datafusion-net](https://github.com/datafusion-contrib/datafusion-dft/tree/main/crates/datafusion-net) for querying network packet captures with SQL, similar to wireshark / tshark.  Both functions share a wireshark-style schema (`timestamp`, `src_ip`, `dst_ip`, `protocol`, `src_port`, `dst_port`, `tcp_flags`, `payload`, etc.); frames that cannot be decoded produce rows with null columns rather than errors.

`pcap` reads a pcap/pcapng capture file as a table:

```sql
SELECT src_ip, dst_ip, protocol, length FROM pcap('capture.pcap') WHERE dst_port = 443
```

`capture` streams live-captured packets from a network interface, with an optional BPF filter and an optional duration in seconds:

```sql
-- Stream until 100 packets have been captured
SELECT * FROM capture('en0') LIMIT 100;

-- Capture for 10 seconds; the stream terminates, so aggregations work
SELECT src_ip, count(*) FROM capture('en0', 'tcp port 443', 10) GROUP BY src_ip;
```

Without a duration the live capture is unbounded: use a `LIMIT` or the query streams until cancelled.  Live capture requires elevated privileges (sudo, `cap_net_raw`+`cap_net_admin` on Linux, or ChmodBPF on macOS) and links against libpcap (`libpcap-dev` on Debian/Ubuntu; included with macOS).

`pcap_wide` and `capture_wide` take the same arguments as their narrow counterparts and append DNS and geolocation enrichment columns for the source and destination addresses: `src_host` / `dst_host` (reverse DNS) and `src_country`, `src_city`, `src_lat`, `src_lon` plus the `dst_` equivalents (resolved against the configured `geoip` database; without one the geolocation columns are `NULL`):

```sql
SELECT dst_ip, dst_host, dst_country, count(*) AS packets
FROM pcap_wide('capture.pcap') GROUP BY dst_ip, dst_host, dst_country ORDER BY packets DESC
```

`interfaces` lists the system's network capture interfaces (similar to `tshark -D`) with their addresses and status flags, which is useful for discovering what to pass to `capture`.  Listing does not require elevated privileges:

```sql
SELECT name, description, addresses FROM interfaces() WHERE is_up AND NOT is_loopback
```

`tcp_conversations` aggregates a capture file into one row per TCP connection (like wireshark's "Statistics → Conversations" or `tshark -z conv,tcp`), with per-direction packet/byte/retransmission counts, the handshake RTT, duration, and connection state (`active`, `half_closed`, `closed`, or `reset`).  Direction is from the connection initiator's perspective (`fwd` is initiator → responder):

```sql
SELECT dst_ip, dst_port, handshake_rtt_ms, retransmissions_fwd, state
FROM tcp_conversations('capture.pcap') ORDER BY retransmissions_fwd DESC
```

The feature also adds a `reverse_dns` scalar function that resolves an IP address string to a hostname via reverse DNS (PTR) lookup, which pairs naturally with the `src_ip` / `dst_ip` columns:

```sql
SELECT dst_ip, reverse_dns(dst_ip) AS host, count(*) AS packets
FROM capture('en0', 'tcp', 10) GROUP BY dst_ip, host ORDER BY packets DESC
```

Lookups are cached and bounded by a timeout; addresses that fail to parse, fail to resolve, or time out yield `NULL`.

There is also a `geoip` scalar function that geolocates an IP address using a MaxMind-format (`.mmdb`) database, returning a struct with `country_code`, `country`, `city`, `latitude`, `longitude`, `time_zone`, and `error` fields:

```sql
SELECT geoip(src_ip, '/path/GeoLite2-City.mmdb')['country_code'] AS country, count(*) AS packets
FROM pcap('capture.pcap') GROUP BY country ORDER BY packets DESC
```

A single database file serves every field — there is no per-field database.  A City-schema database ([GeoLite2-City](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data), free with a MaxMind account, or the commercial GeoIP2-City) populates all fields; a Country-schema database (GeoLite2-Country) populates only `country_code` and `country`, leaving the rest `NULL`; other schemas (ASN, ISP, ...) yield all-`NULL` fields.

The database path can also come from the `GEOIP_DB` environment variable or the `geoip_db_path` entry in the `[execution.net]` config section (the environment variable takes precedence), in which case the second argument can be omitted.

```toml
[execution.net]
geoip_db_path = "/path/to/GeoLite2-City.mmdb"
```

Addresses that fail to parse or have no entry in the database yield `NULL`.  A database that is missing, unreadable, or unconfigured does not fail the query: the location fields are `NULL` and the `error` field (`NULL` on success) carries the reason, e.g. `SELECT geoip(src_ip)['error']`.  The same applies to the geolocation columns of `pcap_wide` / `capture_wide`, which are `NULL` when the database is unavailable.

Two payload-decoding scalar functions parse application-layer details out of the `payload` column.  `dns_query(payload)` decodes a DNS message (typically UDP port 53) into a struct with `is_response`, `name`, `query_type`, `response_code`, and `answers` (a list of A/AAAA addresses and CNAME/NS/PTR names).  `tls_sni(payload)` extracts the SNI host name from a TLS ClientHello (typically TCP port 443) — the ClientHello is unencrypted even for HTTPS, so this reveals the destination host of otherwise opaque connections.  Both return `NULL` for payloads that do not parse:

```sql
SELECT tls_sni(payload) AS host, count(*) AS hellos
FROM pcap('capture.pcap') WHERE tls_sni(payload) IS NOT NULL GROUP BY host ORDER BY hellos DESC
```

## External Features

`dft` also has several external optional (conditionally compiled features) integrations which are controlled by [Rust Crate Features]

To build with all features, you can run 

```shell
cargo install --path . --all-features
````

[Rust Crate Features]: https://doc.rust-lang.org/cargo/reference/features.html


### S3 (`--features=s3`)

Mutliple s3 `ObjectStore`s can be registered, following the below model in your configuration file.

```toml
[[execution.object_store.s3]]
bucket_name = "my_bucket"
object_store_url = "s3://my_bucket"
aws_endpoint = "https://s3.amazonaws"
aws_access_key_id = "MY_ACCESS_KEY"
aws_secret_access_key = "MY SECRET"

[[execution.object_store.s3]]
bucket_name = "my_bucket"
object_store_url = "ny1://my_bucket"
aws_endpoint = "https://s3.amazonaws"
aws_access_key_id = "MY_ACCESS_KEY"
aws_secret_access_key = "MY SECRET"
```

Then you can run DDL such as 

```sql
CREATE EXTERNAL TABLE my_table STORED AS PARQUET LOCATION 's3://my_bucket/table';

CREATE EXTERNAL TABLE other_table STORED AS PARQUET LOCATION 'ny1://other_bucket/table';
```

### FlightSQL (`--features=flightsql`)

A separate editor for connecting to a FlightSQL server is provided.

The default `connection_url` is `http://localhost:50051` but this can be configured your config as well:

```toml
[flightsql]
connection_url = "http://myhost:myport"
```

### Deltalake (`--features=deltalake`)

Register deltalake tables.  For example:

```sql
CREATE EXTERNAL TABLE table_name STORED AS DELTATABLE LOCATION 's3://bucket/table'
```

### ClickHouse (`--features=clickhouse`)

Register an entire ClickHouse instance as a catalog (backed by [datafusion-table-providers]).  Each non-system database in the instance becomes a schema in the catalog and all of its tables are queryable.  For example use the following config:

```toml
[[execution.clickhouse]]
name = "clickhouse"                # catalog name to register (default "clickhouse")
url = "http://localhost:8123"
user = "admin"
password = "secret"
# database = "my_db"               # optionally limit the catalog to a single database
# compression = "lz4"              # transport compression: "lz4" or "none"

# Additional ClickHouse client settings applied to queries. The setting below returns
# ClickHouse String columns as Arrow Utf8 instead of Binary.
options = { output_format_arrow_string_as_string = "1" }
```

And then query with:

```sql
SELECT * FROM clickhouse.my_db.my_table
```

Multiple `[[execution.clickhouse]]` entries can be defined, each registered as its own catalog.  Databases and table names are discovered once at startup; the data itself is queried from ClickHouse on demand.

### MongoDB (`--features=mongodb`)

Register an entire MongoDB instance as a catalog (backed by [datafusion-table-providers]).  Each non-system database in the instance becomes a schema in the catalog and its collections are queryable as tables.  For example use the following config:

```toml
[[execution.mongodb]]
name = "mongodb"                   # catalog name to register (default "mongodb")
host = "localhost"
port = 27017
user = "admin"
password = "secret"
# database = "my_db"               # optionally limit the catalog to a single database

# Alternatively provide a full connection string (takes precedence over the fields above and
# limits the catalog to the database in the connection string)
# connection_string = "mongodb://admin:secret@localhost:27017/my_db?authSource=admin"

# Additional connection parameters passed to the underlying pool.  Note that TLS is
# required by default; use sslmode = "disabled" for local instances without TLS.
options = { auth_source = "admin", sslmode = "disabled" }
```

And then query with:

```sql
SELECT * FROM mongodb.my_db.my_collection
```

Multiple `[[execution.mongodb]]` entries can be defined, each registered as its own catalog.  Databases and collection names are discovered once at startup; the data itself is queried from MongoDB on demand.

Because MongoDB is schemaless, the Arrow schema of a collection is inferred by sampling documents (400 by default, configurable with `options = { schema_infer_max_records = "1000" }`) the first time the collection is used in a query.  Nested documents can be flattened into top-level columns with `options = { unnest_depth = "2" }`.

[datafusion-table-providers]: https://github.com/datafusion-contrib/datafusion-table-providers

### Json Functions (`--features=function-json`)

Adds functions from [datafusion-function-json] for querying JSON strings in DataFusion in `dft`.  For example:

```sql
select * from foo where json_get(attributes, 'bar')::string='ham'
(show examples of using operators too)
```

[datafusion-function-json]: https://github.com/datafusion-contrib/datafusion-functions-json
[datafusion-functions-arrow]: https://github.com/datafusion-contrib/datafusion-dft/tree/main/crates/datafusion-functions-arrow

### HuggingFace (`--features=huggingface`)

Register tables from HuggingFace datasets.  For example use the following config:

```toml
[[execution.object_store.huggingface]]
repo_type = "dataset"
repo_id = "HuggingFaceTB/finemath"
revision = "main"
```

and then you can register external tables like so:

```sql
CREATE EXTERNAL TABLE hf4 STORED AS PARQUET LOCATION 'hf://HuggingFaceTB-finemath/finemath-3plus/';
```

The "/" in the `repo_id` is replaced with a "-" for the base url that is registered with DataFusion to work better with its path parsing.



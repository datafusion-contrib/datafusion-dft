# DataFusion RocksDB

DataFusion table functions for inspecting RocksDB databases. Databases are opened read-only (the LOCK file is never taken), so a database that another process has open read-write can be inspected safely. Data still in the WAL is not visible to a read-only handle, so memtable-related metrics read 0 and key estimates exclude unflushed writes.

## Functions

- `rocksdb_metadata(path)` — a single summary row: column families, latest sequence number, live SST file count and total size, estimated key count, snapshot count, and MANIFEST / WAL file details.
- `rocksdb_sstables(path [, cf])` — one row per live SST file: column family, file name, LSM level, size, entry and deletion counts, and key range (as lossless hex and lossy UTF-8).
- `rocksdb_cf_metrics(path [, cf])` — one row per column family and RocksDB property (long format): key counts, SST / memtable sizes, compaction state, block cache usage, and per-level file counts.

## Example

```sql
SELECT * FROM rocksdb_metadata('/path/to/db');
SELECT column_family, file_name, level, size_bytes, num_entries FROM rocksdb_sstables('/path/to/db');
SELECT * FROM rocksdb_cf_metrics('/path/to/db', 'default') WHERE property = 'rocksdb.estimate-num-keys';
```

## Limitations

- The Rust `rocksdb` binding does not expose `SstFileReader` or per-file table properties, so per-SST compression and block statistics are not available.
- SST sequence number ranges (`smallest_seqno` / `largest_seqno`) are not exposed by the binding's `live_files()` and are therefore not included.

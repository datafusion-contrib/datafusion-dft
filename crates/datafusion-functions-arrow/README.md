# DataFusion Functions Arrow

DataFusion table functions for inspecting Arrow IPC files (the Arrow "file format", also used by Feather V2).

## Functions

- `arrow_schema(path)` — one row per top-level field in the file's schema: field name, data type, nullability, and field-level metadata.
- `arrow_metadata(path)` — one row per custom key-value metadata entry stored in the file footer.
- `arrow_batches(path)` — one row per record batch block: file offset, metadata and body sizes, row count, and compression codec.
- `arrow_dictionaries(path)` — one row per dictionary block: dictionary id, whether it is a delta, file offset, sizes, and entry count.
- `arrow_file_metadata(path)` — a single summary row: IPC metadata version, batch and dictionary counts, total rows, and total body bytes.

## Example

```sql
SELECT * FROM arrow_file_metadata('data.arrow');
SELECT * FROM arrow_batches('data.arrow');
```

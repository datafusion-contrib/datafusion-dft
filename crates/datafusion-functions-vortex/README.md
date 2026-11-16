# DataFusion Vortex Functions

DataFusion table functions for inspecting [Vortex](https://vortex.dev) files: file metadata, schema, file-level statistics, the layout tree, zone maps, segments, array encodings, compression, user-defined metadata, dictionaries, and row ranges.

Only the file footer is read unless a function needs segment data. The functions take a local file path.

## Functions

| Function | Grain | Reads |
|---|---|---|
| `vortex_file_metadata(file)` | one summary row | footer |
| `vortex_schema(file)` | one row per DType node | footer |
| `vortex_field_stats(file)` | one row per top-level field | footer |
| `vortex_layout(file)` | one row per layout node | footer |
| `vortex_zone_map(file [, column])` | one row per zone, column, and aggregate | footer and zone map segments |
| `vortex_segments(file)` | one row per segment | footer |
| `vortex_encodings(file [, column])` | one row per array encoding node | footer and every selected data segment |
| `vortex_compression(file)` | one row per top-level field | whole file |
| `vortex_kv_metadata(file)` | one row per metadata segment | footer and metadata segments |
| `vortex_dictionary(file, column)` | one row per dictionary value | footer and dictionary value segments |
| `vortex_rows(file, start [, end])` | data rows in `[start, end)` | segments of the selected rows |

- `node_id` is the depth-first pre-order index of a layout node. It is the same in every function, so the results can be joined.
- `role` is the purpose of a layout node: `data`, `zone_map`, `dict_values`, or `validity`.
- Statistics, zone map values, and dictionary values are formatted as strings.
- `vortex_zone_map` uses long format (`aggregate`, `value`), because the set of aggregates depends on the column type and the writer.

## Example

```sql
SELECT * FROM vortex_file_metadata('data.vortex');
SELECT node_id, parent_id, path, encoding, row_count, zone_len FROM vortex_layout('data.vortex');
SELECT zone_id, first_row, num_rows, aggregate, value FROM vortex_zone_map('data.vortex', 'id');
SELECT path, encoding, count(*), sum(buffer_bytes) FROM vortex_encodings('data.vortex') GROUP BY 1, 2;
SELECT field_name, compressed_size, uncompressed_size, compression_ratio FROM vortex_compression('data.vortex');
SELECT * FROM vortex_rows('data.vortex', 100, 200);
```

## Limitations

- Only local file paths are supported. Object store URLs are not.
- `vortex_dictionary` returns layout-level dictionaries (`vortex.dict` layouts). Dictionary encoding inside a flat segment appears in `vortex_encodings` instead.
- The functions depend on Vortex layout internals and track the pinned Vortex version.

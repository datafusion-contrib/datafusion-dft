# Database Guide

`dft` uses a configured database path to load tables into the DataFusion `SessionContext` for querying.

```
[db]
path = /path/to/db
```

Within this path it will look for a `tables` directory and then the expected path structure is `{catalog_name}/{schema_name}/{table_name}` where table name must be a directory where the tables data files are stored.

For example, after generating TPC-H data into the `dft` catalog and `tpch` schema the following paths are in the database path:

```
/path/to/db/tables/dft/tpch/customers/
/path/to/db/tables/dft/tpch/line_items/
/path/to/db/tables/dft/tpch/nations/
/path/to/db/tables/dft/tpch/orders/
/path/to/db/tables/dft/tpch/part_supps/
/path/to/db/tables/dft/tpch/parts/
/path/to/db/tables/dft/tpch/regions/
/path/to/db/tables/dft/tpch/suppliers/
```

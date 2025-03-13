# Features

## Internal Optional Features (Workspace Features)

`dft` incubates several optional features in it's `crates` directory.  This provides us with the ability to quickly iterate on new features and test them in the main application while at the same time making it easy to export them to their own crates when they are ready.

### Parquet Functions (`--features=functions-parquet`)

Includes functions from [datafusion-function-parquet] for querying Parquet files in DataFusion in `dft`.  For example:

```sql
SELECT * FROM parquet_metadata('my_parquet_file.parquet')
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

### Iceberg (`--features=iceberg`)

Register iceberg tables.  For example:

```sql
CREATE EXTERNAL TABLE table_name STORED AS ICEBERG LOCATION 's3://bucket/table'
```

Register Iceberg Rest Catalog

```toml
[[execution.iceberg.rest_catalog]]
name = "my_iceberg_catalog"
addr = "192.168.1.1:8181"
```


### Json Functions (`--features=function-json`)

Adds functions from [datafusion-function-json] for querying JSON strings in DataFusion in `dft`.  For example:

```sql
select * from foo where json_get(attributes, 'bar')::string='ham'
(show examples of using operators too)
```

[datafusion-function-json]: https://github.com/datafusion-contrib/datafusion-functions-json

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



# dft - Batteries included DataFusion

## 🚧 DOCS UNDER CONSTRUCTION
Documentation is undergoing a significant revamp - the new documentation will be finalized as part of the v0.3 release in the late Spring or early Summer of 2025.

## Overview

`dft` is a batteries included suite of a [DataFusion](https://github.com/apache/arrow-datafusion) applications. The batteries being several common features to modern query execution engines such as:

- Query files from S3 or HuggingFace datasets
- Support for common table formats (Deltalake, Iceberg, Hudi)
- UDFs defined in multiple languages (WASM and soon Python)
- Popular helper functions (for example for working with JSON and Parquet data)

It provides two client interfaces to the query execution engine:
1. Text User Interface (TUI): An IDE for DataFusion developers and users that provides a local database experience with utilities to analyze / benchmark queries.
2. Command Line Interface (CLI): Scriptable engine for executing queries from files.

And two server implementation, FlightSQL & HTTP, leveraging the same execution engine behind the TUI and CLI.  This allows users to iterate and quickly develop a database then seamlessly deploy applications built on it.

`dft` is inspired by  [`datafusion-cli`], but has some differences:
1. The TUI focuses on more complete and interactive experience for users.
2. It contains many built in integrations such as Delta Lake and Iceberg that are not available in `datafusion-cli`.
3. It provides FlightSQL and HTTP server implementations to make it easy to deploy DataFusion based applications / backends.

[`datafusion-cli`]: https://datafusion.apache.org/user-guide/cli/overview.html

## User Guide

### Installation

Currently, the only supported packaging is on [crates.io](https://crates.io/search?q=datafusion-dft).  If you already have Rust installed it can be installed by running `cargo install datafusion-dft`.  If rust is not installed you can download following the directions [here](https://www.rust-lang.org/tools/install).

### Running the apps

The command for each of the apps are:

```sh
# TUI (enabled by default)
dft

# Execute command with CLI (enabled by default)
dft -c "SELECT 1"

# Execute SQL from file (enabled by default)
dft -f query.sql

# Start FlightSQL Server (requires `flightsql` feature)
dft serve-flight-sql

# Start HTTP Server (requires `http` feature)
dft serve-http
```

### DDL

The CLI can also run your configured DDL prior to executing the query by adding the `--run-ddl` parameter.

To have the best experience with `dft` it is highly recommended to define all of your DDL in `~/.config/ddl.sql` so that any tables you wish to query are available at startup.  Additionally, now that DataFusion supports `CREATE VIEW` via sql you can also make a `VIEW` based on these tables.

For example, your DDL file could look like the following:

```
CREATE EXTERNAL TABLE users STORED AS NDJSON LOCATION 's3://bucket/users';

CREATE EXTERNAL TABLE transactions STORED AS PARQUET LOCATION 's3://bucket/transactions';

CREATE EXTERNAL TABLE listings STORED AS PARQUET LOCATION 'file://folder/listings';

CREATE VIEW OR REPLACE users_listings AS SELECT * FROM users LEFT JOIN listings USING (user_id);
```

This would make the tables `users`, `transactions`, `listings`, and the view  `users_listings` available at startup.  Any of these DDL statements could also be run interactively from the SQL editor as well to create the tables.

# Additional Documentation

Links to more detailed documentation for each of the apps and all of the features can be found below.

- [Features](docs/features.md)
- [CLI Docs](docs/cli.md)
- [TUI Docs](docs/tui.md)
- [FlightSQL Server Docs](docs/flightsql_server.md)
- [HTTP Server Docs](docs/http_server.md)
- [Config Reference](docs/config.md)

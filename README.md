# dft

`dft` provides two interfaces to the [DataFusion](https://github.com/apache/arrow-datafusion) query execution engine:
1. Text User Interface (TUI): An extensible terminal based data analysis tool that allows users to query and join data from disparate data sources.
2. Command Line Interface (CLI): Scriptable engine for executing queries from files.

`dft` is inspired by  [`datafusion-cli`], but has some differences:
1. `dft` TUI focuses on more complete and interactive experience for users.
2. `dft` contains many built in integrations such as Delta Lake, Iceberg, and MySQL (Coming Soon) that are not available in `datafusion-cli`.

[`datafusion-cli`]: https://datafusion.apache.org/user-guide/cli/overview.html

## `dft` TUI

The objective of `dft` is to provide users with the experience of having their own local database that allows them to query and join data from disparate data sources all from the terminal.  

<table width="100%">
    <tr>
        <th>SQL & FlightSQL Editor and Results</th>
        <th>Query History and Stats</th>
    </tr>
    <tr>
        <td width="50%">
            <img width="1728" alt="image" src="https://github.com/user-attachments/assets/f9af22c2-665b-487b-bd8c-d714fc7c65d4">
        </td>
        <td width="50%">
            <img width="1728" alt="image" src="https://github.com/user-attachments/assets/e05a8ff3-17e8-4663-a297-5bb4ab19fdc7">
        </td>
    </tr>
    <tr>
        <th>Filterable Logs</th>
        <th>DataFusion Session Context Details</th>
    </tr>
    <tr>
        <td width="50%">
            <img width="1728" alt="image" src="https://github.com/user-attachments/assets/94861585-e1ca-481d-9b46-c1ced4976b9a">
        </td>
        <td width="50%">
            <img width="1728" alt="image" src="https://github.com/user-attachments/assets/0f272db1-3432-4dd7-9fb6-2e438db1b268">
        </td>
    </tr>
</table>

Some of the current and planned features are:

- Tab management to provide clean and structured organization of DataFusion queries, results, and context
  - SQL editor
    - Write query results to file (TODO)
    - Multiple SQL Editor tabs (TODO)
  - Query history
    - History and statistics of executed queries
  - ExecutionContext information
    - Information from ExecutionContext / Catalog / ObjectStore / State / Config
  - Logs
    - Logs from `dft` and `DataFusion`
- Custom `ObjectStore` Support
  - S3, Azure(TODO), GCP(TODO)
  - `ObjectStore` explorer. I.e. able to list files in `ObjectStore`
- `TableProviderFactory` data sources
  - Deltalake
  - Iceberg (TODO)
  - Hudi (TODO)
- Preloading DDL from `~/.datafusion/.datafusionrc` for local database available on startup
- "Catalog File" support - see [#122](https://github.com/datafusion-contrib/datafusion-tui/issues/122)
  - Save table definitions *and* data
  - Save parquet metadata from remote object stores

  
## `dft` CLI

The `dft` CLI is a scriptable interface to the `tui` engine for executing
queries from files or the command line. The CLI is used in a similar manner to
`datafusion-cli` but with the added benefit of supporting multiple pre-integrated
data sources.

### Example: Run the contents of `query.sql` 

```shell
$ dft -f query.sql
```

### Example: Run a query from the command line

```shell
$ dft -c "SELECT 1+2"
```


## User Guide

### Installation

Currently, the only supported packaging is on [crates.io](https://crates.io/search?q=datafusion-tui).  If you already have Rust installed it can be installed by running `cargo install datafusion-tui`.  If rust is not installed you can download following the directions [here](https://www.rust-lang.org/tools/install).

Once installed you can run `dft` to start the application.

#### Features

#### S3 (`--features=s3`)

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

#### FlightSQL (`--features=flightsql`)

A separate editor for connecting to a FlightSQL server is provided.

The default `connection_url` is `http://localhost:50051` but this can be configured your config as well:

```toml
[execution.flight_sql]
connection_url = "http://myhost:myport"
```

#### Deltalake (`--features=deltalake`)

Register deltalake tables.  For example:

```sql
CREATE EXTERNAL TABLE table_name STORED AS DELTATABLE LOCATION 's3://bucket/table'
```

### Config

The `dft` configuration is stored in `~/.config/dft/config.toml`

### Getting Started

To have the best experience with `dft` it is highly recommended to define all of your DDL in `~/.datafusion/.datafusionrc` so that any tables you wish to query are available at startup.  Additionally, now that DataFusion supports `CREATE VIEW` via sql you can also make a `VIEW` based on these tables.

For example, your `~/.datafusion/.datafusionrc` file could look like the following:

```
CREATE EXTERNAL TABLE users STORED AS NDJSON LOCATION 's3://bucket/users';

CREATE EXTERNAL TABLE transactions STORED AS PARQUET LOCATION 's3://bucket/transactions';

CREATE EXTERNAL TABLE listings STORED AS PARQUET LOCATION 'file://folder/listings';

CREATE VIEW OR REPLACE users_listings AS SELECT * FROM users LEFT JOIN listings USING (user_id);
```

This would make the tables `users`, `transactions`, `listings`, and the view  `users_listings` available at startup.  Any of these DDL statements could also be run interactively from the SQL editor as well to create the tables.

### Key Mappings

The interface is split into several tabs so that relevant information can be viewed and controlled in a clean and organized manner. When not writing a SQL query keys can be entered to navigate and control the interface.

- SQL & FlightSQL Editor: where queries are entered and results can be viewed.  Drawing inspiration from vim there are multiple modes.
  - Normal mode
    - `q` => quit datafusion-tui
    - `e` => start editing SQL Editor in Edit mode
    - `c` => clear contents of SQL Editor
    - `Enter` => execute query
    - Enter the tab number in brackets after a tabs name to navigate to that tab
    - If query results are longer or wider than screen, you can use arrow keys to scroll
  - Edit mode
    - Character keys to write queries
    - Backspace / tab / enter work same as normal
    - `esc` to exit Edit mode and go back to Normal mode
  - Rc mode
    - `l` => load `~/.datafusion/.datafusionrc` into editor (TODO)
    - `r` => rerun `~/.datafusion/.datafusionrc` (TODO)
    - `w` => write editor contents to `~/.datafusion/.datafusionrc` (TODO)
  - Logging mode (coming from [tui_logger](https://docs.rs/tui-logger/latest/tui_logger/index.html))
    - `h` => Toggles target selector widget hidden/visible
    - `f` => Toggle focus on the selected target only
    - `UP` => Select previous target in target selector widget
    - `DOWN` => Select next target in target selector widget
    - `LEFT` => Reduce SHOWN (!) log messages by one level
    - `RIGHT` => Increase SHOWN (!) log messages by one level
    - `-` => Reduce CAPTURED (!) log messages by one level
    - `+` => Increase CAPTURED (!) log messages by one level
    - `PAGEUP` => Enter Page Mode and scroll approx. half page up in log history.
    - `PAGEDOWN` => Only in page mode: scroll 10 events down in log history.
    - `ESCAPE` => Exit page mode and go back to scrolling mode
    - `SPACE` => Toggles hiding of targets, which have logfilter set to off

# datafusion-tui (dft)

DataFusion-tui provides a feature rich terminal application, built with [tui-rs](https://github.com/fdehau/tui-rs), for using DataFusion (and eventually Ballista). It has drawn inspiration and several features from `datafusion-cli`. In contrast to `datafusion-cli` the objective of this tool is to provide a light SQL IDE experience for querying data with DataFusion. It is currently in early stages of development and as such there are likely to be bugs.

https://user-images.githubusercontent.com/622789/161690194-c7c1e1b0-e432-43ab-9e44-f7673868b9cb.mp4

Some of the current and planned features are listed here:

- Tab management to provide clean and structured organization of DataFusion queries, results, and context
  - SQL editor
    - Text editor for writing SQL queries
    - Scrollable query results
    - Track memory usage during query (TODO)
    - Write query results to file (TODO)
    - Multiple SQL Editor tabs (TODO)
  - Query history
    - History of executed queries
  - ExecutionContext information
    - Information from ExecutionContext / Catalog / ObjectStore / State / Config
  - Logs
    - Logs from `dft` and `DataFusion`
  - Help (TODO)
    - Documentation on functions / commands
- Custom ObjectStore Support
  - S3 with AWS default credentials
  - S3 with custom endpoint / provider (i.e. MinIO)
  - HDFS (TODO)
  - `ObjectStore` explorer. I.e. able to list files in `ObjectStore`
- Custom Table Providers (if supported in SQL by DataFusion)
  - Delta Table (TODO)
  - Big Table
- Preloading DDL from `~/.datafusion/.datafusionrc` for local database available on startup

## User Guide

The interface is split into several tabs so that relevant information can be viewed and controlled in a clean and organized manner. When not writing a SQL query keys can be entered to navigate and control the interface.

- SQL Editor: where queries are entered and results can be viewed
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
    - `l` load `~/.datafusion/.datafusionrc` into editor
    - `r` rerun `~/.datafusion/.datafusionrc`
    - `w` write editor contents to `~/.datafusion/.datafusionrc`
- Register custom `ObjectStore`
  - S3: run / install with `--features=s3`
    - If you want to use your default AWS credentials, then no further action is required. For example your credentials in `~/.aws/credentials` will automatically be picked up.
    - If you want to use a custom S3 provider, such as MinIO, then you must create a `s3.json` configuration file in `~/.datafusion/object_stores/` with the fields `endpoint`, `access_key_id`, and `secret_access_key`.

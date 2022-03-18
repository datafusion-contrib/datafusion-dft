# datafusion-tui (dft)

DataFusion-tui provides a feature rich terminal application, built with [tui-rs](https://github.com/fdehau/tui-rs), for using DataFusion (and eventually Ballista).  It has drawn inspiration and several features from `datafusion-cli`.  In contrast to `datafusion-cli` the objective of this tool is to provide a light SQL IDE experience for querying data with DataFusion. It is currently in early stages of development and as such there are likely to be bugs.

https://user-images.githubusercontent.com/22136083/158945369-77708c59-c2bc-4580-b449-0b593e6ed18f.mov

Some of the current and planned features are listed here:
- Tab management to provide clean and structured organization of DataFusion queries, results, and context
  - SQL editor
    - Text editor for writing SQL queries
    - Scrollable query results
    - Write query results to file (TODO)
    - Multiple SQL Editor tabs (TODO)
  - Query history
    - History of executed queries
  - ExecutionContext information (TODO)
    - Information from ExecutionContext / Catalog / ObjectStore / State / Config
  - Logs
    - Logs from `dft` and `DataFusion`
  - Help (TODO)
    - Documentation on functions / commands
- Custom ObjectStore Support
  - S3
  - HDFS (TODO)
- Custom Table Providers (if supported in SQL by DataFusion)
  - Delta Table (TODO)
  - Big Table  (TODO)
- Preloading DDL from `~/.datafusionrc` for local database available on startup

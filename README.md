# datafusion-tui (dft)

DataFusion-tui provides a feature rich terminal application for using DataFusion.  It has drawn inspiration and several features from `datafusion-cli`.  In contrast to `datafusion-cli` the objective of this tool is to provide a light SQL IDE experience for querying data with DataFusion.  This includes features such as the following:

- Tab management to provide clean and structured organization of DataFusion queries, results, and context
  - Tabs
  - SQL Editor
    - Text editor for writing SQL queries
    - Scrollable query results
    - Write query results to file (TODO)
    - Multiple SQL Editor tabs (TODO)
  - Query History
    - Full history of all executed queries
  - Context (TODO)
    - Information from ExecutionContext / Catalog / ObjectStore / State / Config
  - Logs
    - Logs from `dft` and `DataFusion`
  - Help (TODO)
    - Documentation on functions / commands
- Custom ObjectStore Support (TODO)
  - S3 (TODO)
- Preloading DDL from `~/.datafusionrc`

![Alt text](media/screenshot.jpeg?raw=true "DataFusion-tui")

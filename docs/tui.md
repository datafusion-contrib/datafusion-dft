# TUI Guide

The `dft` TUI (Text User Interface) provides an IDE-like experience for querying DataFusion in a local "database-like" environment. It integrates query execution, analysis, result browsing, and benchmarking in a single terminal interface.

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

## Getting Started

Start the TUI with a simple command:

```bash
# Basic launch
dft

# With specific configuration file
dft --config path/to/config.toml

# Load tables and views from DDL file
dft --run-ddl
```

## Interface Overview

The TUI is organized into several tabs that you can navigate between:

### 1. SQL Editor Tab (Default)
- Write and execute SQL directly against a local DataFusion context
- View query results with automatic pagination
- Optimize queries with real-time execution statistics

### 2. FlightSQL Tab
- Connect to and query remote FlightSQL servers 
- Same interface as the SQL editor but sends queries to a FlightSQL server
- Configure connection details in your config file

### 3. Query History Tab
- Review previously executed queries
- See execution statistics and performance metrics
- Re-run previous queries with a single keystroke

### 4. Context Tab
- Explore available tables, views and columns
- View registered functions and their signatures
- Examine catalog, schema, and table metadata

### 5. Logs Tab
- Filter logs by source and level
- Search log messages for specific text
- Monitor DataFusion's internal operations in real-time

## Key Features

### Data Exploration
- **Automatic DDL Loading**: Tables defined in your DDL file are available immediately
- **Rich Metadata**: Explore table schemas, column types, and statistics
- **Result Pagination**: Browse through large result sets efficiently

### Query Development
- **Syntax Highlighting**: Color-coded SQL for better readability
- **Editor History**: Navigate through your command history
- **Multiple Query Editor Modes**: SQL and Flight SQL in the same interface

### Performance Analysis
- **Query Benchmarking**: Measure execution times across multiple runs
- **Execution Statistics**: See detailed breakdowns of query component performance
- **Resource Utilization**: Monitor memory usage during query execution (TODO)

### Integration Support
- **Object Store Support**: Query S3, local files, and more
- **Table Formats**: Connect to Delta Lake tables
- **Extension System**: Load custom UDFs and table providers

## Limitations

- **Horizontal Scrolling**: The underlying terminal widget does not support horizontal scrolling well. For wide data sets, consider selecting only the columns you need until improved scrolling support arrives.

## Key Mappings

The interface is split into several tabs and modes so that relevant information can be viewed and controlled in a clean and organized manner. When not writing a SQL query keys can be entered to navigate and control the interface.

#### SQL Tab

Editor for executing SQL with local DataFusion `SessionContext`.

- Normal mode
    - Not editable
        - `q` => quit datafusion-tui
        - `e` => start editing SQL Editor in Edit mode
        - `c` => clear contents of SQL Editor
        - `Enter` => execute query
        - Enter the tab number in brackets after a tabs name to navigate to that tab
        - If query results are longer or wider than screen, you can use arrow keys to scroll
    - Editable
        - Character keys to write queries
        - Backspace / tab / enter work same as normal
        - `Shift` + Up/Down/Left/Right => Select text
        - `Alt` + `Enter` => execute query
        - `esc` to exit Edit mode and go back to Normal mode
- DDL mode
    - Not editable
        - `l` => load configured DDL file into editor
        - `enter` => rerun configured DDL file
        - `s` => write editor contents to configured DDL file
    - Editable
        - Character keys to write queries
        - Backspace / tab / enter work same as normal
        - `Shift` + Up/Down/Left/Right => Select text
        - `Alt` + `Enter` => execute query
        - `esc` to exit Edit mode and go back to Normal mode

#### FlightSQL Tab

Same interface as SQL tab but sends SQL queries to FlightSQL server.

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
    - `Shift` + Up/Down/Left/Right => Select text
    - `Alt` + `Enter` => execute query
    - `esc` to exit Edit mode and go back to Normal mode

#### History Tab

- Review previously executed queries with their execution times
- Re-run queries by selecting them and pressing Enter
- Filter and search through query history

#### Logs Tab

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

## Configuration

### Editor Settings

Enable syntax highlighting for SQL in your config file:

```toml
[tui.editor]
experimental_syntax_highlighting = true
```

### Display Settings

Configure the TUI's frame rate:

```toml
[tui.display]
frame_rate = 60  # Default is 60
```

### Performance Settings

Adjust batch size for result pagination:

```toml
[tui.execution.datafusion]
execution.batch_size = 100  # Default; smaller values may improve performance
```

# dft TUI

The `dft` TUI (Text User Interface) is an IDE-like experience that allows you to query DataFusion in a local “database-like” environment. It includes built-in utilities for analyzing queries, exploring catalogs, benchmarking, and more.

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

## Key Features of the TUI

- **Multiple Tabs** for organizing queries and results:
  - **SQL Editor** – execute queries locally or (optionally) against FlightSQL.
  - **Query History** – quickly review previously run queries.
  - **Execution Context** – inspect DataFusion session details.
  - **Logs** – filter logs from `dft` or DataFusion in real time.
- **Custom ObjectStore Support** – easily query data from AWS S3, Azure (TODO), GCP (TODO), or local file systems.
- **TableProviderFactory Integrations** – Delta Lake, Iceberg, Hudi, etc.
- **Preloading DDL** – automatically run your configured DDL on startup, giving a “local DB” feel.
- **Benchmark & Analyze** – quickly measure and analyze query performance.

---

## Limitations

- **Horizontal Scrolling**: The underlying terminal widget does not support horizontal scrolling well. For wide data sets, consider selecting only the columns you need until improved scrolling support arrives.

---

## Starting the TUI

Once you have installed `dft`, simply run:

```bash
dft
```

This will launch the TUI in your terminal.

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

TODO

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

## Editor Settings

```toml
[tui.editor]
experimental_syntax_highlighting = true
```

# Config Reference

`dft` is configuration through a TOML file with default location of `~/.config/dft/config.toml`.  Within the config there is a shared configuration that can be used across all apps and app specific configuration.  The shared and app specific configs are merged to come up with a final configuration that is used.  DataFusion's execution configuration can be fully customized as part of this.

The sections for configuring each app are shown below.
```toml
[shared]

[cli]

[tui]

[flightsql_client]

[flightsql_server]

```

## Execution Config

The execution config is where you can define query execution properties for each app (so the below would each expect to be in a relevant app section like `shared`, `tui`, `cli`, or `flightsql_server` (The FlightSQL client doesnt actually execute so doesnt have an execution config).  You can configure the `ObjectStore`s that you want to use in your queries and path of a DDL file that you want to run on startup.  For example, if you have an S3 bucket you want to query you could define it like so:

```toml
[[execution.object_store.s3]]
bucket_name = "my_bucket"
object_store_url = "s3://my_bucket"
aws_endpoint = "https://s3.amazonaws"
aws_access_key_id = "MY_ACCESS_KEY"
aws_secret = "MY SECRET"
aws_session_token = "MY_SESSION"
aws_allow_http = false
```

And define a custom DDL path like so (the default is `~/.config/dft/ddl.sql`).

```toml
[execution]
ddl_path = "/path/to/my/ddl.sql"
```

Multiple `ObjectStore`s can be defined in the config file. In the future datafusion `SessionContext` and `SessionState` options can be configured here.

Set the number of iterations for benchmarking queries (10 is the default).

```toml
[execution]
benchmark_iterations = 10
```

The batch size for query execution can be configured based on the app being used (TUI, CLI, or FlightSQL Server). For the TUI it defaults to 100, which may slow down queries, because a Record Batch is used as a unit of pagination and too many rows can cause the TUI to hang. For the CLI and FlightSQL Server, the default is 8092.

```toml
[execution]
cli_batch_size = 8092
tui_batch_size = 100
flightsql_server_batch_size = 8092
```

## Display Config

The display config is where you can define the frame rate of the TUI.

```toml
[tui.display]
frame_rate = 60
```

## Interaction Config

The interaction config is where mouse and paste behavior can be defined.  This is not currently implemented.

```toml
[tui.interaction]
mouse = true
paste = true
```

## FlightSQL Config

The FlightSQL config is where you can define the connection URL for the FlightSQL client & server.

```toml
[flightsql_client]
connection_url = "http://localhost:50051"
benchmark_iterations = 10

[flightsql_server]
connection_url = "http://localhost:50051"
server_metrics_port = "0.0.0.0:9000"
```

## Editor Config

The editor config is where you can set your preferred editor settings.

Currently only syntax highlighting is supported.  It is experimental because currently the regex that is used to determine keywords only works in simple cases.

```toml
[tui.editor]
experimental_syntax_highlighting = true
```


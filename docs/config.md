# Configuration Reference

`dft` is configured through a TOML file located at `~/.config/dft/config.toml` by default. The configuration system uses a hierarchical approach:

1. **Default values** built into the application
2. **Shared configuration** applied to all interfaces
3. **App-specific configuration** for each interface (TUI, CLI, etc.)

The final configuration for each interface is a merge of these three layers, with app-specific settings taking precedence over shared settings.

## Configuration Structure

```toml
# Settings applied to all interfaces
[shared]
# ...shared settings...

# CLI-specific settings
[cli]
# ...CLI settings...

# TUI-specific settings
[tui]
# ...TUI settings...

# FlightSQL client settings
[flightsql_client]
# ...FlightSQL client settings...

# FlightSQL server settings
[flightsql_server]
# ...FlightSQL server settings...

# HTTP server settings
[http_server]
# ...HTTP server settings...
```

You can specify a different config file with the `--config` parameter:

```bash
dft --config /path/to/custom/config.toml
```

## Execution Config

The execution config is where you can define query execution properties for each app (so the below would each expect to be in a relevant app section like `shared`, `tui`, `cli`, or `flightsql_server` (The FlightSQL client doesnt actually execute so doesnt have an execution config).  You can configure the `ObjectStore`s that you want to use in your queries and path of a DDL file that you want to run on startup.

### S3 Object Store Configuration

There are multiple ways to configure S3 credentials:

#### Option 1: Static Credentials (Existing behavior)

```toml
[[execution.object_store.s3]]
bucket_name = "my_bucket"
object_store_url = "s3://my_bucket"
aws_endpoint = "https://s3.amazonaws.com"
aws_access_key_id = "MY_ACCESS_KEY"
aws_secret_access_key = "MY_SECRET"
aws_session_token = "MY_SESSION"
aws_allow_http = false
```

#### Option 2: AWS Credential Chain (New)

Enable automatic credential resolution from environment variables, `~/.aws/credentials`, IAM roles, or EKS service accounts:

```toml
[[execution.object_store.s3]]
bucket_name = "my_bucket"
object_store_url = "s3://my_bucket"
use_credential_chain = true
aws_allow_http = false
```

When `use_credential_chain = true`, credentials are resolved in this order:
1. TOML static credentials (if provided, override environment)
2. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables
3. `AWS_WEB_IDENTITY_TOKEN_FILE` (for EKS/IRSA)
4. ECS container credentials
5. EC2 instance profile (via IMDSv2)

#### Option 3: Hybrid Approach

Use credential chain but override specific settings like endpoint:

```toml
[[execution.object_store.s3]]
bucket_name = "my_bucket"
object_store_url = "s3://my_bucket"
use_credential_chain = true
aws_endpoint = "https://custom-s3.example.com"
aws_allow_http = false
```

**Security Note:** Credential chain is opt-in via the `use_credential_chain` flag. When false (default), only TOML credentials are used, preventing accidental exposure of unintended AWS accounts

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
server_metrics_addr = "0.0.0.0:9000"
```

## Editor Config

The editor config is where you can set your preferred editor settings.

Currently only syntax highlighting is supported.  It is experimental because currently the regex that is used to determine keywords only works in simple cases.

```toml
[tui.editor]
experimental_syntax_highlighting = true
```


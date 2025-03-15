# NOTES
# We use shebang in places where multi line constructs are needed
# Ref: https://github.com/casey/just?tab=readme-ov-file#if-statements

# Installs `dft` dev tools. Assumes you already have `cargo` installed
install-tools:
    cargo install oha

# Starts a debug HTTP server
serve-http:
    RUST_LOG=info cargo r --features=http -- serve-http   

# Starts a debug FlightSQL server
serve-flight-sql:
    RUST_LOG=info cargo r --features=flightsql -- serve-flight-sql

# You should already have run `cargo r --features=http -- serve-http` in another shell
bench-http-basic:
    #!/usr/bin/env sh
    http_code=$(curl "http://localhost:8080/health-check" --silent -o /dev/null -w "%{http_code}")
    if [ "$http_code" -ne "200" ]; then
      echo "HTTP server not running on http://localhost:8080"
      exit 1
    fi
    oha "http://localhost:8080/sql" -m POST -d 'SELECT 1'

# Run a custom benchmark script
bench-http-custom file:
    #!/usr/bin/env sh
    http_code=$(curl "http://localhost:8080/health-check" --silent -o /dev/null -w "%{http_code}")
    if [ "$http_code" -ne "200" ]; then
      echo "HTTP server not running on http://localhost:8080"
      exit 1
    fi
    custom_bench_path="bench/url_files/{{file}}"
    echo "Running bench on $custom_bench_path"
    echo ""
    oha --urls-from-file "$custom_bench_path"

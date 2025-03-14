# NOTES
# We use shebang in places where multi line constructs are needed
# Ref: https://github.com/casey/just?tab=readme-ov-file#if-statements

# Assumes you already have `cargo` installed
install-deps:
    cargo install oha

# You should already have run `cargo r --features=http -- serve-http` in another shell
bench-http-basic:
    #!/usr/bin/env sh
    http_code=$(curl "http://localhost:8080/health-check" --silent -o /dev/null -w "%{http_code}")
    if [ "$http_code" -ne "200" ]; then
      echo "HTTP server not running on http://localhost:8080"
      exit 1
    fi
    oha "http://localhost:8080" -m POST -d '{"query": "SELECT 1"}'

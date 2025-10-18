# DataFusion RocksDB

A DataFusion extension crate that provides User-Defined Functions (UDFs) and extension points for integrating RocksDB with Apache DataFusion.

## Overview

This crate enables DataFusion to work with RocksDB, providing:

- **UDFs**: Custom functions for interacting with RocksDB data
- **Extension Points**: DataFusion extension interfaces for RocksDB integration
- **Performance**: Efficient key-value storage and retrieval through RocksDB

## Features

This crate is currently under development. Planned features include:

- Reading and writing data to RocksDB key-value stores
- Table functions for querying RocksDB data
- Custom UDFs for RocksDB operations

## Usage

Add this crate to your `Cargo.toml`:

```toml
[dependencies]
datafusion-rocksdb = "0.1.0"
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE.txt](../../LICENSE.txt) for details.

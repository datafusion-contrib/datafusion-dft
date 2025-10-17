// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! DataFusion RocksDB integration
//!
//! This crate provides User-Defined Functions (UDFs) and DataFusion extension points
//! for working with RocksDB key-value stores.

#![deny(clippy::clone_on_ref_ptr)]

/// Module for RocksDB-related User-Defined Functions
pub mod udfs {
    //! User-Defined Functions for RocksDB operations
}

/// Module for RocksDB DataFusion extension points
pub mod extensions {
    //! DataFusion extension points for RocksDB integration
}

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

pub mod catalog;
pub mod config;
pub mod executor;
pub mod extensions;
#[cfg(feature = "flightsql")]
pub mod flightsql;
#[cfg(feature = "flightsql")]
pub mod flightsql_benchmarks;
pub mod local;
pub mod local_benchmarks;
#[cfg(feature = "observability")]
pub mod observability;
pub mod sql_utils;
pub mod stats;
pub mod tables;
#[cfg(feature = "udfs-wasm")]
pub mod wasm;

pub use stats::{collect_plan_io_stats, ExecutionStats};

use datafusion::execution::SendableRecordBatchStream;

pub struct ExecOptions {
    pub limit: Option<usize>,
    pub flightsql: bool,
}

impl ExecOptions {
    pub fn new(limit: Option<usize>, flightsql: bool) -> Self {
        Self { limit, flightsql }
    }
}

pub enum ExecResult {
    RecordBatchStream(SendableRecordBatchStream),
    RecordBatchStreamWithMetrics(()),
}

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

//! DataFusion table functions for inspecting [Vortex](https://vortex.dev) files.
//!
//! | Function | Grain |
//! |---|---|
//! | `vortex_file_metadata(file)` | one row per file |
//! | `vortex_schema(file)` | one row per DType node |
//! | `vortex_field_stats(file)` | one row per top-level field |
//! | `vortex_layout(file)` | one row per layout node |
//! | `vortex_zone_map(file [, column])` | one row per zone, column, and aggregate |
//! | `vortex_segments(file)` | one row per segment |
//! | `vortex_encodings(file [, column])` | one row per array encoding node |
//! | `vortex_compression(file)` | one row per top-level field |
//! | `vortex_kv_metadata(file)` | one row per metadata segment |
//! | `vortex_dictionary(file, column)` | one row per dictionary value |
//! | `vortex_rows(file, start [, end])` | data rows in a row range |

use datafusion::catalog::TableFunctionImpl;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

mod common;
mod compression;
mod dictionary;
mod encodings;
mod field_stats;
mod file_metadata;
mod kv_metadata;
mod layout;
mod rows;
mod schema;
mod segments;
#[cfg(test)]
mod test_util;
mod zone_map;

pub use compression::VortexCompressionFunc;
pub use dictionary::VortexDictionaryFunc;
pub use encodings::VortexEncodingsFunc;
pub use field_stats::VortexFieldStatsFunc;
pub use file_metadata::VortexFileMetadataFunc;
pub use kv_metadata::VortexKvMetadataFunc;
pub use layout::VortexLayoutFunc;
pub use rows::VortexRowsFunc;
pub use schema::VortexSchemaFunc;
pub use segments::VortexSegmentsFunc;
pub use zone_map::VortexZoneMapFunc;

/// All table functions in this crate with their SQL names
pub fn all_functions() -> Vec<(&'static str, Arc<dyn TableFunctionImpl>)> {
    vec![
        ("vortex_file_metadata", Arc::new(VortexFileMetadataFunc {})),
        ("vortex_schema", Arc::new(VortexSchemaFunc {})),
        ("vortex_field_stats", Arc::new(VortexFieldStatsFunc {})),
        ("vortex_layout", Arc::new(VortexLayoutFunc {})),
        ("vortex_zone_map", Arc::new(VortexZoneMapFunc {})),
        ("vortex_segments", Arc::new(VortexSegmentsFunc {})),
        ("vortex_encodings", Arc::new(VortexEncodingsFunc {})),
        ("vortex_compression", Arc::new(VortexCompressionFunc {})),
        ("vortex_kv_metadata", Arc::new(VortexKvMetadataFunc {})),
        ("vortex_dictionary", Arc::new(VortexDictionaryFunc {})),
        ("vortex_rows", Arc::new(VortexRowsFunc {})),
    ]
}

/// Register every table function in this crate with `ctx`
pub fn register_all(ctx: &SessionContext) {
    for (name, func) in all_functions() {
        ctx.register_udtf(name, func);
    }
}

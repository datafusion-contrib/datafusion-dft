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

//! This module has code to register DataFusion extensions

use crate::config::ExecutionConfig;
use datafusion::common::Result;
use std::fmt::Debug;

mod builder;
#[cfg(feature = "deltalake")]
mod deltalake;
#[cfg(feature = "s3")]
mod s3;

pub use builder::DftSessionStateBuilder;

pub trait Extension: Debug {
    /// Registers this extension with the DataFusion [`SessionStateBuilder`]
    fn register(
        &self,
        _config: &ExecutionConfig,
        _builder: DftSessionStateBuilder,
    ) -> Result<DftSessionStateBuilder>;
}

/// Return all extensions currently enabled
pub fn all_extensions() -> Vec<Box<dyn Extension>> {
    vec![
        #[cfg(feature = "s3")]
        Box::new(s3::AwsS3Extension::new()),
        #[cfg(feature = "deltalake")]
        Box::new(deltalake::DeltaLakeExtension::new()),
    ]
}

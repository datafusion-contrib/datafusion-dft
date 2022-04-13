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

use datafusion::error::DataFusionError;
use std::error;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;

pub type Result<T> = result::Result<T, DftError>;

#[derive(Debug)]
pub enum DftError {
    DataFusionError(DataFusionError),
    IoError(io::Error),
    UiError(String),
}

impl From<io::Error> for DftError {
    fn from(e: io::Error) -> Self {
        DftError::IoError(e)
    }
}

impl From<DataFusionError> for DftError {
    fn from(e: DataFusionError) -> Self {
        DftError::DataFusionError(e)
    }
}

impl Display for DftError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            DftError::DataFusionError(ref desc) => write!(f, "DataFusion error: {}", desc),
            DftError::IoError(ref desc) => write!(f, "IO error: {}", desc),
            DftError::UiError(ref text) => write!(f, "UI Error: {}", text),
        }
    }
}

impl error::Error for DftError {}

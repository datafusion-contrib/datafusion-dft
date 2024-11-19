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

use std::sync::Arc;

use async_trait::async_trait;
use object_store::{path::Path, GetOptions, GetResult, ObjectStore, Result};

use crate::execution::executor::io::spawn_io;

/// 'ObjectStore' that wraps an inner `ObjectStore` and wraps all the underlying methods with
/// [`spawn_io`] so that they are run on the Tokio Runtime dedicated to doing IO.
#[derive(Debug)]
pub struct IoObjectStore {
    inner: Arc<dyn ObjectStore>,
}

#[async_trait]
impl ObjectStore for IoObjectStore {
    async fn get(&self, location: &Path) -> Result<GetResult> {
        let location = location.clone();
        let store = self.inner.clone();
        spawn_io(async move { store.get(&location).await }).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let location = location.clone();
        let store = self.inner.clone();
        spawn_io(async move { store.get_opts(&location, options).await }).await
    }
}

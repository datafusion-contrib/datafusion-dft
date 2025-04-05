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

use datafusion::execution::cache::CacheAccessor;
use indexmap::IndexMap;
use object_store::{path::Path, ObjectMeta};

struct IndexMapFileCache {
    name: String,
    inner: IndexMap<Path, Arc<Vec<ObjectMeta>>>,
}

impl IndexMapFileCache {
    pub fn new(name: String, data: IndexMap<Path, Arc<Vec<ObjectMeta>>>) -> Self {
        Self { name, inner: data }
    }
}

impl CacheAccessor<Path, Arc<Vec<ObjectMeta>>> for IndexMapFileCache {
    type Extra = ObjectMeta;

    fn name(&self) -> String {
        self.name.clone()
    }

    fn get(&self, k: &Path) -> Option<Arc<Vec<ObjectMeta>>> {
        todo!()
    }

    fn get_with_extra(&self, k: &Path, e: &Self::Extra) -> Option<Arc<Vec<ObjectMeta>>> {
        todo!()
    }

    fn put(&self, key: &Path, value: Arc<Vec<ObjectMeta>>) -> Option<Arc<Vec<ObjectMeta>>> {
        todo!()
    }

    fn put_with_extra(
        &self,
        key: &Path,
        value: Arc<Vec<ObjectMeta>>,
        e: &Self::Extra,
    ) -> Option<Arc<Vec<ObjectMeta>>> {
        todo!()
    }

    fn remove(&mut self, k: &Path) -> Option<Arc<Vec<ObjectMeta>>> {
        todo!()
    }

    fn contains_key(&self, k: &Path) -> bool {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn clear(&self) {
        todo!()
    }
}

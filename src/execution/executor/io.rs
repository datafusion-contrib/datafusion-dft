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

use std::{
    cell::RefCell,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::Duration,
};

use futures::{
    future::{BoxFuture, Shared},
    Future, FutureExt, TryFutureExt,
};
use tokio::{runtime::Handle, task::JoinHandle};

thread_local! {
    /// Tokio runtime `Handle` for doing network (I/O) operations, see [`spawn_io`]
    pub static IO_RUNTIME: RefCell<Option<Handle>> = const { RefCell::new(None) };
}

/// Registers `handle` as the IO runtime for this thread
///
/// See [`spawn_io`]
pub fn register_io_runtime(handle: Option<Handle>) {
    IO_RUNTIME.set(handle)
}

/// Runs `fut` on the runtime registered by [`register_io_runtime`] if any,
/// otherwise awaits on the current thread
///
/// # Panic
/// Needs a IO runtime [registered](register_io_runtime).
pub async fn spawn_io<Fut>(fut: Fut) -> Fut::Output
where
    Fut: Future + Send + 'static,
    Fut::Output: Send,
{
    let h = IO_RUNTIME.with_borrow(|h| h.clone()).expect(
        "No IO runtime registered. If you hit this panic, it likely \
            means a DataFusion plan or other CPU bound work is running on the \
            a tokio threadpool used for IO. Try spawning the work using \
            `DedicatedExcutor::spawn` or for tests `register_current_runtime_for_io`",
    );
    DropGuard(h.spawn(fut)).await
}

struct DropGuard<T>(JoinHandle<T>);
impl<T> Drop for DropGuard<T> {
    fn drop(&mut self) {
        self.0.abort()
    }
}

impl<T> Future for DropGuard<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(match std::task::ready!(self.0.poll_unpin(cx)) {
            Ok(v) => v,
            Err(e) if e.is_cancelled() => panic!("IO runtime was shut down"),
            Err(e) => std::panic::resume_unwind(e.into_panic()),
        })
    }
}

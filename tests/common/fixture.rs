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

// This file was copied from the official arrow-rs repo.
//
// Ref: https://github.com/apache/arrow-rs/blob/a65e14a5c48d52bc8ab19b56e696a47567328056/arrow-flight/tests/common/fixture.rs

use crate::common::trailers_layer::TrailersLayer;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionEndTransactionRequest,
    SqlInfo,
};
use arrow_flight::Action;
use datafusion::arrow::array::RecordBatch;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Status};
use uuid::Uuid;

#[derive(Clone)]
pub struct FlightSqlServiceImpl {
    transactions: Arc<Mutex<HashMap<String, ()>>>,
    ingested_batches: Arc<Mutex<Vec<RecordBatch>>>,
}

impl FlightSqlServiceImpl {
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(Mutex::new(HashMap::new())),
            ingested_batches: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Return an [`FlightServiceServer`] that can be used with a
    /// [`Server`](tonic::transport::Server)
    pub fn service(&self) -> FlightServiceServer<Self> {
        // wrap up tonic goop
        FlightServiceServer::new(self.clone())
    }
}

impl Default for FlightSqlServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        let transaction_id = Uuid::new_v4().to_string();
        self.transactions
            .lock()
            .await
            .insert(transaction_id.clone(), ());
        Ok(ActionBeginTransactionResult {
            transaction_id: transaction_id.as_bytes().to_vec().into(),
        })
    }

    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        let transaction_id = String::from_utf8(query.transaction_id.to_vec())
            .map_err(|_| Status::invalid_argument("Invalid transaction id"))?;
        if self
            .transactions
            .lock()
            .await
            .remove(&transaction_id)
            .is_none()
        {
            return Err(Status::invalid_argument("Transaction id not found"));
        }
        Ok(())
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    // async fn do_put_statement_ingest(
    //     &self,
    //     _ticket: CommandStatementIngest,
    //     request: Request<PeekableFlightDataStream>,
    // ) -> Result<i64, Status> {
    //     let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
    //         request.into_inner().map_err(|e| e.into()),
    //     )
    //     .try_collect()
    //     .await?;
    //     let affected_rows = batches.iter().map(|batch| batch.num_rows() as i64).sum();
    //     *self.ingested_batches.lock().await.as_mut() = batches;
    //     Ok(affected_rows)
    // }
}

/// All tests must complete within this many seconds or else the test server is shutdown
const DEFAULT_TIMEOUT_SECONDS: u64 = 30;

/// Creates and manages a running TestServer with a background task
pub struct TestFixture {
    /// channel to send shutdown command
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,

    /// Address the server is listening on
    pub addr: SocketAddr,

    /// handle for the server task
    handle: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl TestFixture {
    /// create a new test fixture from the server
    #[allow(dead_code)]
    pub async fn new<T: FlightService>(test_server: FlightServiceServer<T>) -> Self {
        // let OS choose a free port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        println!("Listening on {addr}");

        // prepare the shutdown channel
        let (tx, rx) = tokio::sync::oneshot::channel();

        let server_timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECONDS);

        let shutdown_future = async move {
            rx.await.ok();
        };

        let serve_future = tonic::transport::Server::builder()
            .timeout(server_timeout)
            .layer(TrailersLayer)
            .add_service(test_server)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                shutdown_future,
            );

        // Run the server in its own background task
        let handle = tokio::task::spawn(serve_future);

        Self {
            shutdown: Some(tx),
            addr,
            handle: Some(handle),
        }
    }

    /// Return a [`Channel`] connected to the TestServer
    #[allow(dead_code)]
    pub async fn channel(&self) -> Channel {
        let url = format!("http://{}", self.addr);
        let uri: Uri = url.parse().expect("Valid URI");
        Channel::builder(uri)
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECONDS))
            .connect()
            .await
            .expect("error connecting to server")
    }

    /// Stops the test server and waits for the server to shutdown
    #[allow(dead_code)]
    pub async fn shutdown_and_wait(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).expect("server quit early");
        }
        if let Some(handle) = self.handle.take() {
            println!("Waiting on server to finish");
            handle
                .await
                .expect("task join error (panic?)")
                .expect("Server Error found at shutdown");
        }
    }
}

impl Drop for TestFixture {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).ok();
        }
        if self.handle.is_some() {
            // tests should properly clean up TestFixture
            println!("TestFixture::Drop called prior to `shutdown_and_wait`");
        }
    }
}

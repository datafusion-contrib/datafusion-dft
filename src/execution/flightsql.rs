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

use arrow_flight::sql::client::FlightSqlServiceClient;
use log::{debug, error, info};

use color_eyre::eyre::{self, Result};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::config::FlightSQLConfig;

pub type FlightSQLClient = Mutex<Option<FlightSqlServiceClient<Channel>>>;

#[derive(Default)]
pub struct FlightSQLContext {
    config: FlightSQLConfig,
    flightsql_client: FlightSQLClient,
}

impl FlightSQLContext {
    pub fn new(config: FlightSQLConfig) -> Self {
        Self {
            config,
            flightsql_client: Mutex::new(None),
        }
    }

    pub fn client(&self) -> &FlightSQLClient {
        &self.flightsql_client
    }

    /// Create FlightSQL client from users FlightSQL config
    pub async fn create_client(&self) -> Result<()> {
        let url = Box::leak(self.config.connection_url.clone().into_boxed_str());
        info!("Connecting to FlightSQL host: {}", url);
        let channel = Channel::from_static(url).connect().await;
        match channel {
            Ok(c) => {
                let client = FlightSqlServiceClient::new(c);
                let mut guard = self.flightsql_client.lock().await;
                *guard = Some(client);
                Ok(())
            }
            Err(e) => Err(eyre::eyre!(
                "Error creating channel for FlightSQL client: {:?}",
                e
            )),
        }
    }

    // fn benchmark_query(&self, query: &str) -> Result<FlightSQLBenchmarkStats> {
    //     let iterations = self.config.benchmark_iterations;
    // }
}

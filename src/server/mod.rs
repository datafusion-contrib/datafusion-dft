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

use std::net::SocketAddr;

use color_eyre::Result;
use log::info;
use metrics::{describe_counter, describe_histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};

#[cfg(feature = "flightsql")]
pub mod flightsql;
#[cfg(feature = "http")]
pub mod http;

fn describe_metrics() {
    describe_counter!("requests", "Incoming requests by FlightSQL endpoint");

    describe_histogram!(
        "get_flight_info_latency_ms",
        metrics::Unit::Milliseconds,
        "Get flight info latency ms"
    );

    describe_histogram!(
        "do_get_fallback_latency_ms",
        metrics::Unit::Milliseconds,
        "Do get fallback latency ms"
    )
}

pub fn try_start_metrics_server(metrics_addr: SocketAddr) -> Result<()> {
    let builder = PrometheusBuilder::new();
    info!("Listening to metrics on {metrics_addr}");
    builder
        .with_http_listener(metrics_addr)
        .set_buckets_for_metric(
            Matcher::Suffix("latency_ms".to_string()),
            &[
                1.0, 3.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
                10000.0, 20000.0,
            ],
        )?
        .install()
        .expect("failed to install metrics recorder/exporter");

    describe_metrics();
    Ok(())
}

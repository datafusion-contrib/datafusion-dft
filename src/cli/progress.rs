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

//! Progress reporting for CLI benchmarks

use datafusion_app::local_benchmarks::BenchmarkProgressReporter;
use indicatif::{ProgressBar, ProgressStyle};
use std::time::{Duration, Instant};

/// Progress reporter that displays progress bars using indicatif
pub struct IndicatifProgressReporter {
    pb: ProgressBar,
    start_time: Instant,
    concurrent: bool,
}

impl IndicatifProgressReporter {
    pub fn new(query: &str, total_iterations: usize, concurrent: bool, concurrency: usize) -> Self {
        let pb = ProgressBar::new(total_iterations as u64);

        // Truncate query for display (max 50 chars)
        let query_display = if query.len() > 50 {
            format!("{}...", &query[..47])
        } else {
            query.to_string()
        };

        let template = if concurrent {
            format!(
                "{{spinner:.green}} Benchmarking (concurrent, {} workers): {} [{{elapsed_precise}}]\n\
                 {{bar:40.cyan/blue}} {{pos}}/{{len}} ({{percent}}%) | {{msg}}",
                concurrency, query_display
            )
        } else {
            format!(
                "{{spinner:.green}} Benchmarking: {} [{{elapsed_precise}}]\n\
                 {{bar:40.cyan/blue}} {{pos}}/{{len}} ({{percent}}%) | {{msg}}",
                query_display
            )
        };

        pb.set_style(
            ProgressStyle::default_bar()
                .template(&template)
                .expect("Invalid progress template")
                .progress_chars("━━╾─")
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ "),
        );

        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        Self {
            pb,
            start_time: Instant::now(),
            concurrent,
        }
    }
}

impl BenchmarkProgressReporter for IndicatifProgressReporter {
    fn on_iteration_complete(&self, completed: usize, total: usize, _last_duration: Duration) {
        self.pb.set_position(completed as u64);

        let elapsed = self.start_time.elapsed();

        if self.concurrent {
            // For concurrent mode, show throughput (iterations per second)
            let throughput = if elapsed.as_secs_f64() > 0.0 {
                completed as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            };

            // Calculate ETA based on average throughput
            let remaining = total - completed;
            let eta_secs = if throughput > 0.0 {
                remaining as f64 / throughput
            } else {
                0.0
            };

            self.pb
                .set_message(format!("{:.1} iter/s | ETA: {:.1}s", throughput, eta_secs));
        } else {
            // For serial mode, show time per iteration
            let avg_time = elapsed.as_secs_f64() / completed as f64;
            let remaining = total - completed;
            let eta_secs = avg_time * remaining as f64;

            self.pb
                .set_message(format!("{:.2}s per iter | ETA: {:.1}s", avg_time, eta_secs));
        }
    }

    fn finish(&self) {
        self.pb.finish_and_clear();
    }
}

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

use std::time::Duration;

use super::local_benchmarks::DurationsSummary;

pub struct FlightSQLBenchmarkStats {
    query: String,
    runs: usize,
    get_flight_info_durations: Vec<Duration>,
    ttfb_durations: Vec<Duration>,
    do_get_durations: Vec<Duration>,
    total_durations: Vec<Duration>,
}

impl FlightSQLBenchmarkStats {
    pub fn new(
        query: String,
        get_flight_info_durations: Vec<Duration>,
        ttfb_durations: Vec<Duration>,
        do_get_durations: Vec<Duration>,
        total_durations: Vec<Duration>,
    ) -> Self {
        let runs = get_flight_info_durations.len();
        Self {
            query,
            runs,
            get_flight_info_durations,
            ttfb_durations,
            do_get_durations,
            total_durations,
        }
    }

    fn summarize(&self, durations: &[Duration]) -> DurationsSummary {
        let mut sorted = durations.to_vec();
        sorted.sort();
        let len = sorted.len();
        let min = *sorted.first().unwrap();
        let max = *sorted.last().unwrap();
        let mean = sorted.iter().sum::<Duration>() / len as u32;
        let median = sorted[len / 2];
        let this_total = durations.iter().map(|d| d.as_nanos()).sum::<u128>();
        let total_duration = self
            .total_durations
            .iter()
            .map(|d| d.as_nanos())
            .sum::<u128>();
        let percent_of_total = (this_total as f64 / total_duration as f64) * 100.0;
        DurationsSummary {
            min,
            max,
            mean,
            median,
            percent_of_total,
        }
    }
}

impl std::fmt::Display for FlightSQLBenchmarkStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f)?;
        writeln!(f, "----------------------------")?;
        writeln!(f, "Benchmark Stats ({} runs)", self.runs)?;
        writeln!(f, "----------------------------")?;
        writeln!(f, "{}", self.query)?;
        writeln!(f, "----------------------------")?;

        let logical_planning_summary = self.summarize(&self.get_flight_info_durations);
        writeln!(f, "Get Flight Info")?;
        writeln!(f, "{}", logical_planning_summary)?;

        let physical_planning_summary = self.summarize(&self.ttfb_durations);
        writeln!(f, "Time to First Byte")?;
        writeln!(f, "{}", physical_planning_summary)?;

        let execution_summary = self.summarize(&self.do_get_durations);
        writeln!(f, "Do Get")?;
        writeln!(f, "{}", execution_summary)?;

        let total_summary = self.summarize(&self.total_durations);
        writeln!(f, "Total")?;
        writeln!(f, "{}", total_summary)
    }
}

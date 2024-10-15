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

//! [`ExecutionContext`]: DataFusion based execution context for running SQL queries

use std::time::Duration;

/// Duration summary statistics
#[derive(Debug)]
pub struct DurationsSummary {
    pub min: Duration,
    pub max: Duration,
    pub mean: Duration,
    pub median: Duration,
    pub percent_of_total: f64,
}

impl std::fmt::Display for DurationsSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Min: {:?}", self.min)?;
        writeln!(f, "Max: {:?}", self.max)?;
        writeln!(f, "Median: {:?}", self.median)?;
        writeln!(f, "Mean: {:?} ({:.2}%)", self.mean, self.percent_of_total)
    }
}

/// Contains stats for all runs of a benchmarked query and provides methods for aggregating
#[derive(Debug, Default)]
pub struct LocalBenchmarkStats {
    query: String,
    runs: usize,
    logical_planning_durations: Vec<Duration>,
    physical_planning_durations: Vec<Duration>,
    execution_durations: Vec<Duration>,
    total_durations: Vec<Duration>,
}

impl LocalBenchmarkStats {
    pub fn new(
        query: String,
        logical_planning_durations: Vec<Duration>,
        physical_planning_durations: Vec<Duration>,
        execution_durations: Vec<Duration>,
        total_durations: Vec<Duration>,
    ) -> Self {
        let runs = logical_planning_durations.len();
        Self {
            query,
            runs,
            logical_planning_durations,
            physical_planning_durations,
            execution_durations,
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

impl std::fmt::Display for LocalBenchmarkStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f)?;
        writeln!(f, "----------------------------")?;
        writeln!(f, "Benchmark Stats ({} runs)", self.runs)?;
        writeln!(f, "----------------------------")?;
        writeln!(f, "{}", self.query)?;
        writeln!(f, "----------------------------")?;

        let logical_planning_summary = self.summarize(&self.logical_planning_durations);
        writeln!(f, "Logical Planning")?;
        writeln!(f, "{}", logical_planning_summary)?;

        let physical_planning_summary = self.summarize(&self.physical_planning_durations);
        writeln!(f, "Physical Planning")?;
        writeln!(f, "{}", physical_planning_summary)?;

        let execution_summary = self.summarize(&self.execution_durations);
        writeln!(f, "Execution")?;
        writeln!(f, "{}", execution_summary)?;

        let total_summary = self.summarize(&self.total_durations);
        writeln!(f, "Total")?;
        writeln!(f, "{}", total_summary)
    }
}

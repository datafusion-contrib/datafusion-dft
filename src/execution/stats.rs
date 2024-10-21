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

use datafusion::physical_plan::{
    metrics::MetricValue, visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor,
};
use log::info;
use std::{sync::Arc, time::Duration};

#[derive(Clone, Debug)]
pub struct ExecutionStats {
    // bytes_scanned: usize,
    rows: usize,
    batches: i32,
    durations: ExecutionDurationStats,
    io: Option<ExecutionIOStats>,
    plan: Arc<dyn ExecutionPlan>,
}

impl ExecutionStats {
    pub fn try_new(
        durations: ExecutionDurationStats,
        rows: usize,
        batches: i32,
        plan: Arc<dyn ExecutionPlan>,
    ) -> color_eyre::Result<Self> {
        Ok(Self {
            durations,
            rows,
            batches,
            plan,
            io: None,
        })
    }

    pub fn collect_stats(&mut self) {
        if let Some(io) = collect_plan_io_stats(Arc::clone(&self.plan)) {
            self.io = Some(io)
        }
    }

    // pub fn bytes_scanned(&self) -> usize {
    //     self.bytes_scanned
    // }
}

impl std::fmt::Display for ExecutionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "==================== Execution Summary ===================="
        )?;
        writeln!(f, "{:<20} {:<20}", "Rows Returned", "Batches Processed")?;
        writeln!(f, "{:<20} {:<20}", self.rows, self.batches)?;
        writeln!(f)?;
        writeln!(f, "{}", self.durations)?;
        if let Some(io_stats) = &self.io {
            writeln!(f, "{}", io_stats)?;
        };
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionDurationStats {
    parsing: Duration,
    logical_planning: Duration,
    physical_planning: Duration,
    execution: Duration,
    total: Duration,
}

impl ExecutionDurationStats {
    pub fn new(
        parsing: Duration,
        logical_planning: Duration,
        physical_planning: Duration,
        execution: Duration,
        total: Duration,
    ) -> Self {
        Self {
            parsing,
            logical_planning,
            physical_planning,
            execution,
            total,
        }
    }
}

impl std::fmt::Display for ExecutionDurationStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "{:<20} {:<20} {:<20}",
            "Parsing", "Logical Planning", "Physical Planning"
        )?;
        writeln!(
            f,
            "{:<20?} {:<20?} {:<20?}",
            self.parsing, self.logical_planning, self.physical_planning
        )?;
        writeln!(f)?;
        writeln!(f, "{:<20} {:<20}", "Execution", "Total")?;
        writeln!(f, "{:<20?} {:<20?}", self.execution, self.total)?;
        writeln!(f)
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionIOStats {
    bytes_scanned: Option<MetricValue>,
    time_opening: Option<MetricValue>,
    time_scanning: Option<MetricValue>,
    parquet_stats: Option<ParquetStats>,
}

impl std::fmt::Display for ExecutionIOStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "======================= IO Summary ========================"
        )?;
        writeln!(
            f,
            "{:<20} {:<20} {:<20}",
            "Bytes Scanned", "Time Opening", "Time Scanning"
        )?;
        writeln!(
            f,
            "{:<20} {:<20} {:<20}",
            self.bytes_scanned
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or("None".to_string()),
            self.time_opening
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or("None".to_string()),
            self.time_scanning
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or("None".to_string())
        )
    }
}

#[derive(Clone, Debug)]
pub struct ParquetStats {}

/// Visitor to collect IO metrics from an execution plan
///
/// IO metrics are collected from nodes that perform IO operations, such as
/// `CsvExec`, `ParquetExec`, and `ArrowExec`.
struct PlanIOVisitor {
    bytes_scanned: Option<MetricValue>,
    time_opening: Option<MetricValue>,
    time_scanning: Option<MetricValue>,
}

impl PlanIOVisitor {
    fn new() -> Self {
        Self {
            bytes_scanned: None,
            time_opening: None,
            time_scanning: None,
        }
    }

    fn collect_io_metrics(&mut self, plan: &dyn ExecutionPlan) {
        let io_metrics = plan.metrics();
        if let Some(metrics) = io_metrics {
            println!("Metrics for {}: {:#?}", plan.name(), metrics);
            if let Some(scanned_bytes) = metrics.sum_by_name("bytes_scanned") {
                self.bytes_scanned = Some(scanned_bytes);
            }
            if let Some(opening) = metrics.sum_by_name("time_elapsed_opening") {
                self.time_opening = Some(opening);
            }
            if let Some(scanning) = metrics.sum_by_name("time_elapsed_scanning_total") {
                self.time_scanning = Some(scanning);
            }
        }
    }
}

impl From<PlanIOVisitor> for ExecutionIOStats {
    fn from(value: PlanIOVisitor) -> Self {
        Self {
            bytes_scanned: value.bytes_scanned,
            time_opening: value.time_opening,
            time_scanning: value.time_scanning,
            parquet_stats: None,
        }
    }
}

impl ExecutionPlanVisitor for PlanIOVisitor {
    type Error = datafusion_common::DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> color_eyre::Result<bool, Self::Error> {
        if is_io_plan(plan) {
            println!("Collecting IO metrics for {}", plan.name());
            self.collect_io_metrics(plan);
        }
        match plan.metrics() {
            Some(metrics) => match metrics.sum_by_name("bytes_scanned") {
                Some(bytes_scanned) => {
                    self.bytes_scanned = Some(bytes_scanned);
                }
                None => {
                    info!("No bytes_scanned for {}", plan.name())
                }
            },
            None => {
                info!("No MetricsSet for {}", plan.name())
            }
        }
        Ok(true)
    }
}

fn is_io_plan(plan: &dyn ExecutionPlan) -> bool {
    let io_plans = ["CsvExec", "ParquetExec", "ArrowExec"];
    println!("Plan name: {}", plan.name());
    io_plans.contains(&plan.name())
}

pub fn collect_plan_io_stats(plan: Arc<dyn ExecutionPlan>) -> Option<ExecutionIOStats> {
    let mut visitor = PlanIOVisitor::new();
    if visit_execution_plan(plan.as_ref(), &mut visitor).is_ok() {
        Some(visitor.into())
    } else {
        None
    }
}

// pub fn print_execution_summary(
//     rows: usize,
//     batches: i32,
//     parsing_dur: Duration,
//     logical_planning_dur: Duration,
//     physical_planning_dur: Duration,
//     execution_dur: Duration,
//     total_dur: Duration,
// ) {
//     println!("==================== Execution Summary ====================");
//     println!("{:<20} {:<20}", "Rows Returned", "Batches Processed");
//     println!("{:<20} {:<20}", rows, batches);
//     println!();
//     println!(
//         "{:<20} {:<20} {:<20}",
//         "Parsing", "Logical Planning", "Physical Planning"
//     );
//     println!(
//         "{:<20?} {:<20?} {:<20?}",
//         parsing_dur, logical_planning_dur, physical_planning_dur
//     );
//     println!();
//     println!("{:<20} {:<20}", "Execution", "Total");
//     println!("{:<20?} {:<20?}", execution_dur, total_dur);
//     println!();
// }

pub fn print_io_summary(plan: Arc<dyn ExecutionPlan>) {
    println!("======================= IO Summary ========================");
    if let Some(stats) = collect_plan_io_stats(plan) {
        println!("IO Stats: {:#?}", stats);
    } else {
        println!("No IO metrics found");
    }
}

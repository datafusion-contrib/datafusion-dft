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

use datafusion::{
    datasource::physical_plan::ParquetExec,
    physical_plan::{
        filter::FilterExec, metrics::MetricValue, visit_execution_plan, ExecutionPlan,
        ExecutionPlanVisitor,
    },
};
use itertools::Itertools;
use log::info;
use std::{sync::Arc, time::Duration};

#[derive(Clone, Debug)]
pub struct ExecutionStats {
    rows: usize,
    batches: i32,
    durations: ExecutionDurationStats,
    io: Option<ExecutionIOStats>,
    compute: Option<ExecutionComputeStats>,
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
            compute: None,
        })
    }

    pub fn collect_stats(&mut self) {
        if let Some(io) = collect_plan_io_stats(Arc::clone(&self.plan)) {
            self.io = Some(io)
        }
        if let Some(compute) = collect_plan_compute_stats(Arc::clone(&self.plan)) {
            self.compute = Some(compute)
        }
    }
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
        if let Some(compute_stats) = &self.compute {
            writeln!(f, "{}", compute_stats)?;
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
    parquet_output_rows: Option<usize>,
    parquet_pruned_page_index: Option<MetricValue>,
    parquet_matched_page_index: Option<MetricValue>,
    parquet_rg_pruned_stats: Option<MetricValue>,
    parquet_rg_matched_stats: Option<MetricValue>,
    parquet_rg_pruned_bloom_filter: Option<MetricValue>,
    parquet_rg_matched_bloom_filter: Option<MetricValue>,
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
        )?;
        writeln!(f)?;
        writeln!(
            f,
            "Parquet IO Stats (Output Rows: {})",
            self.parquet_output_rows
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or("None".to_string())
        )?;
        writeln!(f)?;
        writeln!(
            f,
            "{:<30} {:<30}",
            "Pruned Page Index", "Matched Page Index"
        )?;
        writeln!(
            f,
            "{:<30} {:<30}",
            self.parquet_pruned_page_index
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or("None".to_string()),
            self.parquet_matched_page_index
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or("None".to_string())
        )?;
        writeln!(f)?;
        writeln!(
            f,
            "{:<30} {:<30}",
            "Row Groups Pruned (Stats)", "Row Groups Matched (Stats)"
        )?;
        writeln!(
            f,
            "{:<30} {:<30}",
            self.parquet_rg_pruned_stats
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or("None".to_string()),
            self.parquet_rg_matched_stats
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or("None".to_string())
        )?;
        writeln!(f)?;
        writeln!(
            f,
            "{:<30} {:<30}",
            "Row Groups Pruned (Bloom)", "Row Groups Matched (Bloom)"
        )?;
        writeln!(
            f,
            "{:<30} {:<30}",
            self.parquet_rg_pruned_bloom_filter
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or("None".to_string()),
            self.parquet_rg_matched_bloom_filter
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or("None".to_string())
        )
    }
}

/// Visitor to collect IO metrics from an execution plan
///
/// IO metrics are collected from nodes that perform IO operations, such as
/// `CsvExec`, `ParquetExec`, and `ArrowExec`.
struct PlanIOVisitor {
    bytes_scanned: Option<MetricValue>,
    time_opening: Option<MetricValue>,
    time_scanning: Option<MetricValue>,
    parquet_output_rows: Option<usize>,
    parquet_pruned_page_index: Option<MetricValue>,
    parquet_matched_page_index: Option<MetricValue>,
    parquet_rg_pruned_stats: Option<MetricValue>,
    parquet_rg_matched_stats: Option<MetricValue>,
    parquet_rg_pruned_bloom_filter: Option<MetricValue>,
    parquet_rg_matched_bloom_filter: Option<MetricValue>,
}

impl PlanIOVisitor {
    fn new() -> Self {
        Self {
            bytes_scanned: None,
            time_opening: None,
            time_scanning: None,
            parquet_output_rows: None,
            parquet_pruned_page_index: None,
            parquet_matched_page_index: None,
            parquet_rg_pruned_stats: None,
            parquet_rg_matched_stats: None,
            parquet_rg_pruned_bloom_filter: None,
            parquet_rg_matched_bloom_filter: None,
        }
    }

    fn collect_io_metrics(&mut self, plan: &dyn ExecutionPlan) {
        let io_metrics = plan.metrics();
        if let Some(metrics) = io_metrics {
            println!("Metrics for {}: {:#?}", plan.name(), metrics);
            self.bytes_scanned = metrics.sum_by_name("bytes_scanned");
            self.time_opening = metrics.sum_by_name("time_elapsed_opening");
            self.time_scanning = metrics.sum_by_name("time_elapsed_scanning_total");

            if plan.as_any().downcast_ref::<ParquetExec>().is_some() {
                self.parquet_output_rows = metrics.output_rows();
                self.parquet_rg_pruned_stats = metrics.sum_by_name("row_groups_pruned_statistics");
                self.parquet_rg_matched_stats =
                    metrics.sum_by_name("row_groups_matched_statistics");
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
            parquet_output_rows: value.parquet_output_rows,
            parquet_pruned_page_index: value.parquet_pruned_page_index,
            parquet_matched_page_index: value.parquet_matched_page_index,
            parquet_rg_pruned_stats: value.parquet_rg_pruned_stats,
            parquet_rg_matched_stats: value.parquet_rg_matched_stats,
            parquet_rg_pruned_bloom_filter: value.parquet_rg_pruned_bloom_filter,
            parquet_rg_matched_bloom_filter: value.parquet_rg_matched_bloom_filter,
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
        Ok(true)
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionComputeStats {
    elapsed_compute: Option<usize>,
}

impl std::fmt::Display for ExecutionComputeStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "======================= Compute Summary ========================"
        )?;
        writeln!(f, "{:<20}", "Elapsed Compute",)?;
        writeln!(
            f,
            "{:<20}",
            self.elapsed_compute
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or("None".to_string()),
        )?;
        writeln!(f)
    }
}

pub struct PlanComputeVisitor {
    elapsed_compute: Option<usize>,
}

impl PlanComputeVisitor {
    pub fn new() -> Self {
        Self {
            elapsed_compute: None,
        }
    }

    fn collect_compute_metrics(&mut self, plan: &dyn ExecutionPlan) {
        let compute_metrics = plan.metrics();
        if let Some(metrics) = compute_metrics {
            println!("Metrics for {}: {:#?}", plan.name(), metrics);
            self.elapsed_compute = metrics.elapsed_compute();
        }
    }

    fn collect_filter_metrics(&mut self, plan: &dyn ExecutionPlan) {
        if plan.as_any().downcast_ref::<FilterExec>().is_some() {
            if let Some(metrics) = plan.metrics() {
                let agg = metrics.iter().chunk_by(|m| m.labels());
            }
        }
    }
}

impl From<PlanComputeVisitor> for ExecutionComputeStats {
    fn from(value: PlanComputeVisitor) -> Self {
        Self {
            elapsed_compute: value.elapsed_compute,
        }
    }
}

impl ExecutionPlanVisitor for PlanComputeVisitor {
    type Error = datafusion_common::DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> color_eyre::Result<bool, Self::Error> {
        if !is_io_plan(plan) {
            println!("Collecting Compute metrics for {}", plan.name());
            self.collect_compute_metrics(plan);
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

pub fn collect_plan_compute_stats(plan: Arc<dyn ExecutionPlan>) -> Option<ExecutionComputeStats> {
    let mut visitor = PlanComputeVisitor::new();
    if visit_execution_plan(plan.as_ref(), &mut visitor).is_ok() {
        Some(visitor.into())
    } else {
        None
    }
}

pub fn print_io_summary(plan: Arc<dyn ExecutionPlan>) {
    println!("======================= IO Summary ========================");
    if let Some(stats) = collect_plan_io_stats(plan) {
        println!("IO Stats: {:#?}", stats);
    } else {
        println!("No IO metrics found");
    }
}

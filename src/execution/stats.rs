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
        aggregates::AggregateExec,
        filter::FilterExec,
        joins::{
            CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec,
            SymmetricHashJoinExec,
        },
        metrics::MetricValue,
        projection::ProjectionExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor,
    },
};
use itertools::Itertools;
use std::{sync::Arc, time::Duration};

#[derive(Clone, Debug)]
pub struct ExecutionStats {
    query: String,
    rows: usize,
    batches: i32,
    bytes: usize,
    durations: ExecutionDurationStats,
    io: Option<ExecutionIOStats>,
    compute: Option<ExecutionComputeStats>,
    plan: Arc<dyn ExecutionPlan>,
}

impl ExecutionStats {
    pub fn try_new(
        query: String,
        durations: ExecutionDurationStats,
        rows: usize,
        batches: i32,
        bytes: usize,
        plan: Arc<dyn ExecutionPlan>,
    ) -> color_eyre::Result<Self> {
        Ok(Self {
            query,
            durations,
            rows,
            batches,
            bytes,
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

    pub fn rows_selectivity(&self) -> f64 {
        let maybe_io_output_rows = self.io.as_ref().and_then(|io| io.parquet_output_rows);
        if let Some(io_output_rows) = maybe_io_output_rows {
            self.rows as f64 / io_output_rows as f64
        } else {
            0.0
        }
    }

    pub fn bytes_selectivity(&self) -> f64 {
        let maybe_io_output_bytes = self.io.as_ref().and_then(|io| io.bytes_scanned.clone());
        if let Some(io_output_bytes) = maybe_io_output_bytes {
            self.bytes as f64 / io_output_bytes.as_usize() as f64
        } else {
            0.0
        }
    }

    /// A ratio of the selectivity of the query to the effectivness of pruning the parquet file.
    ///
    /// V1: Simply look at row groups pruned by statistics and the row selectivity
    /// TODO: Incorporate bloom filter and page index pruning
    ///
    /// Example calculations:
    ///
    /// 1. No pruning and select all rows
    ///    - row groups pruned: 0
    ///    - row groups matched: 10
    ///    - row selectivity: 1.0
    pub fn efficiency(&self) -> f64 {
        if let Some(io) = &self.io {
            io.parquet_rg_pruned_stats_ratio() / self.rows_selectivity()
        } else {
            0.0
        }
    }
}

impl std::fmt::Display for ExecutionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "========================= Query ==========================="
        )?;
        writeln!(f, "{}", self.query)?;
        writeln!(
            f,
            "==================== Execution Summary ===================="
        )?;
        writeln!(
            f,
            "{:<20} {:<20} {:<20}",
            "Output Rows (%)", "Output Bytes (%)", "Batches Processed",
        )?;
        writeln!(
            f,
            "{:<20} {:<20} {:<20}",
            format!("{} ({:.2})", self.rows, self.rows_selectivity()),
            format!("{} ({:.2})", self.bytes, self.bytes_selectivity()),
            self.batches,
        )?;
        writeln!(f)?;
        writeln!(f, "{}", self.durations)?;
        writeln!(f, "{:<20}", "Efficiency")?;
        writeln!(f, "{:<20.2}", self.efficiency())?;
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
        writeln!(f, "{:<20} {:<20}", "Execution", "Total",)?;
        writeln!(f, "{:<20?} {:<20?}", self.execution, self.total)?;
        Ok(())
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

impl ExecutionIOStats {
    fn parquet_rg_pruned_stats_ratio(&self) -> f64 {
        if let (Some(pruned), Some(matched)) = (
            self.parquet_rg_matched_stats.as_ref(),
            self.parquet_rg_pruned_stats.as_ref(),
        ) {
            let pruned = pruned.as_usize() as f64;
            let matched = matched.as_usize() as f64;
            matched / (pruned + matched)
        } else {
            0.0
        }
    }

    fn parquet_rg_pruned_bloom_filter_ratio(&self) -> f64 {
        if let (Some(pruned), Some(matched)) = (
            self.parquet_rg_matched_bloom_filter.as_ref(),
            self.parquet_rg_pruned_bloom_filter.as_ref(),
        ) {
            let pruned = pruned.as_usize() as f64;
            let matched = matched.as_usize() as f64;
            matched / (pruned + matched)
        } else {
            0.0
        }
    }

    fn parquet_rg_pruned_page_index_ratio(&self) -> f64 {
        if let (Some(pruned), Some(matched)) = (
            self.parquet_matched_page_index.as_ref(),
            self.parquet_pruned_page_index.as_ref(),
        ) {
            let pruned = pruned.as_usize() as f64;
            let matched = matched.as_usize() as f64;
            matched / (pruned + matched)
        } else {
            0.0
        }
    }

    fn row_group_count(&self) -> usize {
        if let (Some(pruned), Some(matched)) = (
            self.parquet_rg_matched_stats.as_ref(),
            self.parquet_rg_pruned_stats.as_ref(),
        ) {
            let pruned = pruned.as_usize();
            let matched = matched.as_usize();
            pruned + matched
        } else {
            0
        }
    }
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
            "Parquet Pruning Stats (Output Rows: {}, Row Groups: {})",
            self.parquet_output_rows
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or("None".to_string()),
            self.row_group_count()
        )?;
        writeln!(
            f,
            "{:<20} {:<20} {:<20}",
            "Matched RG Stats %", "Matched RG Bloom %", "Matched Page Index %"
        )?;
        writeln!(
            f,
            "{:<20.2} {:<20.2} {:<20.2}",
            self.parquet_rg_pruned_stats_ratio(),
            self.parquet_rg_pruned_bloom_filter_ratio(),
            self.parquet_rg_pruned_page_index_ratio()
        )?;
        Ok(())
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
            self.collect_io_metrics(plan);
        }
        Ok(true)
    }
}

#[derive(Clone, Debug)]
pub struct PartitionsComputeStats {
    /// Sorted elapsed compute times
    elapsed_computes: Vec<usize>,
}

impl PartitionsComputeStats {
    fn summary_stats(&self) -> (usize, usize, usize, usize, usize) {
        if self.elapsed_computes.is_empty() {
            (0, 0, 0, 0, 0)
        } else {
            let min = self.elapsed_computes[0];
            let median = self.elapsed_computes[self.elapsed_computes.len() / 2];
            let max = self.elapsed_computes[self.elapsed_computes.len() - 1];
            let total: usize = self.elapsed_computes.iter().sum();
            let mean = total / self.elapsed_computes.len();
            (min, median, mean, max, total)
        }
    }

    fn partitions(&self) -> usize {
        self.elapsed_computes.len()
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionComputeStats {
    elapsed_compute: Option<usize>,
    projection_compute: Option<Vec<PartitionsComputeStats>>,
    filter_compute: Option<Vec<PartitionsComputeStats>>,
    sort_compute: Option<Vec<PartitionsComputeStats>>,
    join_compute: Option<Vec<PartitionsComputeStats>>,
    aggregate_compute: Option<Vec<PartitionsComputeStats>>,
    other_compute: Option<Vec<PartitionsComputeStats>>,
}

impl ExecutionComputeStats {
    fn display_compute(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        compute: &Option<Vec<PartitionsComputeStats>>,
        label: &str,
    ) -> std::fmt::Result {
        if let (Some(filter_compute), Some(elapsed_compute)) = (compute, &self.elapsed_compute) {
            let partitions = filter_compute.iter().fold(0, |acc, c| acc + c.partitions());
            writeln!(
                f,
                "{label} Stats ({} nodes, {} partitions)",
                filter_compute.len(),
                partitions
            )?;
            filter_compute.iter().try_for_each(|node| {
                let (min, median, mean, max, total) = node.summary_stats();
                writeln!(
                    f,
                    "{:<18} {:<18} {:<18} {:<18} {:<18}",
                    "Min", "Median", "Mean", "Max", "Total (%)"
                )?;
                let total = format!(
                    "{} ({:.2}%)",
                    total,
                    (total as f32 / *elapsed_compute as f32) * 100.0
                );
                writeln!(
                    f,
                    "{:<18} {:<18} {:<18} {:<18} {:<18}",
                    min, median, mean, max, total,
                )?;
                Ok(())
            })?
        } else {
            writeln!(f, "No {label} Stats")?;
        };
        Ok(())
    }
}

impl std::fmt::Display for ExecutionComputeStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "==================================== Compute Summary ====================================="
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
        writeln!(f)?;
        self.display_compute(f, &self.projection_compute, "Projection")?;
        writeln!(f)?;
        self.display_compute(f, &self.filter_compute, "Filter")?;
        writeln!(f)?;
        self.display_compute(f, &self.sort_compute, "Sort")?;
        writeln!(f)?;
        self.display_compute(f, &self.join_compute, "Join")?;
        writeln!(f)?;
        self.display_compute(f, &self.aggregate_compute, "Aggregate")?;
        writeln!(f)?;
        self.display_compute(f, &self.other_compute, "Other")?;
        writeln!(f)
    }
}

#[derive(Default)]
pub struct PlanComputeVisitor {
    elapsed_compute: Option<usize>,
    filter_computes: Vec<PartitionsComputeStats>,
    sort_computes: Vec<PartitionsComputeStats>,
    projection_computes: Vec<PartitionsComputeStats>,
    join_computes: Vec<PartitionsComputeStats>,
    aggregate_computes: Vec<PartitionsComputeStats>,
    other_computes: Vec<PartitionsComputeStats>,
}

impl PlanComputeVisitor {
    fn add_elapsed_compute(&mut self, node_elapsed_compute: Option<usize>) {
        match (self.elapsed_compute, node_elapsed_compute) {
            (Some(agg_elapsed_compute), Some(node_elapsed_compute)) => {
                self.elapsed_compute = Some(agg_elapsed_compute + node_elapsed_compute)
            }
            (Some(_), None) | (None, None) => {}
            (None, Some(node_elapsed_compute)) => self.elapsed_compute = Some(node_elapsed_compute),
        }
    }

    fn collect_compute_metrics(&mut self, plan: &dyn ExecutionPlan) {
        let compute_metrics = plan.metrics();
        if let Some(metrics) = compute_metrics {
            self.add_elapsed_compute(metrics.elapsed_compute());
        }
        self.collect_filter_metrics(plan);
        self.collect_sort_metrics(plan);
        self.collect_projection_metrics(plan);
        self.collect_join_metrics(plan);
        self.collect_aggregate_metrics(plan);
        self.collect_other_metrics(plan);
    }

    // TODO: Refactor to have a single function that takes predicate and collector
    fn collect_filter_metrics(&mut self, plan: &dyn ExecutionPlan) {
        if is_filter_plan(plan) {
            if let Some(metrics) = plan.metrics() {
                let sorted_computes: Vec<usize> = metrics
                    .iter()
                    .filter_map(|m| match m.value() {
                        MetricValue::ElapsedCompute(t) => Some(t.value()),
                        _ => None,
                    })
                    .sorted()
                    .collect();
                let p = PartitionsComputeStats {
                    elapsed_computes: sorted_computes,
                };
                self.filter_computes.push(p)
            }
        }
    }

    fn collect_sort_metrics(&mut self, plan: &dyn ExecutionPlan) {
        if is_sort_plan(plan) {
            if let Some(metrics) = plan.metrics() {
                let sorted_computes: Vec<usize> = metrics
                    .iter()
                    .filter_map(|m| match m.value() {
                        MetricValue::ElapsedCompute(t) => Some(t.value()),
                        _ => None,
                    })
                    .sorted()
                    .collect();
                let p = PartitionsComputeStats {
                    elapsed_computes: sorted_computes,
                };
                self.sort_computes.push(p)
            }
        }
    }

    fn collect_projection_metrics(&mut self, plan: &dyn ExecutionPlan) {
        if is_projection_plan(plan) {
            if let Some(metrics) = plan.metrics() {
                let sorted_computes: Vec<usize> = metrics
                    .iter()
                    .filter_map(|m| match m.value() {
                        MetricValue::ElapsedCompute(t) => Some(t.value()),
                        _ => None,
                    })
                    .sorted()
                    .collect();
                let p = PartitionsComputeStats {
                    elapsed_computes: sorted_computes,
                };
                self.projection_computes.push(p)
            }
        }
    }

    fn collect_join_metrics(&mut self, plan: &dyn ExecutionPlan) {
        if is_join_plan(plan) {
            if let Some(metrics) = plan.metrics() {
                let sorted_computes: Vec<usize> = metrics
                    .iter()
                    .filter_map(|m| match m.value() {
                        MetricValue::ElapsedCompute(t) => Some(t.value()),
                        _ => None,
                    })
                    .sorted()
                    .collect();
                let p = PartitionsComputeStats {
                    elapsed_computes: sorted_computes,
                };
                self.join_computes.push(p)
            }
        }
    }

    fn collect_aggregate_metrics(&mut self, plan: &dyn ExecutionPlan) {
        if is_aggregate_plan(plan) {
            if let Some(metrics) = plan.metrics() {
                let sorted_computes: Vec<usize> = metrics
                    .iter()
                    .filter_map(|m| match m.value() {
                        MetricValue::ElapsedCompute(t) => Some(t.value()),
                        _ => None,
                    })
                    .sorted()
                    .collect();
                let p = PartitionsComputeStats {
                    elapsed_computes: sorted_computes,
                };
                self.aggregate_computes.push(p)
            }
        }
    }

    fn collect_other_metrics(&mut self, plan: &dyn ExecutionPlan) {
        if !is_filter_plan(plan) && !is_sort_plan(plan) && !is_projection_plan(plan) {
            if let Some(metrics) = plan.metrics() {
                let sorted_computes: Vec<usize> = metrics
                    .iter()
                    .filter_map(|m| match m.value() {
                        MetricValue::ElapsedCompute(t) => Some(t.value()),
                        _ => None,
                    })
                    .sorted()
                    .collect();
                let p = PartitionsComputeStats {
                    elapsed_computes: sorted_computes,
                };
                self.other_computes.push(p)
            }
        }
    }
}

fn is_filter_plan(plan: &dyn ExecutionPlan) -> bool {
    plan.as_any().downcast_ref::<FilterExec>().is_some()
}

fn is_sort_plan(plan: &dyn ExecutionPlan) -> bool {
    plan.as_any().downcast_ref::<SortExec>().is_some()
        || plan
            .as_any()
            .downcast_ref::<SortPreservingMergeExec>()
            .is_some()
}

fn is_projection_plan(plan: &dyn ExecutionPlan) -> bool {
    plan.as_any().downcast_ref::<ProjectionExec>().is_some()
}

fn is_join_plan(plan: &dyn ExecutionPlan) -> bool {
    plan.as_any().downcast_ref::<HashJoinExec>().is_some()
        || plan.as_any().downcast_ref::<CrossJoinExec>().is_some()
        || plan.as_any().downcast_ref::<SortMergeJoinExec>().is_some()
        || plan.as_any().downcast_ref::<NestedLoopJoinExec>().is_some()
        || plan
            .as_any()
            .downcast_ref::<SymmetricHashJoinExec>()
            .is_some()
}

fn is_aggregate_plan(plan: &dyn ExecutionPlan) -> bool {
    plan.as_any().downcast_ref::<AggregateExec>().is_some()
}

impl From<PlanComputeVisitor> for ExecutionComputeStats {
    fn from(value: PlanComputeVisitor) -> Self {
        Self {
            elapsed_compute: value.elapsed_compute,
            filter_compute: Some(value.filter_computes),
            sort_compute: Some(value.sort_computes),
            projection_compute: Some(value.projection_computes),
            join_compute: Some(value.join_computes),
            aggregate_compute: Some(value.aggregate_computes),
            other_compute: Some(value.other_computes),
        }
    }
}

impl ExecutionPlanVisitor for PlanComputeVisitor {
    type Error = datafusion_common::DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> color_eyre::Result<bool, Self::Error> {
        if !is_io_plan(plan) {
            self.collect_compute_metrics(plan);
        }
        Ok(true)
    }
}

fn is_io_plan(plan: &dyn ExecutionPlan) -> bool {
    let io_plans = ["CsvExec", "ParquetExec", "ArrowExec"];
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
    let mut visitor = PlanComputeVisitor::default();
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

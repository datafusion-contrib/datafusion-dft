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
    arrow::{
        array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    datasource::{physical_plan::ParquetSource, source::DataSourceExec},
    physical_plan::{
        aggregates::AggregateExec,
        empty::EmptyExec,
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
use log::debug;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};

/// Request structure for the analyze_query action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzeQueryRequest {
    /// SQL query to analyze (currently the only supported format)
    pub sql: Option<String>,

    // Future extensibility fields (not yet implemented):
    // /// Substrait query plan (binary or JSON)
    // pub substrait: Option<Vec<u8>>,
    // /// Serialized logical plan
    // pub logical_plan: Option<String>,
    // /// Serialized physical plan
    // pub physical_plan: Option<String>,
}

impl AnalyzeQueryRequest {
    /// Create a new request with a SQL query
    pub fn with_sql(sql: impl Into<String>) -> Self {
        Self {
            sql: Some(sql.into()),
        }
    }

    /// Get the SQL query, returning an error if not present
    pub fn sql(&self) -> color_eyre::Result<&str> {
        self.sql
            .as_deref()
            .ok_or_else(|| color_eyre::eyre::eyre!("sql field is required"))
    }
}

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
    /// Maps operator name to (parent_name, child_index)
    #[allow(dead_code)]
    // TODO: Use for populating operator_parent/operator_index in to_metrics_table
    operator_hierarchy: HashMap<String, (Option<String>, i32)>,
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
        // Collect operator hierarchy
        // Root node has no parent (None) and index -1 (represents NULL)
        let operator_hierarchy = collect_operator_hierarchy(&plan, None, -1);

        Ok(Self {
            query,
            durations,
            rows,
            batches,
            bytes,
            plan,
            io: None,
            compute: None,
            operator_hierarchy,
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

    pub fn selectivity_efficiency(&self) -> f64 {
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
        writeln!(f, "{:<20}", "Parquet Efficiency (Pruning / Selectivity)")?;
        writeln!(f, "{:<20.2}", self.selectivity_efficiency())?;
        writeln!(f)?;
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
            "Parquet Pruning Stats (Output Rows: {}, Row Groups: {} [{}ms per row group])",
            self.parquet_output_rows
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or("None".to_string()),
            self.row_group_count(),
            self.time_scanning
                .as_ref()
                .map(|ts| format!(
                    "{:.2}",
                    (ts.as_usize() / 1_000_000) as f64 / self.row_group_count() as f64
                ))
                .unwrap_or("None".to_string())
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

            if let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
                if data_source_exec
                    .data_source()
                    .as_any()
                    .downcast_ref::<ParquetSource>()
                    .is_some()
                {
                    self.parquet_output_rows = metrics.output_rows();
                    self.parquet_rg_pruned_stats =
                        metrics.sum_by_name("row_groups_pruned_statistics");
                    self.parquet_rg_matched_stats =
                        metrics.sum_by_name("row_groups_matched_statistics");
                }
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
    type Error = datafusion::common::DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> color_eyre::Result<bool, Self::Error> {
        if is_io_plan(plan) {
            self.collect_io_metrics(plan);
        }
        Ok(true)
    }
}

#[derive(Clone, Debug)]
pub struct PartitionsComputeStats {
    name: String,
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
    window_compute: Option<Vec<PartitionsComputeStats>>,
    distinct_compute: Option<Vec<PartitionsComputeStats>>,
    limit_compute: Option<Vec<PartitionsComputeStats>>,
    union_compute: Option<Vec<PartitionsComputeStats>>,
    other_compute: Option<Vec<PartitionsComputeStats>>,
}

impl ExecutionComputeStats {
    fn display_compute(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        compute: &Option<Vec<PartitionsComputeStats>>,
        label: &str,
    ) -> std::fmt::Result {
        match (compute, &self.elapsed_compute) {
            (Some(filter_compute), Some(elapsed_compute)) if !filter_compute.is_empty() => {
                let partitions = filter_compute.iter().fold(0, |acc, c| acc + c.partitions());
                writeln!(
                    f,
                    "{label}: {} nodes, {} partitions",
                    filter_compute.len(),
                    partitions
                )?;
                writeln!(
                    f,
                    "{:<30} {:<16} {:<16} {:<16} {:<16} {:<16}",
                    "Node(Partitions)", "Min", "Median", "Mean", "Max", "Total (%)"
                )?;
                filter_compute.iter().try_for_each(|node| {
                    let (min, median, mean, max, total) = node.summary_stats();
                    let total = format!(
                        "{} ({:.2}%)",
                        total,
                        (total as f32 / *elapsed_compute as f32) * 100.0
                    );
                    writeln!(
                        f,
                        "{:<30} {:<16} {:<16} {:<16} {:<16} {:<16}",
                        format!("{}({})", node.name, node.elapsed_computes.len()),
                        min,
                        median,
                        mean,
                        max,
                        total,
                    )
                })
            }
            _ => {
                writeln!(f, "{label}: No data")
            }
        }
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

        // Display all categories in order
        self.display_compute(f, &self.projection_compute, "Projection")?;
        writeln!(f)?;
        self.display_compute(f, &self.filter_compute, "Filter")?;
        writeln!(f)?;
        self.display_compute(f, &self.sort_compute, "Sort")?;
        writeln!(f)?;
        self.display_compute(f, &self.aggregate_compute, "Aggregate")?;
        writeln!(f)?;
        self.display_compute(f, &self.join_compute, "Join")?;
        writeln!(f)?;
        self.display_compute(f, &self.window_compute, "Window")?;
        writeln!(f)?;
        self.display_compute(f, &self.distinct_compute, "Distinct")?;
        writeln!(f)?;
        self.display_compute(f, &self.limit_compute, "Limit")?;
        writeln!(f)?;
        self.display_compute(f, &self.union_compute, "Union")?;
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
                    name: plan.name().to_string(),
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
                    name: plan.name().to_string(),
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
                    name: plan.name().to_string(),
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
                    name: plan.name().to_string(),
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
                    name: plan.name().to_string(),
                    elapsed_computes: sorted_computes,
                };
                self.aggregate_computes.push(p)
            }
        }
    }

    fn collect_other_metrics(&mut self, plan: &dyn ExecutionPlan) {
        if !is_filter_plan(plan)
            && !is_sort_plan(plan)
            && !is_projection_plan(plan)
            && !is_aggregate_plan(plan)
            && !is_join_plan(plan)
        {
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
                    name: plan.name().to_string(),
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
            window_compute: None,   // TODO: Collect from visitor
            distinct_compute: None, // TODO: Collect from visitor
            limit_compute: None,    // TODO: Collect from visitor
            union_compute: None,    // TODO: Collect from visitor
            other_compute: Some(value.other_computes),
        }
    }
}

impl ExecutionPlanVisitor for PlanComputeVisitor {
    type Error = datafusion::common::DataFusionError;

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

/// Classify an operator into a category based on its name
#[allow(dead_code)] // TODO: Use in compute visitor for better categorization
fn classify_operator_category(operator_name: &str) -> &'static str {
    // Check for specific operator types
    if operator_name.contains("Filter") {
        return "filter";
    }
    if operator_name.contains("Sort") {
        return "sort";
    }
    if operator_name.contains("Projection") {
        return "projection";
    }
    if operator_name.contains("Join") {
        return "join";
    }
    if operator_name.contains("Aggregate") || operator_name.contains("GroupBy") {
        return "aggregate";
    }
    if operator_name.contains("Window") {
        return "window";
    }
    if operator_name.contains("Distinct") || operator_name.contains("Deduplicate") {
        return "distinct";
    }
    if operator_name.contains("Limit") || operator_name.contains("TopK") {
        return "limit";
    }
    if operator_name.contains("Union") {
        return "union";
    }
    if is_io_plan_by_name(operator_name) {
        return "io";
    }

    "other"
}

#[allow(dead_code)] // Used by classify_operator_category
fn is_io_plan_by_name(name: &str) -> bool {
    let io_plans = [
        "CsvExec",
        "ParquetExec",
        "ArrowExec",
        "JsonExec",
        "AvroExec",
    ];
    io_plans.iter().any(|p| name.contains(p))
}

/// Collect operator hierarchy from execution plan tree
/// Returns a map of operator_name -> (parent_name, child_index)
/// Note: Root node should have parent=None and index=-1 (which represents NULL)
fn collect_operator_hierarchy(
    plan: &Arc<dyn ExecutionPlan>,
    parent: Option<String>,
    index: i32,
) -> HashMap<String, (Option<String>, i32)> {
    let mut hierarchy = HashMap::new();

    // Get operator name - use the plan's name
    let operator_name = plan.name().to_string();

    // Store this operator's hierarchy info
    // For root nodes (parent=None), index should be -1 to represent NULL
    hierarchy.insert(operator_name.clone(), (parent, index));

    // Recursively collect from children
    for (child_index, child) in plan.children().iter().enumerate() {
        let child_hierarchy =
            collect_operator_hierarchy(child, Some(operator_name.clone()), child_index as i32);
        hierarchy.extend(child_hierarchy);
    }

    hierarchy
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

/// Standard Arrow schema for analyze metrics
pub fn analyze_metrics_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value", DataType::UInt64, false),
        Field::new("value_type", DataType::Utf8, false),
        Field::new("operator_name", DataType::Utf8, true),
        Field::new("partition_id", DataType::Int32, true),
        Field::new("operator_category", DataType::Utf8, true),
        Field::new("operator_parent", DataType::Utf8, true),
        Field::new("operator_index", DataType::Int32, true),
    ]))
}

/// Helper to build metrics table rows
struct MetricsTableBuilder {
    metric_names: Vec<String>,
    values: Vec<u64>,
    value_types: Vec<String>,
    operator_names: Vec<Option<String>>,
    partition_ids: Vec<Option<i32>>,
    operator_categories: Vec<Option<String>>,
    operator_parents: Vec<Option<String>>,
    operator_indices: Vec<Option<i32>>,
}

impl MetricsTableBuilder {
    fn new() -> Self {
        Self {
            metric_names: Vec::new(),
            values: Vec::new(),
            value_types: Vec::new(),
            operator_names: Vec::new(),
            partition_ids: Vec::new(),
            operator_categories: Vec::new(),
            operator_parents: Vec::new(),
            operator_indices: Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn add(
        &mut self,
        metric_name: &str,
        value: u64,
        value_type: &str,
        operator_name: Option<&str>,
        partition_id: Option<i32>,
        operator_category: Option<&str>,
        operator_parent: Option<&str>,
        operator_index: Option<i32>,
    ) {
        self.metric_names.push(metric_name.to_string());
        self.values.push(value);
        self.value_types.push(value_type.to_string());
        self.operator_names.push(operator_name.map(String::from));
        self.partition_ids.push(partition_id);
        self.operator_categories
            .push(operator_category.map(String::from));
        self.operator_parents
            .push(operator_parent.map(String::from));
        self.operator_indices.push(operator_index);
    }

    fn build(self, schema: SchemaRef) -> color_eyre::Result<RecordBatch> {
        let metric_names_array: ArrayRef = Arc::new(StringArray::from(self.metric_names));
        let values_array: ArrayRef = Arc::new(UInt64Array::from(self.values));
        let value_types_array: ArrayRef = Arc::new(StringArray::from(self.value_types));
        let operator_names_array: ArrayRef = Arc::new(StringArray::from(self.operator_names));
        let partition_ids_array: ArrayRef = Arc::new(Int32Array::from(self.partition_ids));
        let operator_categories_array: ArrayRef =
            Arc::new(StringArray::from(self.operator_categories));
        let operator_parents_array: ArrayRef = Arc::new(StringArray::from(self.operator_parents));
        let operator_indices_array: ArrayRef = Arc::new(Int32Array::from(self.operator_indices));

        Ok(RecordBatch::try_new(
            schema,
            vec![
                metric_names_array,
                values_array,
                value_types_array,
                operator_names_array,
                partition_ids_array,
                operator_categories_array,
                operator_parents_array,
                operator_indices_array,
            ],
        )?)
    }
}

impl ExecutionStats {
    /// Serialize ExecutionStats to metrics table format
    pub fn to_metrics_table(&self) -> color_eyre::Result<RecordBatch> {
        let schema = analyze_metrics_schema();
        let mut rows = MetricsTableBuilder::new();

        // Add basic metrics with namespacing
        rows.add(
            "query.rows",
            self.rows as u64,
            "count",
            None,
            None,
            None,
            None,
            None,
        );
        rows.add(
            "query.batches",
            self.batches as u64,
            "count",
            None,
            None,
            None,
            None,
            None,
        );
        rows.add(
            "query.bytes",
            self.bytes as u64,
            "bytes",
            None,
            None,
            None,
            None,
            None,
        );

        // Add duration metrics with namespacing
        rows.add(
            "stage.parsing",
            self.durations.parsing.as_nanos() as u64,
            "duration_ns",
            None,
            None,
            None,
            None,
            None,
        );
        rows.add(
            "stage.logical_planning",
            self.durations.logical_planning.as_nanos() as u64,
            "duration_ns",
            None,
            None,
            None,
            None,
            None,
        );
        rows.add(
            "stage.physical_planning",
            self.durations.physical_planning.as_nanos() as u64,
            "duration_ns",
            None,
            None,
            None,
            None,
            None,
        );
        rows.add(
            "stage.execution",
            self.durations.execution.as_nanos() as u64,
            "duration_ns",
            None,
            None,
            None,
            None,
            None,
        );
        rows.add(
            "stage.total",
            self.durations.total.as_nanos() as u64,
            "duration_ns",
            None,
            None,
            None,
            None,
            None,
        );

        // Add IO metrics if present with namespacing
        // TODO: Populate operator_parent and operator_index from execution plan hierarchy
        if let Some(io) = &self.io {
            if let Some(bytes) = &io.bytes_scanned {
                rows.add(
                    "io.parquet.bytes_scanned",
                    bytes.as_usize() as u64,
                    "bytes",
                    Some("ParquetExec"),
                    None,
                    Some("io"),
                    None, // operator_parent - will be populated with hierarchy collection
                    None, // operator_index - will be populated with hierarchy collection
                );
            }
            if let Some(time) = &io.time_opening {
                rows.add(
                    "io.parquet.time_opening",
                    time.as_usize() as u64,
                    "duration_ns",
                    Some("ParquetExec"),
                    None,
                    Some("io"),
                    None,
                    None,
                );
            }
            if let Some(time) = &io.time_scanning {
                rows.add(
                    "io.parquet.time_scanning",
                    time.as_usize() as u64,
                    "duration_ns",
                    Some("ParquetExec"),
                    None,
                    Some("io"),
                    None,
                    None,
                );
            }
            if let Some(output_rows) = io.parquet_output_rows {
                rows.add(
                    "io.parquet.output_rows",
                    output_rows as u64,
                    "count",
                    Some("ParquetExec"),
                    None,
                    Some("io"),
                    None,
                    None,
                );
            }
            if let Some(pruned) = &io.parquet_rg_pruned_stats {
                rows.add(
                    "io.parquet.rg_pruned",
                    pruned.as_usize() as u64,
                    "count",
                    Some("ParquetExec"),
                    None,
                    Some("io"),
                    None,
                    None,
                );
            }
            if let Some(matched) = &io.parquet_rg_matched_stats {
                rows.add(
                    "io.parquet.rg_matched",
                    matched.as_usize() as u64,
                    "count",
                    Some("ParquetExec"),
                    None,
                    Some("io"),
                    None,
                    None,
                );
            }
            if let Some(pruned) = &io.parquet_rg_pruned_bloom_filter {
                rows.add(
                    "io.parquet.bloom_pruned",
                    pruned.as_usize() as u64,
                    "count",
                    Some("ParquetExec"),
                    None,
                    Some("io"),
                    None,
                    None,
                );
            }
            if let Some(matched) = &io.parquet_rg_matched_bloom_filter {
                rows.add(
                    "io.parquet.bloom_matched",
                    matched.as_usize() as u64,
                    "count",
                    Some("ParquetExec"),
                    None,
                    Some("io"),
                    None,
                    None,
                );
            }
            if let Some(pruned) = &io.parquet_pruned_page_index {
                rows.add(
                    "io.parquet.page_index_pruned",
                    pruned.as_usize() as u64,
                    "count",
                    Some("ParquetExec"),
                    None,
                    Some("io"),
                    None,
                    None,
                );
            }
            if let Some(matched) = &io.parquet_matched_page_index {
                rows.add(
                    "io.parquet.page_index_matched",
                    matched.as_usize() as u64,
                    "count",
                    Some("ParquetExec"),
                    None,
                    Some("io"),
                    None,
                    None,
                );
            }
        }

        // Add compute metrics if present with namespacing
        // TODO: Populate operator_parent and operator_index from execution plan hierarchy
        if let Some(compute) = &self.compute {
            if let Some(elapsed) = compute.elapsed_compute {
                rows.add(
                    "compute.elapsed_compute",
                    elapsed as u64,
                    "duration_ns",
                    None,
                    None,
                    None,
                    None,
                    None,
                );
            }

            // Helper to add compute metrics for a category
            let add_compute_category = |rows: &mut MetricsTableBuilder,
                                        compute_stats: &Option<Vec<PartitionsComputeStats>>,
                                        category: &str| {
                if let Some(stats) = compute_stats {
                    for stat in stats {
                        for (partition_id, elapsed) in stat.elapsed_computes.iter().enumerate() {
                            rows.add(
                                "compute.elapsed_compute",
                                *elapsed as u64,
                                "duration_ns",
                                Some(&stat.name),
                                Some(partition_id as i32),
                                Some(category),
                                None, // operator_parent - will be populated with hierarchy collection
                                None, // operator_index - will be populated with hierarchy collection
                            );
                        }
                    }
                }
            };

            add_compute_category(&mut rows, &compute.filter_compute, "filter");
            add_compute_category(&mut rows, &compute.sort_compute, "sort");
            add_compute_category(&mut rows, &compute.projection_compute, "projection");
            add_compute_category(&mut rows, &compute.join_compute, "join");
            add_compute_category(&mut rows, &compute.aggregate_compute, "aggregate");
            add_compute_category(&mut rows, &compute.other_compute, "other");
        }

        rows.build(schema)
    }

    /// Deserialize ExecutionStats from metrics table
    pub fn from_metrics_table(batch: RecordBatch, query: String) -> color_eyre::Result<Self> {
        let mut stats_builder = ExecutionStatsBuilder::new(query);

        // Extract column arrays
        let metric_names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| color_eyre::eyre::eyre!("Invalid metric_name column type"))?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| color_eyre::eyre::eyre!("Invalid value column type"))?;
        let value_types = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| color_eyre::eyre::eyre!("Invalid value_type column type"))?;
        let operator_names = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| color_eyre::eyre::eyre!("Invalid operator_name column type"))?;
        let partition_ids = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| color_eyre::eyre::eyre!("Invalid partition_id column type"))?;
        let operator_categories = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| color_eyre::eyre::eyre!("Invalid operator_category column type"))?;
        let operator_parents = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| color_eyre::eyre::eyre!("Invalid operator_parent column type"))?;
        let operator_indices = batch
            .column(7)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| color_eyre::eyre::eyre!("Invalid operator_index column type"))?;

        // Iterate rows and populate stats
        for row_idx in 0..batch.num_rows() {
            let metric_name = metric_names.value(row_idx);
            let value = values.value(row_idx);
            let value_type = value_types.value(row_idx);
            let operator_name = if operator_names.is_null(row_idx) {
                None
            } else {
                Some(operator_names.value(row_idx))
            };
            let partition_id = if partition_ids.is_null(row_idx) {
                None
            } else {
                Some(partition_ids.value(row_idx))
            };
            let operator_category = if operator_categories.is_null(row_idx) {
                None
            } else {
                Some(operator_categories.value(row_idx))
            };
            // Extract operator_parent and operator_index (not currently used but preserved for future)
            let _operator_parent = if operator_parents.is_null(row_idx) {
                None
            } else {
                Some(operator_parents.value(row_idx))
            };
            let _operator_index = if operator_indices.is_null(row_idx) {
                None
            } else {
                Some(operator_indices.value(row_idx))
            };

            stats_builder.add_metric(
                metric_name,
                value,
                value_type,
                operator_name,
                partition_id,
                operator_category,
            )?;
        }

        stats_builder.build()
    }
}

/// Helper builder to construct ExecutionStats from metrics
struct ExecutionStatsBuilder {
    query: String,
    rows: usize,
    batches: i32,
    bytes: usize,
    parsing: Duration,
    logical_planning: Duration,
    physical_planning: Duration,
    execution: Duration,
    total: Duration,
    io_metrics: HashMap<String, u64>,
    compute_metrics: HashMap<String, Vec<(String, Option<i32>, u64)>>,
    elapsed_compute: Option<usize>,
}

impl ExecutionStatsBuilder {
    fn new(query: String) -> Self {
        Self {
            query,
            rows: 0,
            batches: 0,
            bytes: 0,
            parsing: Duration::ZERO,
            logical_planning: Duration::ZERO,
            physical_planning: Duration::ZERO,
            execution: Duration::ZERO,
            total: Duration::ZERO,
            io_metrics: HashMap::new(),
            compute_metrics: HashMap::new(),
            elapsed_compute: None,
        }
    }

    fn add_metric(
        &mut self,
        name: &str,
        value: u64,
        _value_type: &str,
        operator: Option<&str>,
        partition: Option<i32>,
        category: Option<&str>,
    ) -> color_eyre::Result<()> {
        // Support both namespaced (e.g., "query.rows") and legacy (e.g., "rows") metric names
        match (name, category) {
            // Query-level metrics (support both forms)
            ("rows" | "query.rows", None) => self.rows = value as usize,
            ("batches" | "query.batches", None) => self.batches = value as i32,
            ("bytes" | "query.bytes", None) => self.bytes = value as usize,

            // Stage duration metrics (support both forms)
            ("parsing" | "stage.parsing", None) => self.parsing = Duration::from_nanos(value),
            ("logical_planning" | "stage.logical_planning", None) => {
                self.logical_planning = Duration::from_nanos(value)
            }
            ("physical_planning" | "stage.physical_planning", None) => {
                self.physical_planning = Duration::from_nanos(value)
            }
            ("execution" | "stage.execution", None) => self.execution = Duration::from_nanos(value),
            ("total" | "stage.total", None) => self.total = Duration::from_nanos(value),

            // Compute metrics (support both forms)
            ("elapsed_compute" | "compute.elapsed_compute", None) => {
                self.elapsed_compute = Some(value as usize)
            }
            ("elapsed_compute" | "compute.elapsed_compute", Some(cat)) => {
                self.compute_metrics
                    .entry(cat.to_string())
                    .or_default()
                    .push((operator.unwrap_or("Unknown").to_string(), partition, value));
            }

            // I/O metrics (all io.* namespace)
            (metric, Some("io")) => {
                // Store with original metric name (might be namespaced or legacy)
                self.io_metrics.insert(metric.to_string(), value);
            }

            // Unknown metrics - log but don't fail
            _ => {
                debug!("Unknown metric: {} (category: {:?})", name, category);
            }
        }
        Ok(())
    }

    fn build(self) -> color_eyre::Result<ExecutionStats> {
        // Build IO stats from collected metrics
        let io = if !self.io_metrics.is_empty() {
            Some(ExecutionIOStats::from_metrics(self.io_metrics)?)
        } else {
            None
        };

        // Build compute stats from collected metrics
        let compute = if !self.compute_metrics.is_empty() || self.elapsed_compute.is_some() {
            Some(ExecutionComputeStats::from_metrics(
                self.compute_metrics,
                self.elapsed_compute,
            )?)
        } else {
            None
        };

        // Create dummy plan (not serialized)
        let plan = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));

        let durations = ExecutionDurationStats::new(
            self.parsing,
            self.logical_planning,
            self.physical_planning,
            self.execution,
            self.total,
        );

        Ok(ExecutionStats {
            query: self.query,
            rows: self.rows,
            batches: self.batches,
            bytes: self.bytes,
            durations,
            io,
            compute,
            plan,
            // When deserializing from metrics, we don't reconstruct the hierarchy
            // The hierarchy info is preserved in the metrics table itself
            operator_hierarchy: HashMap::new(),
        })
    }
}

impl ExecutionIOStats {
    fn from_metrics(metrics: HashMap<String, u64>) -> color_eyre::Result<Self> {
        use datafusion::physical_plan::metrics::{Count, Time};

        // Helper to create Count from value
        let create_count = |value: u64| -> MetricValue {
            let count = Count::new();
            count.add(value as usize);
            MetricValue::Count {
                name: "count".into(),
                count,
            }
        };

        // Helper to create Time from value
        let create_time = |value: u64| -> MetricValue {
            let time = Time::new();
            time.add_duration(std::time::Duration::from_nanos(value));
            MetricValue::Time {
                name: "time".into(),
                time,
            }
        };

        // Helper to get metric value, trying both namespaced and legacy names
        let get_metric = |namespaced: &str, legacy: &str| -> Option<u64> {
            metrics
                .get(namespaced)
                .or_else(|| metrics.get(legacy))
                .copied()
        };

        Ok(Self {
            bytes_scanned: get_metric("io.parquet.bytes_scanned", "bytes_scanned")
                .map(|v| create_count(v)),
            time_opening: get_metric("io.parquet.time_opening", "time_opening")
                .map(|v| create_time(v)),
            time_scanning: get_metric("io.parquet.time_scanning", "time_scanning")
                .map(|v| create_time(v)),
            parquet_output_rows: get_metric("io.parquet.output_rows", "output_rows")
                .map(|v| v as usize),
            parquet_pruned_page_index: get_metric(
                "io.parquet.page_index_pruned",
                "parquet_page_index_pruned",
            )
            .map(|v| create_count(v)),
            parquet_matched_page_index: get_metric(
                "io.parquet.page_index_matched",
                "parquet_page_index_matched",
            )
            .map(|v| create_count(v)),
            parquet_rg_pruned_stats: get_metric("io.parquet.rg_pruned", "parquet_rg_pruned")
                .map(|v| create_count(v)),
            parquet_rg_matched_stats: get_metric("io.parquet.rg_matched", "parquet_rg_matched")
                .map(|v| create_count(v)),
            parquet_rg_pruned_bloom_filter: get_metric(
                "io.parquet.bloom_pruned",
                "parquet_bloom_pruned",
            )
            .map(|v| create_count(v)),
            parquet_rg_matched_bloom_filter: get_metric(
                "io.parquet.bloom_matched",
                "parquet_bloom_matched",
            )
            .map(|v| create_count(v)),
        })
    }
}

impl ExecutionComputeStats {
    fn from_metrics(
        metrics: HashMap<String, Vec<(String, Option<i32>, u64)>>,
        elapsed_compute: Option<usize>,
    ) -> color_eyre::Result<Self> {
        // Helper to convert metrics to PartitionsComputeStats
        let to_partition_stats =
            |entries: &[(String, Option<i32>, u64)]| -> Vec<PartitionsComputeStats> {
                // Group by operator name
                let mut by_operator: HashMap<String, Vec<(i32, u64)>> = HashMap::new();
                for (op_name, partition_id, value) in entries {
                    if let Some(pid) = partition_id {
                        by_operator
                            .entry(op_name.clone())
                            .or_default()
                            .push((*pid, *value));
                    }
                }

                by_operator
                    .into_iter()
                    .map(|(name, mut partitions)| {
                        // Sort by partition ID to ensure correct ordering
                        partitions.sort_by_key(|(pid, _)| *pid);
                        let elapsed_computes: Vec<usize> =
                            partitions.iter().map(|(_, v)| *v as usize).collect();
                        PartitionsComputeStats {
                            name,
                            elapsed_computes,
                        }
                    })
                    .collect()
            };

        Ok(Self {
            elapsed_compute,
            projection_compute: metrics
                .get("projection")
                .map(|m| to_partition_stats(m))
                .filter(|v| !v.is_empty()),
            filter_compute: metrics
                .get("filter")
                .map(|m| to_partition_stats(m))
                .filter(|v| !v.is_empty()),
            sort_compute: metrics
                .get("sort")
                .map(|m| to_partition_stats(m))
                .filter(|v| !v.is_empty()),
            join_compute: metrics
                .get("join")
                .map(|m| to_partition_stats(m))
                .filter(|v| !v.is_empty()),
            aggregate_compute: metrics
                .get("aggregate")
                .map(|m| to_partition_stats(m))
                .filter(|v| !v.is_empty()),
            window_compute: metrics
                .get("window")
                .map(|m| to_partition_stats(m))
                .filter(|v| !v.is_empty()),
            distinct_compute: metrics
                .get("distinct")
                .map(|m| to_partition_stats(m))
                .filter(|v| !v.is_empty()),
            limit_compute: metrics
                .get("limit")
                .map(|m| to_partition_stats(m))
                .filter(|v| !v.is_empty()),
            union_compute: metrics
                .get("union")
                .map(|m| to_partition_stats(m))
                .filter(|v| !v.is_empty()),
            other_compute: metrics
                .get("other")
                .map(|m| to_partition_stats(m))
                .filter(|v| !v.is_empty()),
        })
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

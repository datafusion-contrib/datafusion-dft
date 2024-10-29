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
        filter::FilterExec, metrics::MetricValue, projection::ProjectionExec,
        sorts::sort::SortExec, visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor,
    },
};
use itertools::Itertools;
use std::{sync::Arc, time::Duration};

#[derive(Clone, Debug)]
pub struct ExecutionStats {
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
        durations: ExecutionDurationStats,
        rows: usize,
        batches: i32,
        bytes: usize,
        plan: Arc<dyn ExecutionPlan>,
    ) -> color_eyre::Result<Self> {
        Ok(Self {
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

    pub fn selectivity(&self) -> f64 {
        let bytes_scanned = self.io.as_ref().and_then(|io| io.bytes_scanned.clone());
        if let Some(bytes_scanned) = bytes_scanned {
            bytes_scanned.as_usize() as f64 / self.bytes as f64
        } else {
            0.0
        }
    }
}

impl std::fmt::Display for ExecutionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "==================== Execution Summary ===================="
        )?;
        writeln!(
            f,
            "{:<20} {:<20} {:<20}",
            "Rows Returned", "Batches Processed", "Selectivity"
        )?;
        writeln!(
            f,
            "{:<20} {:<20} {:<20.2}",
            self.rows,
            self.batches,
            self.selectivity()
        )?;
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
        writeln!(f)?;
        self.display_compute(f, &self.filter_compute, "Filter")?;
        writeln!(f)?;
        self.display_compute(f, &self.sort_compute, "Sort")?;
        writeln!(f)?;
        self.display_compute(f, &self.projection_compute, "Projection")?;
        // if let (Some(filter_compute), Some(elapsed_compute)) =
        //     (&self.filter_compute, &self.elapsed_compute)
        // {
        //     let partitions = filter_compute.iter().fold(0, |acc, c| acc + c.partitions());
        //     writeln!(
        //         f,
        //         "Filter Stats ({} nodes, {} partitions)",
        //         filter_compute.len(),
        //         partitions
        //     )?;
        //     filter_compute.iter().try_for_each(|node| {
        //         let (min, median, mean, max, total) = node.summary_stats();
        //         writeln!(
        //             f,
        //             "{:<18} {:<18} {:<18} {:<18} {:<18}",
        //             "Min", "Median", "Mean", "Max", "Total (%)"
        //         )?;
        //         let total = format!(
        //             "{} ({:.2}%)",
        //             total,
        //             (total as f32 / *elapsed_compute as f32) * 100.0
        //         );
        //         writeln!(
        //             f,
        //             "{:<18} {:<18} {:<18} {:<18} {:<18}",
        //             min, median, mean, max, total,
        //         )?;
        //         Ok(())
        //     })?
        // } else {
        //     writeln!(f, "No Filter Stats")?;
        // }
        writeln!(f)
    }
}

#[derive(Default)]
pub struct PlanComputeVisitor {
    elapsed_compute: Option<usize>,
    filter_computes: Vec<PartitionsComputeStats>,
    sort_computes: Vec<PartitionsComputeStats>,
    projection_computes: Vec<PartitionsComputeStats>,
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
    }

    fn collect_filter_metrics(&mut self, plan: &dyn ExecutionPlan) {
        if plan.as_any().downcast_ref::<FilterExec>().is_some() {
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
        if plan.as_any().downcast_ref::<SortExec>().is_some() {
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
        if plan.as_any().downcast_ref::<ProjectionExec>().is_some() {
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
}

impl From<PlanComputeVisitor> for ExecutionComputeStats {
    fn from(value: PlanComputeVisitor) -> Self {
        Self {
            elapsed_compute: value.elapsed_compute,
            filter_compute: Some(value.filter_computes),
            sort_compute: Some(value.sort_computes),
            projection_compute: Some(value.projection_computes),
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

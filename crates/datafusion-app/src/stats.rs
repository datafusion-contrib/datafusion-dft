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
    datasource::{
        physical_plan::{ArrowSource, CsvSource, FileScanConfig, JsonSource, ParquetSource},
        source::DataSourceExec,
    },
    physical_plan::{
        aggregates::AggregateExec,
        filter::FilterExec,
        joins::{
            CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec,
            SymmetricHashJoinExec,
        },
        limit::{GlobalLimitExec, LocalLimitExec},
        metrics::{MetricValue, MetricsSet},
        projection::ProjectionExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
        windows::{BoundedWindowAggExec, WindowAggExec},
        ExecutionPlan,
    },
};
use itertools::Itertools;
use log::debug;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};

/// Version of the analyze protocol implemented by this crate. Carried in the
/// metrics batch schema metadata under [`PROTOCOL_VERSION_METADATA_KEY`].
pub const ANALYZE_PROTOCOL_VERSION: &str = "0.1";

/// Schema metadata key holding the protocol version of the metrics batch
pub const PROTOCOL_VERSION_METADATA_KEY: &str = "analyze.protocol_version";

/// Schema metadata key holding an opaque per-request correlation id
pub const QUERY_ID_METADATA_KEY: &str = "analyze.query_id";

/// Request structure for the analyze_query action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzeQueryRequest {
    /// SQL query to analyze (currently the only supported format)
    pub sql: Option<String>,
    /// Protocol version the client speaks. Servers reject unknown major versions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<String>,
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
            protocol_version: Some(ANALYZE_PROTOCOL_VERSION.to_string()),
        }
    }

    /// Get the SQL query, returning an error if not present
    pub fn sql(&self) -> color_eyre::Result<&str> {
        self.sql
            .as_deref()
            .ok_or_else(|| color_eyre::eyre::eyre!("sql field is required"))
    }
}

/// Returns true when `version` is compatible with the protocol version this
/// crate implements (same major version)
pub fn is_compatible_protocol_version(version: &str) -> bool {
    let major = |v: &str| v.split('.').next().map(str::to_string);
    major(version) == major(ANALYZE_PROTOCOL_VERSION)
}

/// Operator categories used to group metrics. This enum is the single source
/// of truth for classification, wire encoding, and display ordering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OperatorCategory {
    Io,
    Projection,
    Filter,
    Sort,
    Aggregate,
    Join,
    Window,
    Distinct,
    Limit,
    Union,
    Other,
}

impl OperatorCategory {
    /// Compute categories in display order (everything except `Io`)
    pub const COMPUTE: [OperatorCategory; 10] = [
        OperatorCategory::Projection,
        OperatorCategory::Filter,
        OperatorCategory::Sort,
        OperatorCategory::Aggregate,
        OperatorCategory::Join,
        OperatorCategory::Window,
        OperatorCategory::Distinct,
        OperatorCategory::Limit,
        OperatorCategory::Union,
        OperatorCategory::Other,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            OperatorCategory::Io => "io",
            OperatorCategory::Projection => "projection",
            OperatorCategory::Filter => "filter",
            OperatorCategory::Sort => "sort",
            OperatorCategory::Aggregate => "aggregate",
            OperatorCategory::Join => "join",
            OperatorCategory::Window => "window",
            OperatorCategory::Distinct => "distinct",
            OperatorCategory::Limit => "limit",
            OperatorCategory::Union => "union",
            OperatorCategory::Other => "other",
        }
    }

    /// Display label for the category
    fn label(&self) -> &'static str {
        match self {
            OperatorCategory::Io => "IO",
            OperatorCategory::Projection => "Projection",
            OperatorCategory::Filter => "Filter",
            OperatorCategory::Sort => "Sort",
            OperatorCategory::Aggregate => "Aggregate",
            OperatorCategory::Join => "Join",
            OperatorCategory::Window => "Window",
            OperatorCategory::Distinct => "Distinct",
            OperatorCategory::Limit => "Limit",
            OperatorCategory::Union => "Union",
            OperatorCategory::Other => "Other",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "io" => Some(OperatorCategory::Io),
            "projection" => Some(OperatorCategory::Projection),
            "filter" => Some(OperatorCategory::Filter),
            "sort" => Some(OperatorCategory::Sort),
            "aggregate" => Some(OperatorCategory::Aggregate),
            "join" => Some(OperatorCategory::Join),
            "window" => Some(OperatorCategory::Window),
            "distinct" => Some(OperatorCategory::Distinct),
            "limit" => Some(OperatorCategory::Limit),
            "union" => Some(OperatorCategory::Union),
            "other" => Some(OperatorCategory::Other),
            _ => None,
        }
    }

    /// Classify a plan node. Downcasts are used where the operator type is
    /// public; a name-based fallback covers the remaining cases so operators
    /// from newer DataFusion versions still land in a sensible category.
    fn classify(plan: &dyn ExecutionPlan) -> Self {
        if io_format(plan).is_some() {
            return OperatorCategory::Io;
        }
        let any = plan.as_any();
        if any.downcast_ref::<ProjectionExec>().is_some() {
            return OperatorCategory::Projection;
        }
        if any.downcast_ref::<FilterExec>().is_some() {
            return OperatorCategory::Filter;
        }
        if any.downcast_ref::<SortExec>().is_some()
            || any.downcast_ref::<SortPreservingMergeExec>().is_some()
        {
            return OperatorCategory::Sort;
        }
        if any.downcast_ref::<AggregateExec>().is_some() {
            return OperatorCategory::Aggregate;
        }
        if any.downcast_ref::<HashJoinExec>().is_some()
            || any.downcast_ref::<CrossJoinExec>().is_some()
            || any.downcast_ref::<SortMergeJoinExec>().is_some()
            || any.downcast_ref::<NestedLoopJoinExec>().is_some()
            || any.downcast_ref::<SymmetricHashJoinExec>().is_some()
        {
            return OperatorCategory::Join;
        }
        if any.downcast_ref::<WindowAggExec>().is_some()
            || any.downcast_ref::<BoundedWindowAggExec>().is_some()
        {
            return OperatorCategory::Window;
        }
        if any.downcast_ref::<GlobalLimitExec>().is_some()
            || any.downcast_ref::<LocalLimitExec>().is_some()
        {
            return OperatorCategory::Limit;
        }
        if any.downcast_ref::<UnionExec>().is_some() {
            return OperatorCategory::Union;
        }
        // Name-based fallback for operators without a public type
        let name = plan.name();
        if name.contains("Window") {
            return OperatorCategory::Window;
        }
        if name.contains("Distinct") || name.contains("Deduplicate") {
            return OperatorCategory::Distinct;
        }
        OperatorCategory::Other
    }
}

impl std::fmt::Display for OperatorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Represents the file format type for I/O operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IOFormatType {
    Csv,
    Parquet,
    Arrow,
    Json,
}

impl IOFormatType {
    fn namespace_prefix(&self) -> &'static str {
        match self {
            IOFormatType::Csv => "io.csv",
            IOFormatType::Parquet => "io.parquet",
            IOFormatType::Arrow => "io.arrow",
            IOFormatType::Json => "io.json",
        }
    }

    fn from_namespace(namespace: &str) -> Option<Self> {
        match namespace {
            "csv" => Some(IOFormatType::Csv),
            "parquet" => Some(IOFormatType::Parquet),
            "arrow" => Some(IOFormatType::Arrow),
            "json" => Some(IOFormatType::Json),
            _ => None,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            IOFormatType::Csv => "CSV",
            IOFormatType::Parquet => "Parquet",
            IOFormatType::Arrow => "Arrow",
            IOFormatType::Json => "JSON",
        }
    }
}

/// Determine the I/O format of a plan node, if it is a file scan. In
/// DataFusion 51 all file scans are `DataSourceExec` nodes wrapping a
/// `FileScanConfig` that carries the format-specific `FileSource`.
fn io_format(plan: &dyn ExecutionPlan) -> Option<IOFormatType> {
    let data_source_exec = plan.as_any().downcast_ref::<DataSourceExec>()?;
    let file_scan_config = data_source_exec
        .data_source()
        .as_any()
        .downcast_ref::<FileScanConfig>()?;
    let any = file_scan_config.file_source().as_any();
    if any.downcast_ref::<ParquetSource>().is_some() {
        Some(IOFormatType::Parquet)
    } else if any.downcast_ref::<CsvSource>().is_some() {
        Some(IOFormatType::Csv)
    } else if any.downcast_ref::<JsonSource>().is_some() {
        Some(IOFormatType::Json)
    } else if any.downcast_ref::<ArrowSource>().is_some() {
        Some(IOFormatType::Arrow)
    } else {
        None
    }
}

/// Identity and category of a single node in the execution plan. Node ids are
/// assigned by pre-order traversal with the root as 0, so a plan's DAG can be
/// reconstructed from `(node_id, parent_node_id)` pairs even when the same
/// operator type appears multiple times.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanNodeInfo {
    node_id: i32,
    parent_node_id: Option<i32>,
    name: String,
    category: OperatorCategory,
}

impl PlanNodeInfo {
    pub fn node_id(&self) -> i32 {
        self.node_id
    }

    pub fn parent_node_id(&self) -> Option<i32> {
        self.parent_node_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn category(&self) -> OperatorCategory {
        self.category
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionStats {
    query: String,
    rows: usize,
    batches: usize,
    bytes: usize,
    durations: ExecutionDurationStats,
    io: Option<ExecutionIOStats>,
    compute: Option<ExecutionComputeStats>,
    /// The executed physical plan. `None` when the stats were reconstructed
    /// from a metrics table (e.g. received over FlightSQL).
    plan: Option<Arc<dyn ExecutionPlan>>,
    nodes: Vec<PlanNodeInfo>,
}

impl ExecutionStats {
    pub fn try_new(
        query: String,
        durations: ExecutionDurationStats,
        rows: usize,
        batches: usize,
        bytes: usize,
        plan: Arc<dyn ExecutionPlan>,
    ) -> color_eyre::Result<Self> {
        let collected = collect_node_stats(&plan);
        Ok(Self {
            query,
            durations,
            rows,
            batches,
            bytes,
            plan: Some(plan),
            io: None,
            compute: None,
            nodes: collected.nodes,
        })
    }

    /// Collect I/O and compute metrics from the executed plan. No-op when the
    /// stats were reconstructed from a metrics table (no plan available).
    pub fn collect_stats(&mut self) {
        let Some(plan) = &self.plan else { return };
        let collected = collect_node_stats(plan);
        self.nodes = collected.nodes;
        if !collected.io_nodes.is_empty() {
            self.io = Some(ExecutionIOStats {
                nodes: collected.io_nodes,
            });
        }
        self.compute = Some(ExecutionComputeStats {
            elapsed_compute: collected.elapsed_compute,
            computes: collected.computes,
        });
    }

    pub fn nodes(&self) -> &[PlanNodeInfo] {
        &self.nodes
    }

    /// Fraction of scanned rows that made it into the query result. `None`
    /// when no scan output-row metrics are available.
    pub fn rows_selectivity(&self) -> Option<f64> {
        let scan_rows: u64 = self
            .io
            .as_ref()?
            .nodes
            .iter()
            .filter_map(|n| n.output_rows)
            .sum();
        (scan_rows > 0).then(|| self.rows as f64 / scan_rows as f64)
    }

    /// Ratio of result bytes (in-memory Arrow size) to bytes scanned. `None`
    /// when no bytes-scanned metrics are available.
    pub fn bytes_selectivity(&self) -> Option<f64> {
        let scanned: u64 = self
            .io
            .as_ref()?
            .nodes
            .iter()
            .filter_map(|n| n.bytes_scanned)
            .sum();
        (scanned > 0).then(|| self.bytes as f64 / scanned as f64)
    }

    /// Ratio of the Parquet row-group pruning rate to row selectivity. Higher
    /// values mean pruning removed more data relative to how selective the
    /// query was. `None` when either input is unavailable.
    pub fn selectivity_efficiency(&self) -> Option<f64> {
        let matched_ratio = self.io.as_ref()?.parquet_rg_matched_ratio()?;
        let selectivity = self.rows_selectivity()?;
        (selectivity > 0.0).then(|| matched_ratio / selectivity)
    }
}

fn fmt_opt_ratio(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.2}", v))
        .unwrap_or_else(|| "N/A".to_string())
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
            format!("{} ({})", self.rows, fmt_opt_ratio(self.rows_selectivity())),
            format!(
                "{} ({})",
                self.bytes,
                fmt_opt_ratio(self.bytes_selectivity())
            ),
            self.batches,
        )?;
        writeln!(f)?;
        writeln!(f, "{}", self.durations)?;
        writeln!(f, "{:<20}", "Parquet Efficiency (Pruning / Selectivity)")?;
        writeln!(f, "{:<20}", fmt_opt_ratio(self.selectivity_efficiency()))?;
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

/// I/O metrics for a single scan node. Values are plain integers so they
/// round-trip losslessly through the metrics table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IONodeStats {
    node_id: Option<i32>,
    operator_name: String,
    format: IOFormatType,
    bytes_scanned: Option<u64>,
    time_opening_ns: Option<u64>,
    time_scanning_ns: Option<u64>,
    // Parquet-specific pruning metrics
    output_rows: Option<u64>,
    rg_pruned: Option<u64>,
    rg_matched: Option<u64>,
    bloom_pruned: Option<u64>,
    bloom_matched: Option<u64>,
    page_index_pruned: Option<u64>,
    page_index_matched: Option<u64>,
}

impl IONodeStats {
    fn matched_ratio(pruned: Option<u64>, matched: Option<u64>) -> Option<f64> {
        let (pruned, matched) = (pruned?, matched?);
        let total = pruned + matched;
        (total > 0).then(|| matched as f64 / total as f64)
    }

    fn rg_matched_ratio(&self) -> Option<f64> {
        Self::matched_ratio(self.rg_pruned, self.rg_matched)
    }

    fn bloom_matched_ratio(&self) -> Option<f64> {
        Self::matched_ratio(self.bloom_pruned, self.bloom_matched)
    }

    fn page_index_matched_ratio(&self) -> Option<f64> {
        Self::matched_ratio(self.page_index_pruned, self.page_index_matched)
    }

    fn row_group_count(&self) -> Option<u64> {
        match (self.rg_pruned, self.rg_matched) {
            (Some(p), Some(m)) => Some(p + m),
            _ => None,
        }
    }
}

impl std::fmt::Display for IONodeStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let node = self
            .node_id
            .map(|id| format!("node {id}"))
            .unwrap_or_else(|| "node ?".to_string());
        writeln!(
            f,
            "{} ({}) [{}]",
            self.operator_name,
            node,
            self.format.label()
        )?;
        writeln!(
            f,
            "{:<20} {:<20} {:<20}",
            "Bytes Scanned", "Time Opening", "Time Scanning"
        )?;
        let fmt_opt_u64 = |v: Option<u64>| v.map(|v| v.to_string()).unwrap_or("None".to_string());
        let fmt_opt_ns = |v: Option<u64>| {
            v.map(|v| format!("{:?}", Duration::from_nanos(v)))
                .unwrap_or("None".to_string())
        };
        writeln!(
            f,
            "{:<20} {:<20} {:<20}",
            fmt_opt_u64(self.bytes_scanned),
            fmt_opt_ns(self.time_opening_ns),
            fmt_opt_ns(self.time_scanning_ns),
        )?;
        if self.format == IOFormatType::Parquet {
            let scan_time_per_rg = match (self.time_scanning_ns, self.row_group_count()) {
                (Some(ns), Some(rgs)) if rgs > 0 => {
                    format!(
                        "{:.2}ms scan time per row group",
                        ns as f64 / 1e6 / rgs as f64
                    )
                }
                _ => "N/A".to_string(),
            };
            writeln!(
                f,
                "Parquet Pruning Stats (Output Rows: {}, Row Groups: {} [{}])",
                fmt_opt_u64(self.output_rows),
                self.row_group_count()
                    .map(|v| v.to_string())
                    .unwrap_or("None".to_string()),
                scan_time_per_rg,
            )?;
            writeln!(
                f,
                "{:<20} {:<20} {:<20}",
                "Matched RG Stats", "Matched RG Bloom", "Matched Page Index"
            )?;
            writeln!(
                f,
                "{:<20} {:<20} {:<20}",
                fmt_opt_ratio(self.rg_matched_ratio()),
                fmt_opt_ratio(self.bloom_matched_ratio()),
                fmt_opt_ratio(self.page_index_matched_ratio()),
            )?;
        }
        Ok(())
    }
}

/// I/O metrics for all scan nodes in the plan
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionIOStats {
    nodes: Vec<IONodeStats>,
}

impl ExecutionIOStats {
    pub fn nodes(&self) -> &[IONodeStats] {
        &self.nodes
    }

    /// Aggregate Parquet row-group matched ratio across all scan nodes
    fn parquet_rg_matched_ratio(&self) -> Option<f64> {
        let mut pruned = 0u64;
        let mut matched = 0u64;
        let mut any = false;
        for node in &self.nodes {
            if let (Some(p), Some(m)) = (node.rg_pruned, node.rg_matched) {
                pruned += p;
                matched += m;
                any = true;
            }
        }
        if !any {
            return None;
        }
        IONodeStats::matched_ratio(Some(pruned), Some(matched))
    }
}

impl std::fmt::Display for ExecutionIOStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "======================= IO Summary ========================"
        )?;
        for (i, node) in self.nodes.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{}", node)?;
        }
        Ok(())
    }
}

/// Per-partition elapsed-compute values for a single plan node.
///
/// `elapsed_computes` is sorted ascending; the wire `partition_id` is the rank
/// in this ordering, not the physical partition number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionsComputeStats {
    node_id: Option<i32>,
    name: String,
    category: OperatorCategory,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionComputeStats {
    elapsed_compute: Option<usize>,
    computes: Vec<PartitionsComputeStats>,
}

impl ExecutionComputeStats {
    fn display_category(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        category: OperatorCategory,
    ) -> std::fmt::Result {
        let nodes: Vec<&PartitionsComputeStats> = self
            .computes
            .iter()
            .filter(|c| c.category == category)
            .collect();
        if nodes.is_empty() {
            return writeln!(f, "{}: No data", category.label());
        }
        let partitions = nodes.iter().fold(0, |acc, c| acc + c.partitions());
        writeln!(
            f,
            "{}: {} nodes, {} partitions",
            category.label(),
            nodes.len(),
            partitions
        )?;
        writeln!(
            f,
            "{:<30} {:<16} {:<16} {:<16} {:<16} {:<16}",
            "Node(Partitions)", "Min", "Median", "Mean", "Max", "Total (%)"
        )?;
        nodes.iter().try_for_each(|node| {
            let (min, median, mean, max, total) = node.summary_stats();
            let total = match &self.elapsed_compute {
                Some(elapsed) if *elapsed > 0 => format!(
                    "{} ({:.2}%)",
                    total,
                    (total as f32 / *elapsed as f32) * 100.0
                ),
                _ => total.to_string(),
            };
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

        for category in OperatorCategory::COMPUTE {
            self.display_category(f, category)?;
            writeln!(f)?;
        }
        Ok(())
    }
}

/// Result of walking an execution plan once: node identity plus per-node
/// I/O and compute metrics
#[derive(Default)]
struct CollectedPlanStats {
    nodes: Vec<PlanNodeInfo>,
    io_nodes: Vec<IONodeStats>,
    computes: Vec<PartitionsComputeStats>,
    elapsed_compute: Option<usize>,
}

fn collect_node_stats(plan: &Arc<dyn ExecutionPlan>) -> CollectedPlanStats {
    let mut collected = CollectedPlanStats::default();
    let mut next_id = 0i32;
    walk_plan(plan, None, &mut next_id, &mut collected);
    collected
}

fn walk_plan(
    plan: &Arc<dyn ExecutionPlan>,
    parent_node_id: Option<i32>,
    next_id: &mut i32,
    out: &mut CollectedPlanStats,
) {
    let node_id = *next_id;
    *next_id += 1;

    let category = OperatorCategory::classify(plan.as_ref());
    out.nodes.push(PlanNodeInfo {
        node_id,
        parent_node_id,
        name: plan.name().to_string(),
        category,
    });

    if let Some(metrics) = plan.metrics() {
        if category == OperatorCategory::Io {
            if let Some(format) = io_format(plan.as_ref()) {
                out.io_nodes.push(collect_io_node_stats(
                    node_id,
                    plan.name(),
                    format,
                    &metrics,
                ));
            }
        } else {
            if let Some(node_elapsed) = metrics.elapsed_compute() {
                out.elapsed_compute = Some(out.elapsed_compute.unwrap_or(0) + node_elapsed);
            }
            let sorted_computes: Vec<usize> = metrics
                .iter()
                .filter_map(|m| match m.value() {
                    MetricValue::ElapsedCompute(t) => Some(t.value()),
                    _ => None,
                })
                .sorted()
                .collect();
            if !sorted_computes.is_empty() {
                out.computes.push(PartitionsComputeStats {
                    node_id: Some(node_id),
                    name: plan.name().to_string(),
                    category,
                    elapsed_computes: sorted_computes,
                });
            }
        }
    }

    for child in plan.children() {
        walk_plan(child, Some(node_id), next_id, out);
    }
}

/// Sum a named metric across partitions, as a plain integer
fn metric_u64(metrics: &MetricsSet, name: &str) -> Option<u64> {
    metrics.sum_by_name(name).map(|v| v.as_usize() as u64)
}

/// Sum a named pruning metric across partitions, returning `(pruned, matched)`.
/// `PruningMetrics` cannot be read via `as_usize` (it always returns 0).
fn pruning_counts(metrics: &MetricsSet, name: &str) -> Option<(u64, u64)> {
    metrics.sum_by_name(name).and_then(|v| match v {
        MetricValue::PruningMetrics {
            pruning_metrics, ..
        } => Some((
            pruning_metrics.pruned() as u64,
            pruning_metrics.matched() as u64,
        )),
        _ => None,
    })
}

fn collect_io_node_stats(
    node_id: i32,
    operator_name: &str,
    format: IOFormatType,
    metrics: &MetricsSet,
) -> IONodeStats {
    let mut stats = IONodeStats {
        node_id: Some(node_id),
        operator_name: operator_name.to_string(),
        format,
        bytes_scanned: metric_u64(metrics, "bytes_scanned"),
        time_opening_ns: metric_u64(metrics, "time_elapsed_opening"),
        time_scanning_ns: metric_u64(metrics, "time_elapsed_scanning_total"),
        output_rows: None,
        rg_pruned: None,
        rg_matched: None,
        bloom_pruned: None,
        bloom_matched: None,
        page_index_pruned: None,
        page_index_matched: None,
    };
    if format == IOFormatType::Parquet {
        stats.output_rows = metrics.output_rows().map(|v| v as u64);
        if let Some((pruned, matched)) = pruning_counts(metrics, "row_groups_pruned_statistics") {
            stats.rg_pruned = Some(pruned);
            stats.rg_matched = Some(matched);
        }
        if let Some((pruned, matched)) = pruning_counts(metrics, "row_groups_pruned_bloom_filter") {
            stats.bloom_pruned = Some(pruned);
            stats.bloom_matched = Some(matched);
        }
        if let Some((pruned, matched)) = pruning_counts(metrics, "page_index_rows_pruned") {
            stats.page_index_pruned = Some(pruned);
            stats.page_index_matched = Some(matched);
        }
    }
    stats
}

pub fn collect_plan_io_stats(plan: Arc<dyn ExecutionPlan>) -> Option<ExecutionIOStats> {
    let collected = collect_node_stats(&plan);
    if collected.io_nodes.is_empty() {
        None
    } else {
        Some(ExecutionIOStats {
            nodes: collected.io_nodes,
        })
    }
}

pub fn collect_plan_compute_stats(plan: Arc<dyn ExecutionPlan>) -> Option<ExecutionComputeStats> {
    let collected = collect_node_stats(&plan);
    Some(ExecutionComputeStats {
        elapsed_compute: collected.elapsed_compute,
        computes: collected.computes,
    })
}

/// Standard Arrow schema for analyze metrics. The schema metadata carries the
/// protocol version under [`PROTOCOL_VERSION_METADATA_KEY`].
pub fn analyze_metrics_schema() -> SchemaRef {
    let fields = vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value", DataType::UInt64, false),
        Field::new("value_type", DataType::Utf8, false),
        Field::new("operator_name", DataType::Utf8, true),
        Field::new("partition_id", DataType::Int32, true),
        Field::new("operator_category", DataType::Utf8, true),
        Field::new("node_id", DataType::Int32, true),
        Field::new("parent_node_id", DataType::Int32, true),
    ];
    let metadata = HashMap::from([(
        PROTOCOL_VERSION_METADATA_KEY.to_string(),
        ANALYZE_PROTOCOL_VERSION.to_string(),
    )]);
    Arc::new(Schema::new_with_metadata(fields, metadata))
}

/// A single row of the metrics table
struct MetricRow<'a> {
    metric_name: &'a str,
    value: u64,
    value_type: &'a str,
    operator_name: Option<&'a str>,
    partition_id: Option<i32>,
    operator_category: Option<OperatorCategory>,
    node_id: Option<i32>,
    parent_node_id: Option<i32>,
}

impl<'a> MetricRow<'a> {
    /// A query- or stage-level metric, not attached to any operator
    fn query_level(metric_name: &'a str, value: u64, value_type: &'a str) -> Self {
        Self {
            metric_name,
            value,
            value_type,
            operator_name: None,
            partition_id: None,
            operator_category: None,
            node_id: None,
            parent_node_id: None,
        }
    }
}

/// Helper to build metrics table rows
struct MetricsTableBuilder {
    metric_names: Vec<String>,
    values: Vec<u64>,
    value_types: Vec<String>,
    operator_names: Vec<Option<String>>,
    partition_ids: Vec<Option<i32>>,
    operator_categories: Vec<Option<String>>,
    node_ids: Vec<Option<i32>>,
    parent_node_ids: Vec<Option<i32>>,
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
            node_ids: Vec::new(),
            parent_node_ids: Vec::new(),
        }
    }

    fn add(&mut self, row: MetricRow<'_>) {
        self.metric_names.push(row.metric_name.to_string());
        self.values.push(row.value);
        self.value_types.push(row.value_type.to_string());
        self.operator_names
            .push(row.operator_name.map(String::from));
        self.partition_ids.push(row.partition_id);
        self.operator_categories
            .push(row.operator_category.map(|c| c.as_str().to_string()));
        self.node_ids.push(row.node_id);
        self.parent_node_ids.push(row.parent_node_id);
    }

    fn build(self, schema: SchemaRef) -> color_eyre::Result<RecordBatch> {
        let metric_names_array: ArrayRef = Arc::new(StringArray::from(self.metric_names));
        let values_array: ArrayRef = Arc::new(UInt64Array::from(self.values));
        let value_types_array: ArrayRef = Arc::new(StringArray::from(self.value_types));
        let operator_names_array: ArrayRef = Arc::new(StringArray::from(self.operator_names));
        let partition_ids_array: ArrayRef = Arc::new(Int32Array::from(self.partition_ids));
        let operator_categories_array: ArrayRef =
            Arc::new(StringArray::from(self.operator_categories));
        let node_ids_array: ArrayRef = Arc::new(Int32Array::from(self.node_ids));
        let parent_node_ids_array: ArrayRef = Arc::new(Int32Array::from(self.parent_node_ids));

        Ok(RecordBatch::try_new(
            schema,
            vec![
                metric_names_array,
                values_array,
                value_types_array,
                operator_names_array,
                partition_ids_array,
                operator_categories_array,
                node_ids_array,
                parent_node_ids_array,
            ],
        )?)
    }
}

impl ExecutionStats {
    /// Serialize ExecutionStats to metrics table format
    pub fn to_metrics_table(&self) -> color_eyre::Result<RecordBatch> {
        let schema = analyze_metrics_schema();
        let mut rows = MetricsTableBuilder::new();

        rows.add(MetricRow::query_level(
            "query.rows",
            self.rows as u64,
            "count",
        ));
        rows.add(MetricRow::query_level(
            "query.batches",
            self.batches as u64,
            "count",
        ));
        rows.add(MetricRow::query_level(
            "query.bytes",
            self.bytes as u64,
            "bytes",
        ));

        rows.add(MetricRow::query_level(
            "stage.parsing",
            self.durations.parsing.as_nanos() as u64,
            "duration_ns",
        ));
        rows.add(MetricRow::query_level(
            "stage.logical_planning",
            self.durations.logical_planning.as_nanos() as u64,
            "duration_ns",
        ));
        rows.add(MetricRow::query_level(
            "stage.physical_planning",
            self.durations.physical_planning.as_nanos() as u64,
            "duration_ns",
        ));
        rows.add(MetricRow::query_level(
            "stage.execution",
            self.durations.execution.as_nanos() as u64,
            "duration_ns",
        ));
        rows.add(MetricRow::query_level(
            "stage.total",
            self.durations.total.as_nanos() as u64,
            "duration_ns",
        ));

        // Map node_id -> parent_node_id for operator-level rows
        let parents: HashMap<i32, Option<i32>> = self
            .nodes
            .iter()
            .map(|n| (n.node_id, n.parent_node_id))
            .collect();
        let parent_of =
            |node_id: Option<i32>| node_id.and_then(|id| parents.get(&id).copied()).flatten();

        if let Some(io) = &self.io {
            for node in &io.nodes {
                let namespace = node.format.namespace_prefix();
                let parent_node_id = parent_of(node.node_id);
                let mut add_io_metric = |suffix: &str, value: Option<u64>, value_type: &str| {
                    if let Some(value) = value {
                        rows.add(MetricRow {
                            metric_name: &format!("{namespace}.{suffix}"),
                            value,
                            value_type,
                            operator_name: Some(&node.operator_name),
                            partition_id: None,
                            operator_category: Some(OperatorCategory::Io),
                            node_id: node.node_id,
                            parent_node_id,
                        });
                    }
                };
                add_io_metric("bytes_scanned", node.bytes_scanned, "bytes");
                add_io_metric("time_opening", node.time_opening_ns, "duration_ns");
                add_io_metric("time_scanning", node.time_scanning_ns, "duration_ns");
                if node.format == IOFormatType::Parquet {
                    add_io_metric("output_rows", node.output_rows, "count");
                    add_io_metric("rg_pruned", node.rg_pruned, "count");
                    add_io_metric("rg_matched", node.rg_matched, "count");
                    add_io_metric("bloom_pruned", node.bloom_pruned, "count");
                    add_io_metric("bloom_matched", node.bloom_matched, "count");
                    add_io_metric("page_index_pruned", node.page_index_pruned, "count");
                    add_io_metric("page_index_matched", node.page_index_matched, "count");
                }
            }
        }

        if let Some(compute) = &self.compute {
            if let Some(elapsed) = compute.elapsed_compute {
                rows.add(MetricRow::query_level(
                    "compute.elapsed_compute",
                    elapsed as u64,
                    "duration_ns",
                ));
            }

            for node in &compute.computes {
                let parent_node_id = parent_of(node.node_id);
                for (partition_id, elapsed) in node.elapsed_computes.iter().enumerate() {
                    rows.add(MetricRow {
                        metric_name: "compute.elapsed_compute",
                        value: *elapsed as u64,
                        value_type: "duration_ns",
                        operator_name: Some(&node.name),
                        partition_id: Some(partition_id as i32),
                        operator_category: Some(node.category),
                        node_id: node.node_id,
                        parent_node_id,
                    });
                }
            }
        }

        rows.build(schema)
    }

    /// Deserialize ExecutionStats from a metrics table
    pub fn from_metrics_table(batch: RecordBatch, query: String) -> color_eyre::Result<Self> {
        let column_string = |idx: usize, name: &str| -> color_eyre::Result<&StringArray> {
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| color_eyre::eyre::eyre!("Invalid {name} column type"))
        };
        let column_i32 = |idx: usize, name: &str| -> color_eyre::Result<&Int32Array> {
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| color_eyre::eyre::eyre!("Invalid {name} column type"))
        };

        let metric_names = column_string(0, "metric_name")?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| color_eyre::eyre::eyre!("Invalid value column type"))?;
        let operator_names = column_string(3, "operator_name")?;
        let partition_ids = column_i32(4, "partition_id")?;
        let operator_categories = column_string(5, "operator_category")?;
        let node_ids = column_i32(6, "node_id")?;
        let parent_node_ids = column_i32(7, "parent_node_id")?;

        let opt_str = |arr: &StringArray, idx: usize| -> Option<String> {
            (!arr.is_null(idx)).then(|| arr.value(idx).to_string())
        };
        let opt_i32 = |arr: &Int32Array, idx: usize| -> Option<i32> {
            (!arr.is_null(idx)).then(|| arr.value(idx))
        };

        let mut rows = 0usize;
        let mut batches = 0usize;
        let mut bytes = 0usize;
        let mut parsing = Duration::ZERO;
        let mut logical_planning = Duration::ZERO;
        let mut physical_planning = Duration::ZERO;
        let mut execution = Duration::ZERO;
        let mut total = Duration::ZERO;
        let mut elapsed_compute: Option<usize> = None;

        // Operator identity reconstructed from the table
        let mut node_map: HashMap<i32, PlanNodeInfo> = HashMap::new();
        // (node_id, operator_name) -> per-partition compute values
        type ComputeKey = (Option<i32>, String);
        let mut compute_map: HashMap<ComputeKey, (OperatorCategory, Vec<(i32, u64)>)> =
            HashMap::new();
        // (node_id, operator_name) -> (format, metric suffix -> value)
        let mut io_map: HashMap<ComputeKey, (IOFormatType, HashMap<String, u64>)> = HashMap::new();
        let mut io_order: Vec<ComputeKey> = Vec::new();

        for row_idx in 0..batch.num_rows() {
            let metric_name = metric_names.value(row_idx);
            let value = values.value(row_idx);
            let operator_name = opt_str(operator_names, row_idx);
            let partition_id = opt_i32(partition_ids, row_idx);
            let category =
                opt_str(operator_categories, row_idx).and_then(|c| OperatorCategory::from_str(&c));
            let node_id = opt_i32(node_ids, row_idx);
            let parent_node_id = opt_i32(parent_node_ids, row_idx);

            if let (Some(id), Some(name), Some(cat)) = (node_id, &operator_name, category) {
                node_map.entry(id).or_insert_with(|| PlanNodeInfo {
                    node_id: id,
                    parent_node_id,
                    name: name.clone(),
                    category: cat,
                });
            }

            match (metric_name, category) {
                ("query.rows", None) => rows = value as usize,
                ("query.batches", None) => batches = value as usize,
                ("query.bytes", None) => bytes = value as usize,
                ("stage.parsing", None) => parsing = Duration::from_nanos(value),
                ("stage.logical_planning", None) => logical_planning = Duration::from_nanos(value),
                ("stage.physical_planning", None) => {
                    physical_planning = Duration::from_nanos(value)
                }
                ("stage.execution", None) => execution = Duration::from_nanos(value),
                ("stage.total", None) => total = Duration::from_nanos(value),
                ("compute.elapsed_compute", None) => elapsed_compute = Some(value as usize),
                ("compute.elapsed_compute", Some(cat)) => {
                    let key = (
                        node_id,
                        operator_name
                            .clone()
                            .unwrap_or_else(|| "Unknown".to_string()),
                    );
                    compute_map
                        .entry(key)
                        .or_insert_with(|| (cat, Vec::new()))
                        .1
                        .push((partition_id.unwrap_or(0), value));
                }
                (name, Some(OperatorCategory::Io)) => {
                    // io.<format>.<metric>
                    let mut parts = name.splitn(3, '.');
                    match (parts.next(), parts.next(), parts.next()) {
                        (Some("io"), Some(fmt), Some(suffix)) => {
                            if let Some(format) = IOFormatType::from_namespace(fmt) {
                                let key = (
                                    node_id,
                                    operator_name
                                        .clone()
                                        .unwrap_or_else(|| "Unknown".to_string()),
                                );
                                let entry = io_map.entry(key.clone()).or_insert_with(|| {
                                    io_order.push(key);
                                    (format, HashMap::new())
                                });
                                entry.1.insert(suffix.to_string(), value);
                            } else {
                                debug!("Unknown io namespace in metric: {}", name);
                            }
                        }
                        _ => debug!("Malformed io metric name: {}", name),
                    }
                }
                (name, category) => {
                    debug!("Unknown metric: {} (category: {:?})", name, category);
                }
            }
        }

        let io_nodes: Vec<IONodeStats> = io_order
            .into_iter()
            .filter_map(|key| {
                let (format, metrics) = io_map.remove(&key)?;
                let (node_id, operator_name) = key;
                let get = |suffix: &str| metrics.get(suffix).copied();
                Some(IONodeStats {
                    node_id,
                    operator_name,
                    format,
                    bytes_scanned: get("bytes_scanned"),
                    time_opening_ns: get("time_opening"),
                    time_scanning_ns: get("time_scanning"),
                    output_rows: get("output_rows"),
                    rg_pruned: get("rg_pruned"),
                    rg_matched: get("rg_matched"),
                    bloom_pruned: get("bloom_pruned"),
                    bloom_matched: get("bloom_matched"),
                    page_index_pruned: get("page_index_pruned"),
                    page_index_matched: get("page_index_matched"),
                })
            })
            .collect();

        let computes: Vec<PartitionsComputeStats> = compute_map
            .into_iter()
            .map(|((node_id, name), (category, mut partitions))| {
                partitions.sort_by_key(|(pid, _)| *pid);
                PartitionsComputeStats {
                    node_id,
                    name,
                    category,
                    elapsed_computes: partitions.iter().map(|(_, v)| *v as usize).collect(),
                }
            })
            .sorted_by_key(|c| (c.node_id, c.name.clone()))
            .collect();

        let io = (!io_nodes.is_empty()).then_some(ExecutionIOStats { nodes: io_nodes });
        let compute =
            (elapsed_compute.is_some() || !computes.is_empty()).then_some(ExecutionComputeStats {
                elapsed_compute,
                computes,
            });

        let nodes: Vec<PlanNodeInfo> = node_map
            .into_values()
            .sorted_by_key(|n| n.node_id)
            .collect();

        Ok(ExecutionStats {
            query,
            rows,
            batches,
            bytes,
            durations: ExecutionDurationStats::new(
                parsing,
                logical_planning,
                physical_planning,
                execution,
                total,
            ),
            io,
            compute,
            plan: None,
            nodes,
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::WriterProperties;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{ParquetReadOptions, SessionContext};

    /// Execute `sql` to completion and return fully collected ExecutionStats,
    /// mirroring the analyze path in `ExecutionContext::analyze_query`
    async fn analyze(ctx: &SessionContext, sql: &str) -> ExecutionStats {
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        let durations = ExecutionDurationStats::new(
            Duration::from_nanos(1),
            Duration::from_nanos(2),
            Duration::from_nanos(3),
            Duration::from_nanos(4),
            Duration::from_nanos(10),
        );
        let mut stats =
            ExecutionStats::try_new(sql.to_string(), durations, rows, batches.len(), bytes, plan)
                .unwrap();
        stats.collect_stats();
        stats
    }

    fn metric_names(batch: &RecordBatch) -> Vec<String> {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| names.value(i).to_string())
            .collect()
    }

    fn categories(batch: &RecordBatch) -> Vec<Option<String>> {
        let cats = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| (!cats.is_null(i)).then(|| cats.value(i).to_string()))
            .collect()
    }

    #[test]
    fn test_protocol_version_compatibility() {
        assert!(is_compatible_protocol_version(ANALYZE_PROTOCOL_VERSION));
        assert!(is_compatible_protocol_version("0.9"));
        assert!(!is_compatible_protocol_version("99.0"));
    }

    #[tokio::test]
    async fn test_node_ids_are_unique_and_preorder() {
        let ctx = SessionContext::new();
        let stats = analyze(
            &ctx,
            "SELECT column2, COUNT(*) FROM (VALUES (1,'a'),(2,'b'),(3,'a')) AS t(column1, column2) GROUP BY column2",
        )
        .await;

        let nodes = stats.nodes();
        assert!(!nodes.is_empty());
        // Pre-order assignment: ids are 0..n in push order, root first
        for (i, node) in nodes.iter().enumerate() {
            assert_eq!(node.node_id(), i as i32);
        }
        assert_eq!(nodes[0].parent_node_id(), None, "root has no parent");
        // Every non-root parent id refers to an existing, earlier node
        for node in &nodes[1..] {
            let parent = node.parent_node_id().expect("non-root node has a parent");
            assert!(parent >= 0 && parent < node.node_id());
        }
    }

    #[tokio::test]
    async fn test_duplicate_operators_stay_distinct() {
        let ctx = SessionContext::new();
        let stats = analyze(
            &ctx,
            "SELECT column2, COUNT(*) FROM (VALUES (1,'a'),(2,'b'),(3,'a')) AS t(column1, column2) GROUP BY column2",
        )
        .await;

        // A GROUP BY plans two AggregateExec nodes (partial + final)
        let compute = stats.compute.as_ref().unwrap();
        let agg_node_ids: Vec<Option<i32>> = compute
            .computes
            .iter()
            .filter(|c| c.name == "AggregateExec")
            .map(|c| c.node_id)
            .collect();
        assert!(
            agg_node_ids.len() >= 2,
            "expected two AggregateExec nodes, got {:?}",
            agg_node_ids
        );
        let distinct: std::collections::HashSet<_> = agg_node_ids.iter().collect();
        assert_eq!(
            distinct.len(),
            agg_node_ids.len(),
            "AggregateExec nodes must have distinct node ids"
        );

        // And they survive the wire round trip as distinct nodes
        let batch = stats.to_metrics_table().unwrap();
        let roundtripped = ExecutionStats::from_metrics_table(batch, stats.query.clone()).unwrap();
        let rt_agg: Vec<Option<i32>> = roundtripped
            .compute
            .as_ref()
            .unwrap()
            .computes
            .iter()
            .filter(|c| c.name == "AggregateExec")
            .map(|c| c.node_id)
            .collect();
        assert_eq!(agg_node_ids, rt_agg);
    }

    #[tokio::test]
    async fn test_metrics_table_round_trip() {
        let ctx = SessionContext::new();
        let stats = analyze(
            &ctx,
            "SELECT column2, COUNT(*) FROM (VALUES (1,'a'),(2,'b'),(3,'a')) AS t(column1, column2) GROUP BY column2",
        )
        .await;

        let batch = stats.to_metrics_table().unwrap();
        assert_eq!(
            batch.schema().metadata().get(PROTOCOL_VERSION_METADATA_KEY),
            Some(&ANALYZE_PROTOCOL_VERSION.to_string())
        );

        let roundtripped = ExecutionStats::from_metrics_table(batch, stats.query.clone()).unwrap();
        assert_eq!(roundtripped.query, stats.query);
        assert_eq!(roundtripped.rows, stats.rows);
        assert_eq!(roundtripped.batches, stats.batches);
        assert_eq!(roundtripped.bytes, stats.bytes);
        assert_eq!(roundtripped.durations, stats.durations);
        assert_eq!(roundtripped.io, stats.io);
        assert_eq!(roundtripped.compute, stats.compute);
        assert!(roundtripped.plan.is_none());
        // Every node referenced by metrics is reconstructed with identical identity
        for node in roundtripped.nodes() {
            let original = stats
                .nodes()
                .iter()
                .find(|n| n.node_id() == node.node_id())
                .expect("reconstructed node exists in original plan");
            assert_eq!(node, original);
        }
    }

    #[tokio::test]
    async fn test_compute_categories_emitted_on_wire() {
        let ctx = SessionContext::new();
        let stats = analyze(
            &ctx,
            "SELECT v, ROW_NUMBER() OVER (ORDER BY v) AS rn FROM \
             (SELECT v FROM (VALUES (1),(2),(3)) t(v) \
              UNION ALL SELECT v FROM (VALUES (4),(5)) u(v)) \
             LIMIT 3",
        )
        .await;

        let batch = stats.to_metrics_table().unwrap();
        let cats: std::collections::HashSet<String> =
            categories(&batch).into_iter().flatten().collect();
        for expected in ["window", "limit", "union"] {
            assert!(
                cats.contains(expected),
                "expected category {expected} on the wire, got {:?}",
                cats
            );
        }

        // And the round trip keeps them
        let roundtripped = ExecutionStats::from_metrics_table(batch, stats.query.clone()).unwrap();
        assert_eq!(roundtripped.compute, stats.compute);
    }

    #[tokio::test]
    async fn test_parquet_io_and_pruning_metrics() {
        // 1000 sorted values in 10 row groups of 100; `v > 950` prunes 9 of
        // 10 row groups via statistics
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from((0..1000).collect::<Vec<i64>>())) as ArrayRef],
        )
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("t.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(100)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let ctx = SessionContext::new();
        ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();

        let stats = analyze(&ctx, "SELECT v FROM t WHERE v > 950").await;
        assert_eq!(stats.rows, 49);

        let io = stats.io.as_ref().expect("io stats collected");
        assert_eq!(io.nodes().len(), 1);
        let node = &io.nodes()[0];
        assert_eq!(node.format, IOFormatType::Parquet);
        assert_eq!(node.operator_name, "DataSourceExec");
        assert!(node.bytes_scanned.unwrap() > 0);
        assert_eq!(node.rg_pruned, Some(9));
        assert_eq!(node.rg_matched, Some(1));
        assert!(node.output_rows.unwrap() > 0);

        // The scan node is classified as io, not compute
        let compute = stats.compute.as_ref().unwrap();
        assert!(
            compute.computes.iter().all(|c| c.name != "DataSourceExec"),
            "scan must not appear as a compute node"
        );

        // Selectivity helpers have data to work with
        assert!(stats.rows_selectivity().is_some());
        assert!(stats.bytes_selectivity().is_some());
        assert!(stats.selectivity_efficiency().is_some());

        // Round trip preserves I/O values exactly
        let table = stats.to_metrics_table().unwrap();
        let names = metric_names(&table);
        for expected in [
            "io.parquet.bytes_scanned",
            "io.parquet.time_opening",
            "io.parquet.time_scanning",
            "io.parquet.output_rows",
            "io.parquet.rg_pruned",
            "io.parquet.rg_matched",
        ] {
            assert!(
                names.iter().any(|n| n == expected),
                "expected {expected} in metrics table"
            );
        }
        let roundtripped = ExecutionStats::from_metrics_table(table, stats.query.clone()).unwrap();
        assert_eq!(roundtripped.io, stats.io);
    }

    #[tokio::test]
    async fn test_multiple_scans_reported_separately() {
        // Two parquet scans in one query (self join) must produce two
        // distinct I/O nodes rather than one overwritten aggregate
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from((0..10).collect::<Vec<i64>>())) as ArrayRef],
        )
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("t.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let ctx = SessionContext::new();
        ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();

        let stats = analyze(&ctx, "SELECT a.v FROM t a JOIN t b ON a.v = b.v").await;
        let io = stats.io.as_ref().expect("io stats collected");
        assert_eq!(io.nodes().len(), 2, "each scan is a separate io node");
        let ids: std::collections::HashSet<_> = io.nodes().iter().map(|n| n.node_id).collect();
        assert_eq!(ids.len(), 2, "scan nodes have distinct node ids");

        let table = stats.to_metrics_table().unwrap();
        let roundtripped = ExecutionStats::from_metrics_table(table, stats.query.clone()).unwrap();
        assert_eq!(roundtripped.io, stats.io);
    }
}

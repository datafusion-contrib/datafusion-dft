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

//! Shared helpers: argument parsing, file opening, layout tree traversal, and
//! conversion of Vortex values to display strings.

use arrow::array::Array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::common::{plan_err, Column};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;
use std::sync::Arc;
use vortex::array::arrays::ConstantArray;
use vortex::array::expr::root;
use vortex::array::serde::SerializedArray;
use vortex::array::{ArrayRef, IntoArray, MaskFuture, VortexSessionExecute};
use vortex::arrow::ArrowSessionExt;
use vortex::error::{VortexError, VortexResult};
use vortex::file::{OpenOptionsSessionExt, VortexFile};
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::layouts::flat::Flat;
use vortex::layout::{LayoutChildType, LayoutReaderContext, LayoutRef};
use vortex::scalar::Scalar;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;

/// Convert a Vortex error into a DataFusion error
pub(crate) fn vx_err(e: VortexError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Extract a string argument from a table function expression, accepting
/// either a single-quoted literal or a double-quoted (column) identifier
pub(crate) fn expr_to_string(expr: Option<&Expr>, func: &str, arg: &str) -> Result<String> {
    match expr {
        Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => Ok(s.clone()),
        Some(Expr::Column(Column { name, .. })) => Ok(name.clone()),
        _ => plan_err!("{func} requires a string {arg} argument"),
    }
}

/// Extract an optional string argument. `None` when the argument is absent.
pub(crate) fn optional_expr_to_string(
    expr: Option<&Expr>,
    func: &str,
    arg: &str,
) -> Result<Option<String>> {
    match expr {
        None => Ok(None),
        Some(_) => expr_to_string(expr, func, arg).map(Some),
    }
}

/// Extract a non-negative integer argument
pub(crate) fn expr_to_u64(expr: &Expr, func: &str, arg: &str) -> Result<u64> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(i)), _) if *i >= 0 => Ok(*i as u64),
        _ => plan_err!("{func} requires a non-negative integer {arg} argument"),
    }
}

/// Reject calls with more arguments than the function accepts
pub(crate) fn check_max_args(exprs: &[Expr], max: usize, func: &str) -> Result<()> {
    if exprs.len() > max {
        return plan_err!(
            "{func} accepts at most {max} argument(s) but {} were given",
            exprs.len()
        );
    }
    Ok(())
}

/// Wrap one record batch in an in-memory table
pub(crate) fn mem_table(schema: SchemaRef, batch: RecordBatch) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

/// An open Vortex file plus the runtime and session that drive its I/O.
///
/// Table functions are planned synchronously, so file I/O is driven to
/// completion on a dedicated [`CurrentThreadRuntime`] on the calling thread.
/// This blocks the thread in the same way that the Parquet table functions
/// block on `std::fs` reads.
pub(crate) struct VortexInput {
    pub runtime: CurrentThreadRuntime,
    pub session: VortexSession,
    pub file: VortexFile,
    pub file_size: u64,
}

impl VortexInput {
    /// Open the Vortex file at `path`. Only the footer is read. When
    /// `include_metadata` is true the user-defined metadata segments are also
    /// read.
    pub fn open(path: &str, include_metadata: bool) -> Result<Self> {
        let file_size = std::fs::metadata(path)?.len();
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        let options = session
            .open_options()
            .with_file_size(file_size)
            .with_include_metadata(include_metadata);
        let file = runtime.block_on(options.open_path(path)).map_err(vx_err)?;
        Ok(Self {
            runtime,
            session,
            file,
            file_size,
        })
    }

    /// Read every row of `layout` into a single array
    pub fn read_layout(&self, layout: &LayoutRef) -> Result<ArrayRef> {
        let fut = read_layout_future(layout, &self.file, &self.session).map_err(vx_err)?;
        self.runtime.block_on(fut).map_err(vx_err)
    }

    /// Fetch the segments of flat layouts and decode each into its encoded
    /// array tree. The data is not decompressed. Segments are fetched
    /// concurrently in groups of [`FLAT_FETCH_BATCH`] to bound memory use.
    /// `f` is called once per layout, in order, with the decoded array.
    pub fn for_each_flat(
        &self,
        layouts: &[&LayoutRef],
        mut f: impl FnMut(usize, ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let mut index = 0;
        for group in layouts.chunks(FLAT_FETCH_BATCH) {
            let mut flats = Vec::with_capacity(group.len());
            for layout in group {
                let Some(flat) = layout.as_opt::<Flat>() else {
                    return plan_err!("layout {} is not a flat layout", layout.encoding_id());
                };
                flats.push(flat);
            }
            let fetches = flats
                .iter()
                .map(|flat| self.file.segment_source().request(flat.segment_id()));
            let segments = self
                .runtime
                .block_on(futures::future::try_join_all(fetches))
                .map_err(vx_err)?;
            for (flat, segment) in flats.into_iter().zip(segments) {
                let row_count = usize::try_from(flat.row_count())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let parts = match flat.array_tree() {
                    Some(tree) => {
                        SerializedArray::from_flatbuffer_and_segment(tree.clone(), segment)
                    }
                    None => SerializedArray::try_from(segment),
                }
                .map_err(vx_err)?;
                let array = parts
                    .decode(flat.dtype(), row_count, flat.array_ctx(), &self.session)
                    .map_err(vx_err)?;
                f(index, array)?;
                index += 1;
            }
        }
        Ok(())
    }

    /// Scan the rows in `row_range` (all rows when `None`) and call `f` with
    /// each chunk converted to Arrow. Chunks are produced one at a time, so
    /// memory use is bounded by the chunk size.
    pub fn scan_arrow(
        &self,
        row_range: Option<std::ops::Range<u64>>,
        mut f: impl FnMut(arrow::array::ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let mut scan = self.file.scan().map_err(vx_err)?;
        if let Some(range) = row_range {
            scan = scan.with_row_range(range);
        }
        for chunk in scan.into_array_iter(&self.runtime).map_err(vx_err)? {
            let chunk = chunk.map_err(vx_err)?;
            f(array_to_arrow(&self.session, chunk)?)?;
        }
        Ok(())
    }

    /// Format each value of a Vortex array as a display string. Nulls become `None`.
    pub fn array_to_strings(&self, array: ArrayRef) -> Result<Vec<Option<String>>> {
        array_to_strings(&self.session, array)
    }

    /// Format a Vortex scalar as a display string. Null becomes `None`.
    pub fn scalar_to_string(&self, scalar: Scalar) -> Result<Option<String>> {
        if scalar.is_null() {
            return Ok(None);
        }
        let array = ConstantArray::new(scalar, 1).into_array();
        Ok(self.array_to_strings(array)?.into_iter().next().flatten())
    }
}

fn read_layout_future(
    layout: &LayoutRef,
    file: &VortexFile,
    session: &VortexSession,
) -> VortexResult<futures::future::BoxFuture<'static, VortexResult<ArrayRef>>> {
    let reader = layout.new_reader(
        "datafusion-functions-vortex".into(),
        file.segment_source(),
        session,
        &LayoutReaderContext::new(),
    )?;
    let row_count = layout.row_count();
    let len = usize::try_from(row_count)?;
    let expr = root().bind(reader.dtype())?;
    reader.projection_evaluation(&(0..row_count), &expr, MaskFuture::new_true(len))
}

fn array_to_strings(session: &VortexSession, array: ArrayRef) -> Result<Vec<Option<String>>> {
    let arrow_array = array_to_arrow(session, array)?;
    arrow_to_strings(arrow_array.as_ref())
}

/// Convert a Vortex array to its preferred Arrow representation
pub(crate) fn array_to_arrow(
    session: &VortexSession,
    array: ArrayRef,
) -> Result<arrow::array::ArrayRef> {
    let mut ctx = session.create_execution_ctx();
    session
        .arrow()
        .execute_arrow(array, None, &mut ctx)
        .map_err(vx_err)
}

/// Format each value of an Arrow array as a display string. Nulls become `None`.
pub(crate) fn arrow_to_strings(arrow_array: &dyn Array) -> Result<Vec<Option<String>>> {
    let options = FormatOptions::default();
    let formatter = ArrayFormatter::try_new(arrow_array, &options)?;
    Ok((0..arrow_array.len())
        .map(|i| {
            if arrow_array.is_null(i) {
                None
            } else {
                Some(formatter.value(i).to_string())
            }
        })
        .collect())
}

/// Number of flat segments fetched concurrently by [`VortexInput::for_each_flat`]
pub(crate) const FLAT_FETCH_BATCH: usize = 64;

/// The purpose of a layout node, inherited from the nearest auxiliary ancestor
pub(crate) const ROLE_DATA: &str = "data";
pub(crate) const ROLE_ZONE_MAP: &str = "zone_map";
pub(crate) const ROLE_DICT_VALUES: &str = "dict_values";
pub(crate) const ROLE_VALIDITY: &str = "validity";

/// One node of the layout tree with its position in the file
pub(crate) struct LayoutNode {
    /// Depth-first pre-order index. Stable for a given file.
    pub node_id: usize,
    pub parent_id: Option<usize>,
    pub depth: usize,
    /// Struct field path from the root to this node
    pub field_path: Vec<String>,
    /// Name of this child in its parent (`""` for the root)
    pub name: String,
    /// How this node relates to its parent
    pub child_type: &'static str,
    /// Purpose of the node (`data`, `zone_map`, `dict_values`, `validity`, or
    /// the name of another auxiliary child)
    pub role: String,
    /// Absolute first row in the file. `None` inside auxiliary children,
    /// whose rows are not file rows.
    pub row_offset: Option<u64>,
    pub layout: LayoutRef,
}

impl LayoutNode {
    /// The field path joined with `.` (`""` for the root)
    pub fn path(&self) -> String {
        self.field_path.join(".")
    }
}

/// Walk the layout tree depth first in pre-order
pub(crate) fn walk_layouts(root: &LayoutRef) -> Result<Vec<LayoutNode>> {
    let mut nodes: Vec<LayoutNode> = Vec::new();
    let mut stack = vec![LayoutNode {
        node_id: 0,
        parent_id: None,
        depth: 0,
        field_path: vec![],
        name: String::new(),
        child_type: "root",
        role: ROLE_DATA.to_string(),
        row_offset: Some(0),
        layout: LayoutRef::clone(root),
    }];
    while let Some(mut node) = stack.pop() {
        node.node_id = nodes.len();
        let children = node.layout.children().map_err(vx_err)?;
        let child_types: Vec<LayoutChildType> = node.layout.child_types().collect();
        let mut pending = Vec::with_capacity(children.len());
        for (child, child_type) in children.into_iter().zip(child_types) {
            let mut field_path = node.field_path.clone();
            let mut role = node.role.clone();
            let (kind, row_offset) = match &child_type {
                LayoutChildType::Field(name) => {
                    field_path.push(name.to_string());
                    ("field", node.row_offset)
                }
                LayoutChildType::Chunk((_, offset)) => {
                    ("chunk", node.row_offset.map(|parent| parent + offset))
                }
                LayoutChildType::Transparent(_) => ("transparent", node.row_offset),
                LayoutChildType::Auxiliary(name) => {
                    role = match name.as_ref() {
                        "zones" => ROLE_ZONE_MAP.to_string(),
                        "values" => ROLE_DICT_VALUES.to_string(),
                        "validity" => ROLE_VALIDITY.to_string(),
                        other => other.to_string(),
                    };
                    ("auxiliary", None)
                }
            };
            pending.push(LayoutNode {
                node_id: 0,
                parent_id: Some(node.node_id),
                depth: node.depth + 1,
                field_path,
                name: child_type.name().to_string(),
                child_type: kind,
                role,
                row_offset,
                layout: child,
            });
        }
        // Push in reverse so that the first child is visited first
        stack.extend(pending.into_iter().rev());
        nodes.push(node);
    }
    Ok(nodes)
}

/// True when `path` is `column` or a nested field of `column`
pub(crate) fn path_matches(path: &str, column: &str) -> bool {
    path == column
        || (path.len() > column.len()
            && path.starts_with(column)
            && path.as_bytes()[column.len()] == b'.')
}

/// Check that `column` names a field path in the layout tree
pub(crate) fn check_column_exists(nodes: &[LayoutNode], column: &str, func: &str) -> Result<()> {
    if nodes.iter().any(|n| n.path() == column) {
        Ok(())
    } else {
        plan_err!("{func}: column '{column}' does not exist in the file")
    }
}

/// Map each segment id to the index (in `nodes`) of the first layout node
/// that references it
pub(crate) fn segment_owners(nodes: &[LayoutNode]) -> std::collections::HashMap<u32, usize> {
    let mut owners = std::collections::HashMap::new();
    for (idx, node) in nodes.iter().enumerate() {
        for segment_id in node.layout.segment_ids() {
            owners.entry(*segment_id).or_insert(idx);
        }
    }
    owners
}

/// Names of the top-level fields. A file whose root DType is not a struct
/// has one unnamed field.
pub(crate) fn top_level_fields(
    dtype: &vortex::dtype::DType,
) -> Vec<(String, vortex::dtype::DType)> {
    match dtype.as_struct_fields_opt() {
        Some(fields) => fields
            .names()
            .iter()
            .map(|n| n.to_string())
            .zip(fields.fields())
            .collect(),
        None => vec![(String::new(), dtype.clone())],
    }
}

/// Zone map settings of a zoned layout
pub(crate) struct ZoneInfo {
    /// Rows per zone. The last zone can be shorter. 0 means no zone map.
    pub zone_len: u64,
    pub num_zones: usize,
    /// Display names of the aggregates stored per zone
    pub aggregates: Vec<String>,
}

/// Zone map settings when `layout` is a `vortex.zoned` or legacy
/// `vortex.stats` layout
pub(crate) fn zone_info(layout: &LayoutRef) -> Option<ZoneInfo> {
    use vortex::layout::layouts::zoned::{LegacyStats, Zoned};
    if let Some(zoned) = layout.as_opt::<Zoned>() {
        return Some(ZoneInfo {
            zone_len: zoned.zone_len() as u64,
            num_zones: zoned.nzones(),
            aggregates: zoned
                .present_aggregates()
                .iter()
                .map(|a| short_aggregate_name(a))
                .collect(),
        });
    }
    if let Some(legacy) = layout.as_opt::<LegacyStats>() {
        // Legacy metadata starts with the zone length as a little-endian u32
        let metadata = layout.metadata();
        let zone_len = metadata
            .get(0..4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            .unwrap_or(0);
        return Some(ZoneInfo {
            zone_len: u64::from(zone_len),
            num_zones: legacy.nzones(),
            aggregates: legacy
                .present_aggregates()
                .iter()
                .map(|a| short_aggregate_name(a))
                .collect(),
        });
    }
    None
}

/// Short display name of a zone map aggregate: `vortex.max()` becomes `max`
/// and `vortex.bounded_max(64)` becomes `bounded_max(64)`
pub(crate) fn short_aggregate_name(name: &str) -> String {
    let name = name.strip_prefix("vortex.").unwrap_or(name);
    name.strip_suffix("()").unwrap_or(name).to_string()
}

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

use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            ArrayBuilder, ArrayRef, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
            LargeStringBuilder, RecordBatch, StringBuilder, UInt16Builder, UInt32Builder,
            UInt64Builder, UInt8Builder,
        },
        datatypes::{DataType, Schema, SchemaRef},
    },
    catalog::{Session, TableProvider},
    common::{internal_err, project_schema, Constraints, DataFusionError, Result},
    datasource::TableType,
    execution::SendableRecordBatchStream,
    logical_expr::dml::InsertOp,
    physical_expr::{EquivalenceProperties, LexOrdering},
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        memory::MemoryStream,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
    prelude::Expr,
    scalar::ScalarValue,
};
use indexmap::IndexMap;
use parking_lot::RwLock;

type ArrayBuilderRef = Box<dyn ArrayBuilder>;

// The first String key is meant to hold primary key and provide O(1) lookup.  The inner HashMap is
// for holding arbitrary column and value pairs - the key is the column name and we use DataFusions
// scalar value to provide dynamic typing for the column values.
type MapData = Arc<RwLock<IndexMap<String, HashMap<String, ScalarValue>>>>;

#[derive(Debug)]
pub struct MapTableConfig {
    table_name: String,
    primary_key: String,
}

/// Table for tracking observability information. Data is held in a IndexMap, which maintains
/// insertion order, while the app is running and is serialized on app shutdown.
///
/// TODO: Add filter pushdown on the primary key and use `get` on that for O(1)
/// TODO: Add filter pushdown on non primary key and use `binary_search_by` / `range` (whatever
/// method the underlying map provides) to search values
#[derive(Debug)]
pub struct MapTable {
    schema: Arc<Schema>,
    constraints: Option<Constraints>,
    config: MapTableConfig,
    // TODO: This will be based on a Trait so you can use IndexMap, DashMap, BTreeMap, etc...
    inner: MapData,
}

impl MapTable {
    pub fn try_new(
        schema: Arc<Schema>,
        constraints: Option<Constraints>,
        config: MapTableConfig,
    ) -> Result<Self> {
        let inner = Arc::new(RwLock::new(IndexMap::new()));
        Ok(Self {
            schema,
            constraints,
            config,
            inner,
        })
    }

    fn try_hashmap_to_row(&self, values: &HashMap<String, ScalarValue>) -> Result<()> {
        for (col, val) in values {
            // Check that the column is in the tables schema
            if let Some(_) = self.schema.fields.find(col) {
            } else {
                return Err(datafusion::error::DataFusionError::External(
                    format!(
                        "Column {} for table {} is not in the provided schema",
                        col, self.config.table_name
                    )
                    .into(),
                ));
            }
        }
        Ok(())
    }

    fn partitions(&self) -> Result<Vec<Vec<RecordBatch>>> {
        let guard = self.inner.read();
        let values = guard.values();
        // For now we just create a single partition
        let mut builders: IndexMap<String, (ArrayBuilderRef, DataType)> = IndexMap::new();
        for f in &self.schema.fields {
            let builder = datatype_to_array_builder(f.data_type())?;
            builders.insert(f.name().clone(), (builder, f.data_type().clone()));
        }

        for value in values {
            for (col, val) in value {
                // Check that the column is in the tables schema
                if let Some(_) = &self.schema.fields.find(col) {
                    if let Some((builder, builder_datatype)) = builders.get_mut(col) {
                        append_scalar_to_builder(builder, builder_datatype, val)
                    }
                } else {
                    return Err(datafusion::error::DataFusionError::External(
                        format!(
                            "Column {} for table {} is not in the provided schema",
                            col, self.config.table_name
                        )
                        .into(),
                    ));
                }
            }
        }

        let arrays: Vec<ArrayRef> = builders.values_mut().map(|(b, _)| b.finish()).collect();

        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        Ok(vec![vec![batch]])
    }
}

#[async_trait]
impl TableProvider for MapTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.constraints.as_ref()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let partitions = self.partitions();
        let exec = MapExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }

    // async fn insert_into(
    //     &self,
    //     _state: &dyn Session,
    //     input: Arc<dyn ExecutionPlan>,
    //     insert_op: InsertOp,
    // ) -> Result<Arc<dyn ExecutionPlan>> {
    // }
}

/// Execution plan for converting Map data into in-memory record batches and then reading from
/// them
#[derive(Debug)]
struct MapExec {
    /// The partitions to query
    partitions: Vec<Vec<RecordBatch>>,
    /// Optional projection
    projection: Option<Vec<usize>>,
    /// Schema representing the data before projection
    schema: SchemaRef,
    /// Schema representing the data after the optional projection is applied
    projected_schema: SchemaRef,
    // Sort information: one or more equivalent orderings
    sort_information: Vec<LexOrdering>,
    cache: PlanProperties,
}

impl MapExec {
    fn try_new(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        let constraints = Constraints::empty();
        let cache =
            Self::compute_properties(Arc::clone(&projected_schema), &[], constraints, partitions);

        Ok(Self {
            partitions: partitions.to_vec(),
            schema,
            projected_schema,
            projection,
            sort_information: vec![],
            cache,
        })
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        constraints: Constraints,
        partitions: &[Vec<RecordBatch>],
    ) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new_with_orderings(schema, orderings)
                .with_constraints(constraints),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for MapExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MapExec")
            }
        }
    }
}

impl ExecutionPlan for MapExec {
    fn name(&self) -> &str {
        "MapExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // This is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // MapExec has no children
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in {self:?}")
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            self.partitions[partition],
            Arc::clone(&self.projected_schema),
            self.projection(),
        )?))
    }
}

fn datatype_to_array_builder(datatype: &DataType) -> Result<Box<dyn ArrayBuilder>> {
    match datatype {
        DataType::Int8 => Ok(Box::new(Int8Builder::new())),
        DataType::Int16 => Ok(Box::new(Int16Builder::new())),
        DataType::Int32 => Ok(Box::new(Int32Builder::new())),
        DataType::Int64 => Ok(Box::new(Int64Builder::new())),
        DataType::UInt8 => Ok(Box::new(UInt8Builder::new())),
        DataType::UInt16 => Ok(Box::new(UInt16Builder::new())),
        DataType::UInt32 => Ok(Box::new(UInt32Builder::new())),
        DataType::UInt64 => Ok(Box::new(UInt64Builder::new())),
        DataType::Utf8 => Ok(Box::new(StringBuilder::new())),
        DataType::LargeUtf8 => Ok(Box::new(LargeStringBuilder::new())),

        _ => {
            return Err(DataFusionError::External(
                "Unsupported column type when constructing batch from Map".into(),
            ))
        }
    }
}

fn try_append_scalar_to_builder(
    builder: &mut Box<dyn ArrayBuilder>,
    builder_datatype: &DataType,
    scalar: &ScalarValue,
) -> Result<()> {
    if builder_datatype == &scalar.data_type() {
        match scalar {
            ScalarValue::Int8(i) => builder,
        };
    } else {
        return Err(DataFusionError::External(
            "Array builder and ScalarValue data types dont match".into(),
        ));
    };
    Ok(())
}

#[cfg(test)]
mod test {}

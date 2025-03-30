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
        array::RecordBatch,
        datatypes::{Schema, SchemaRef},
    },
    catalog::{Session, TableProvider},
    common::{internal_err, project_schema, Constraints, Result},
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

type ObservabilityData = Arc<RwLock<IndexMap<String, HashMap<String, ScalarValue>>>>;

#[derive(Debug)]
pub struct IndexMapTableConfig {
    primary_key: String,
}

/// Table for tracking observability information. Data is held in a IndexMap, which maintains
/// insertion order, while the app is running and is serialized on app shutdown.
#[derive(Debug)]
pub struct IndexMapTable {
    schema: Arc<Schema>,
    constraints: Option<Constraints>,
    config: IndexMapTableConfig,
    inner: ObservabilityData,
}

impl IndexMapTable {
    pub fn try_new(
        schema: Arc<Schema>,
        constraints: Option<Constraints>,
        config: IndexMapTableConfig,
    ) -> Result<Self> {
        let inner = Arc::new(RwLock::new(IndexMap::new()));
        Ok(Self {
            schema,
            constraints,
            config,
            inner,
        })
    }

    fn partitions(&self) -> &[Vec<RecordBatch>] {}
}

#[async_trait]
impl TableProvider for IndexMapTable {
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
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let partitions = self.partitions();
        let exec =
            IndexMapExec::try_new(partitions, Arc::clone(&self.schema), projection.cloned())?;
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

/// Execution plan for converting IndexMap data into in-memory record batches and then reading from
/// them
#[derive(Debug)]
struct IndexMapExec {
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

impl IndexMapExec {
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

impl DisplayAs for IndexMapExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IndexMapExec")
            }
        }
    }
}

impl ExecutionPlan for IndexMapExec {
    fn name(&self) -> &str {
        "IndexMapExec"
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
        // IndexMapExec has no children
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

#[cfg(test)]
mod test {}

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
//
// Todo: Maybe the inner HashMap should be a Vec to make projecting easier
type MapData = Arc<RwLock<IndexMap<ScalarValue, HashMap<String, ScalarValue>>>>;

#[derive(Debug)]
pub struct MapTableConfig {
    table_name: String,
    /// Column name of the primary key
    _primary_key: String,
}

impl MapTableConfig {
    pub fn new(table_name: String, primary_key: String) -> Self {
        Self {
            table_name,
            _primary_key: primary_key,
        }
    }
}

/// Table for tracking observability information. Data is held in a IndexMap, which maintains
/// insertion order, while the app is running and is serialized on app shutdown.
///
/// TODO: Add filter pushdown on the primary key and use `get` on that for O(1)
/// TODO: Add filter pushdown on non primary key and use `binary_search_by` / `range` (whatever
/// TODO: Add projection pushdown to only read keys from HashMap that are projected
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
        data: Option<MapData>,
    ) -> Result<Self> {
        let inner = data.unwrap_or(Arc::new(RwLock::new(IndexMap::new())));
        Ok(Self {
            schema,
            constraints,
            config,
            inner,
        })
    }

    fn try_create_partitions(&self) -> Result<Vec<Vec<RecordBatch>>> {
        let guard = self.inner.read();
        let values = guard.values();
        // We use IndexMap, which has order defined on insertion order to have our builders align
        // with the order of the fields in the Schema.
        let mut builders: IndexMap<String, (ArrayBuilderRef, DataType)> = IndexMap::new();
        for f in &self.schema.fields {
            let builder = datatype_to_array_builder(f.data_type())?;
            builders.insert(f.name().clone(), (builder, f.data_type().clone()));
        }

        for value in values {
            for (col, val) in value {
                // Check that the column is in the tables schema
                if self.schema.fields.find(col).is_some() {
                    if let Some((builder, builder_datatype)) = builders.get_mut(col) {
                        try_append_scalar_to_builder(builder, builder_datatype, val)?;
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

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays)?;
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let partitions = self.try_create_partitions()?;
        let exec = MapExec::try_new(&partitions, Arc::clone(&self.schema), projection.cloned())?;
        Ok(Arc::new(exec))
    }
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
    _schema: SchemaRef,
    /// Schema representing the data after the optional projection is applied
    projected_schema: SchemaRef,
    // Sort information: one or more equivalent orderings
    _sort_information: Vec<LexOrdering>,
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
            _schema: schema,
            projected_schema,
            projection,
            _sort_information: vec![],
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
                write!(
                    f,
                    "MapExec: partitions={}, projection={:?}",
                    self.partitions.len(),
                    self.projection
                )
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
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            self.partitions[partition].clone(),
            Arc::clone(&self.projected_schema),
            self.projection.clone(),
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

        _ => Err(DataFusionError::External(
            "Unsupported column type when constructing batch from Map".into(),
        )),
    }
}

macro_rules! append_primitive_scalar {
    ($scalar:expr, $builder:expr, $variant:ident, $builder_type:ty) => {{
        if let ScalarValue::$variant(val) = $scalar {
            if let Some(b) = $builder.as_any_mut().downcast_mut::<$builder_type>() {
                if let Some(x) = val {
                    b.append_value(*x);
                } else {
                    b.append_null();
                }
                Ok(())
            } else {
                Err(DataFusionError::External(
                    format!("Failed to downcast builder for {}", stringify!($variant)).into(),
                ))
            }
        } else {
            // If the scalar is not of the expected variant, do nothing.
            Ok(())
        }
    }};
}

fn try_append_scalar_to_builder(
    builder: &mut Box<dyn ArrayBuilder>,
    builder_datatype: &DataType,
    scalar: &ScalarValue,
) -> Result<()> {
    if builder_datatype == &scalar.data_type() {
        match scalar {
            ScalarValue::Int8(_) => append_primitive_scalar!(scalar, builder, Int8, Int8Builder)?,
            ScalarValue::Int16(_) => {
                append_primitive_scalar!(scalar, builder, Int16, Int16Builder)?
            }
            ScalarValue::Int32(_) => {
                append_primitive_scalar!(scalar, builder, Int32, Int32Builder)?
            }
            ScalarValue::Int64(_) => {
                append_primitive_scalar!(scalar, builder, Int64, Int64Builder)?
            }
            ScalarValue::UInt8(_) => {
                append_primitive_scalar!(scalar, builder, UInt8, UInt8Builder)?
            }
            ScalarValue::UInt16(_) => {
                append_primitive_scalar!(scalar, builder, UInt16, UInt16Builder)?
            }
            ScalarValue::UInt32(_) => {
                append_primitive_scalar!(scalar, builder, UInt32, UInt32Builder)?
            }
            ScalarValue::UInt64(_) => {
                append_primitive_scalar!(scalar, builder, UInt64, UInt64Builder)?
            }
            ScalarValue::Utf8(s) => {
                if let Some(builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                    if let Some(s) = s {
                        builder.append_value(s.clone())
                    } else {
                        builder.append_null()
                    }
                }
            }
            ScalarValue::LargeUtf8(s) => {
                if let Some(builder) = builder.as_any_mut().downcast_mut::<LargeStringBuilder>() {
                    if let Some(s) = s {
                        builder.append_value(s.clone())
                    } else {
                        builder.append_null()
                    }
                }
            }

            _ => {
                return Err(DataFusionError::External(
                    format!("Unsupported DataType ({}) for conversion", builder_datatype).into(),
                ))
            }
        };
    } else {
        return Err(DataFusionError::External(
            "Array builder and ScalarValue data types dont match".into(),
        ));
    };
    Ok(())
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        assert_batches_eq,
        prelude::{SessionConfig, SessionContext},
        scalar::ScalarValue,
    };
    use indexmap::IndexMap;
    use parking_lot::RwLock;

    use crate::tables::map_table::{MapTable, MapTableConfig};

    fn setup() -> SessionContext {
        let mut data: IndexMap<ScalarValue, HashMap<String, ScalarValue>> = IndexMap::new();
        let ids = vec![1, 2, 3, 4, 5];
        let vals = vec!["val1", "val2", "val3", "val4", "val5"];
        for (id, val) in ids.into_iter().zip(vals) {
            let mut row: HashMap<String, ScalarValue> = HashMap::new();
            row.insert("id".to_string(), ScalarValue::Int32(Some(id)));
            row.insert("val".to_string(), ScalarValue::Utf8(Some(val.to_string())));
            data.insert(ScalarValue::Int32(Some(id)), row);
        }

        let fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ];
        let schema = Schema::new(fields);
        let config = MapTableConfig::new("test".to_string(), "id".to_string());
        let table = MapTable::try_new(
            Arc::new(schema),
            None,
            config,
            Some(Arc::new(RwLock::new(data))),
        )
        .unwrap();
        let config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);
        ctx.register_table("test", Arc::new(table)).unwrap();
        ctx
    }

    #[tokio::test]
    async fn test_map_table_plans_correctly() {
        // TODO UPDATE ROOT KEY, WHICH IS THE PRIMARY KEY, TO BE OF TYPE SCALARVALUE
        let ctx = setup();
        let batches = ctx
            .sql("EXPLAIN SELECT * FROM test")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+---------------+------------------------------------------------+",
            "| plan_type     | plan                                           |",
            "+---------------+------------------------------------------------+",
            "| logical_plan  | TableScan: test projection=[id, val]           |",
            "| physical_plan | MapExec: partitions=1, projection=Some([0, 1]) |",
            "|               |                                                |",
            "+---------------+------------------------------------------------+",
        ];

        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_select_star_from_map_table() {
        let ctx = setup();
        let batches = ctx
            .sql("SELECT * FROM test")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+----+------+",
            "| id | val  |",
            "+----+------+",
            "| 1  | val1 |",
            "| 2  | val2 |",
            "| 3  | val3 |",
            "| 4  | val4 |",
            "| 5  | val5 |",
            "+----+------+",
        ];

        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_select_star_with_filter_from_map_table() {
        let ctx = setup();
        // Check it includes the expected result
        let batches = ctx
            .sql("SELECT * FROM test WHERE id = 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+----+------+",
            "| id | val  |",
            "+----+------+",
            "| 1  | val1 |",
            "+----+------+",
        ];

        assert_batches_eq!(expected, &batches);

        // Check it excludes the expected result
        let batches = ctx
            .sql("SELECT * FROM test WHERE id = 6")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = ["++", "++"];

        assert_batches_eq!(expected, &batches);

        let batches = ctx
            .sql("EXPLAIN SELECT * FROM test WHERE id = 2")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+---------------+--------------------------------------------------------------------------+",
            "| plan_type     | plan                                                                     |",
            "+---------------+--------------------------------------------------------------------------+",
            "| logical_plan  | Filter: test.id = Int32(2)                                               |",
            "|               |   TableScan: test projection=[id, val]                                   |",
            "| physical_plan | CoalesceBatchesExec: target_batch_size=8192                              |",
            "|               |   FilterExec: id@0 = 2                                                   |",
            "|               |     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1 |",
            "|               |       MapExec: partitions=1, projection=Some([0, 1])                     |",
            "|               |                                                                          |",
            "+---------------+--------------------------------------------------------------------------+",
        ];

        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_select_star_with_projection_from_map_table() {
        let ctx = setup();
        // Check it includes the expected result
        let batches = ctx
            .sql("SELECT val FROM test WHERE id = 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = ["+------+", "| val  |", "+------+", "| val1 |", "+------+"];

        assert_batches_eq!(expected, &batches);

        let batches = ctx
            .sql("SELECT id * 2 FROM test")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+--------------------+",
            "| test.id * Int64(2) |",
            "+--------------------+",
            "| 2                  |",
            "| 4                  |",
            "| 6                  |",
            "| 8                  |",
            "| 10                 |",
            "+--------------------+",
        ];

        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_select_star_with_sort_from_map_table() {
        let ctx = setup();
        // Check it includes the expected result
        let batches = ctx
            .sql("SELECT * FROM test ORDER BY id DESC")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+----+------+",
            "| id | val  |",
            "+----+------+",
            "| 5  | val5 |",
            "| 4  | val4 |",
            "| 3  | val3 |",
            "| 2  | val2 |",
            "| 1  | val1 |",
            "+----+------+",
        ];

        assert_batches_eq!(expected, &batches);

        let batches = ctx
            .sql("EXPLAIN SELECT * FROM test ORDER BY id DESC")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+---------------+-----------------------------------------------------------+",
            "| plan_type     | plan                                                      |",
            "+---------------+-----------------------------------------------------------+",
            "| logical_plan  | Sort: test.id DESC NULLS FIRST                            |",
            "|               |   TableScan: test projection=[id, val]                    |",
            "| physical_plan | SortExec: expr=[id@0 DESC], preserve_partitioning=[false] |",
            "|               |   MapExec: partitions=1, projection=Some([0, 1])          |",
            "|               |                                                           |",
            "+---------------+-----------------------------------------------------------+",
        ];

        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_select_star_with_limit_from_map_table() {
        let ctx = setup();
        // Check it includes the expected result
        let batches = ctx
            .sql("SELECT * FROM test LIMIT 2")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+----+------+",
            "| id | val  |",
            "+----+------+",
            "| 1  | val1 |",
            "| 2  | val2 |",
            "+----+------+",
        ];

        assert_batches_eq!(expected, &batches);

        let batches = ctx
            .sql("EXPLAIN SELECT * FROM test LIMIT 2")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = [
            "+---------------+--------------------------------------------------+",
            "| plan_type     | plan                                             |",
            "+---------------+--------------------------------------------------+",
            "| logical_plan  | Limit: skip=0, fetch=2                           |",
            "|               |   TableScan: test projection=[id, val], fetch=2  |",
            "| physical_plan | GlobalLimitExec: skip=0, fetch=2                 |",
            "|               |   MapExec: partitions=1, projection=Some([0, 1]) |",
            "|               |                                                  |",
            "+---------------+--------------------------------------------------+",
        ];

        assert_batches_eq!(expected, &batches);
    }
}

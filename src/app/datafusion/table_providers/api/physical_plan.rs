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

//! Execution plans that read file formats

mod file_stream;
mod json;

pub use self::parquet::ParquetExec;
use arrow::{
    array::{ArrayData, ArrayRef, DictionaryArray},
    buffer::Buffer,
    datatypes::{DataType, Field, Schema, SchemaRef, UInt16Type},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};

pub(crate) use csv::plan_to_csv;
pub use csv::CsvExec;
pub(crate) use json::plan_to_json;
pub use json::NdJsonExec;

use arrow::array::{new_null_array, UInt16BufferBuilder};
use arrow::record_batch::RecordBatchOptions;
use lazy_static::lazy_static;
use log::info;
use std::{
    collections::HashMap,
    fmt::{Display, Formatter, Result as FmtResult},
    sync::Arc,
    vec,
};

use super::{ColumnStatistics, Statistics};

lazy_static! {
    /// The datatype used for all partitioning columns for now
    pub static ref DEFAULT_PARTITION_COLUMN_DATATYPE: DataType = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8));
}

/// The base configurations to provide when creating a physical plan for
/// any given file format.
#[derive(Debug, Clone)]
pub struct ApiScanConfig {
    /// Object store URL
    pub object_store_url: ObjectStoreUrl,
    /// Schema before projection. It contains the columns that are expected
    /// to be in the files without the table partition columns.
    pub file_schema: SchemaRef,
    /// List of files to be processed, grouped into partitions
    pub file_groups: Vec<Vec<PartitionedFile>>,
    /// Estimated overall statistics of the files, taking `filters` into account.
    pub statistics: Statistics,
    /// Columns on which to project the data. Indexes that are higher than the
    /// number of columns of `file_schema` refer to `table_partition_cols`.
    pub projection: Option<Vec<usize>>,
    /// The minimum number of records required from this source plan
    pub limit: Option<usize>,
}

impl FileScanConfig {
    /// Project the schema and the statistics on the given column indices
    fn project(&self) -> (SchemaRef, Statistics) {
        if self.projection.is_none() && self.table_partition_cols.is_empty() {
            return (Arc::clone(&self.file_schema), self.statistics.clone());
        }

        let proj_iter: Box<dyn Iterator<Item = usize>> = match &self.projection {
            Some(proj) => Box::new(proj.iter().copied()),
            None => {
                Box::new(0..(self.file_schema.fields().len() + self.table_partition_cols.len()))
            }
        };

        let mut table_fields = vec![];
        let mut table_cols_stats = vec![];
        for idx in proj_iter {
            if idx < self.file_schema.fields().len() {
                table_fields.push(self.file_schema.field(idx).clone());
                if let Some(file_cols_stats) = &self.statistics.column_statistics {
                    table_cols_stats.push(file_cols_stats[idx].clone())
                } else {
                    table_cols_stats.push(ColumnStatistics::default())
                }
            } else {
                let partition_idx = idx - self.file_schema.fields().len();
                table_fields.push(Field::new(
                    &self.table_partition_cols[partition_idx],
                    DEFAULT_PARTITION_COLUMN_DATATYPE.clone(),
                    false,
                ));
                // TODO provide accurate stat for partition column (#1186)
                table_cols_stats.push(ColumnStatistics::default())
            }
        }

        let table_stats = Statistics {
            num_rows: self.statistics.num_rows,
            is_exact: self.statistics.is_exact,
            // TODO correct byte size?
            total_byte_size: None,
            column_statistics: Some(table_cols_stats),
        };

        let table_schema = Arc::new(Schema::new(table_fields));

        (table_schema, table_stats)
    }

    fn projected_file_column_names(&self) -> Option<Vec<String>> {
        self.projection.as_ref().map(|p| {
            p.iter()
                .filter(|col_idx| **col_idx < self.file_schema.fields().len())
                .map(|col_idx| self.file_schema.field(*col_idx).name())
                .cloned()
                .collect()
        })
    }

    fn file_column_projection_indices(&self) -> Option<Vec<usize>> {
        self.projection.as_ref().map(|p| {
            p.iter()
                .filter(|col_idx| **col_idx < self.file_schema.fields().len())
                .copied()
                .collect()
        })
    }
}

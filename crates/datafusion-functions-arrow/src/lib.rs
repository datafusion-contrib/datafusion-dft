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

mod batch;
mod batches;
mod dictionaries;
mod file_metadata;
mod metadata;
mod schema;

pub use batch::ArrowBatchFunc;
pub use batches::ArrowBatchesFunc;
pub use dictionaries::ArrowDictionariesFunc;
pub use file_metadata::ArrowFileMetadataFunc;
pub use metadata::ArrowMetadataFunc;
pub use schema::ArrowSchemaFunc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{plan_err, Column};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

/// A single in-memory [`RecordBatch`] exposed as a [`TableProvider`].
#[derive(Debug)]
struct BatchTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

impl BatchTable {
    fn new(schema: SchemaRef, batch: RecordBatch) -> Arc<Self> {
        Arc::new(Self { schema, batch })
    }
}

#[async_trait]
impl TableProvider for BatchTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![self.batch.clone()]],
            TableProvider::schema(self),
            projection.cloned(),
        )?)
    }
}

/// Extract the filename argument shared by all functions in this crate.
fn filename_arg<'a>(func_name: &str, exprs: &'a [Expr]) -> Result<&'a str> {
    match exprs.first() {
        Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => Ok(s),
        Some(Expr::Column(Column { name, .. })) => Ok(name),
        _ => plan_err!("{func_name} requires a string filename as its argument"),
    }
}

/// Location and size of one block (record batch or dictionary) in the file.
#[derive(Debug)]
struct BlockInfo {
    offset: i64,
    metadata_length: i64,
    body_length: i64,
}

/// Owned copy of the metadata stored in an Arrow IPC file footer.
#[derive(Debug)]
struct FooterInfo {
    version: String,
    schema: SchemaRef,
    custom_metadata: Vec<(Option<String>, Option<String>)>,
    dictionary_blocks: Vec<BlockInfo>,
    record_batch_blocks: Vec<BlockInfo>,
}

/// Read and parse the footer of an Arrow IPC file.
///
/// Returns a plan error if the file is not in the Arrow IPC file format
/// (e.g. a stream format file, which has no footer).
fn read_footer(filename: &str) -> Result<FooterInfo> {
    let mut file = File::open(filename)?;
    let file_len = file.metadata()?.len();
    if file_len < 10 {
        return Err(DataFusionError::Execution(format!(
            "{filename} is too small to be an Arrow IPC file"
        )));
    }

    file.seek(SeekFrom::End(-10))?;
    let mut trailer = [0u8; 10];
    file.read_exact(&mut trailer)?;
    let footer_len = arrow::ipc::reader::read_footer_length(trailer).map_err(|e| {
        DataFusionError::Execution(format!("failed to read footer length of {filename}: {e}"))
    })?;

    file.seek(SeekFrom::End(-10 - footer_len as i64))?;
    let mut footer_data = vec![0u8; footer_len];
    file.read_exact(&mut footer_data)?;
    let footer = arrow::ipc::root_as_footer(&footer_data).map_err(|e| {
        DataFusionError::Execution(format!("failed to parse footer of {filename}: {e:?}"))
    })?;

    let fb_schema = footer.schema().ok_or_else(|| {
        DataFusionError::Execution(format!("no schema found in footer of {filename}"))
    })?;
    let schema = Arc::new(arrow::ipc::convert::fb_to_schema(fb_schema));

    let custom_metadata = footer
        .custom_metadata()
        .map(|kvs| {
            kvs.iter()
                .map(|kv| {
                    (
                        kv.key().map(|k| k.to_string()),
                        kv.value().map(|v| v.to_string()),
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    let to_block_infos = |blocks: Option<flatbuffers::Vector<'_, arrow::ipc::Block>>| {
        blocks
            .map(|bs| {
                bs.iter()
                    .map(|b| BlockInfo {
                        offset: b.offset(),
                        metadata_length: b.metaDataLength() as i64,
                        body_length: b.bodyLength(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    Ok(FooterInfo {
        version: footer
            .version()
            .variant_name()
            .unwrap_or("UNKNOWN")
            .to_string(),
        schema,
        custom_metadata,
        dictionary_blocks: to_block_infos(footer.dictionaries()),
        record_batch_blocks: to_block_infos(footer.recordBatches()),
    })
}

/// Owned copy of a parsed `RecordBatch` message header.
#[derive(Debug)]
struct RecordBatchHeader {
    num_rows: i64,
    compression_codec: Option<String>,
    compression_method: Option<String>,
}

/// Owned copy of a parsed `DictionaryBatch` message header.
#[derive(Debug)]
struct DictionaryBatchHeader {
    id: i64,
    is_delta: bool,
    num_rows: i64,
}

/// Parse the encapsulated IPC message at the start of `block`.
fn read_block_message<'a>(
    file: &mut File,
    block: &BlockInfo,
    buf: &'a mut Vec<u8>,
) -> Result<arrow::ipc::Message<'a>> {
    file.seek(SeekFrom::Start(block.offset as u64))?;
    buf.clear();
    buf.resize(block.metadata_length as usize, 0);
    file.read_exact(buf)?;
    // Encapsulated message format: optional 4-byte continuation marker
    // followed by a 4-byte message length, then the flatbuffer message.
    let fb = match buf[..4] == [0xFF, 0xFF, 0xFF, 0xFF] {
        true => &buf[8..],
        false => &buf[4..],
    };
    arrow::ipc::root_as_message(fb).map_err(|e| {
        DataFusionError::Execution(format!(
            "failed to parse message at offset {}: {e:?}",
            block.offset
        ))
    })
}

/// Read the `RecordBatch` message header for a record batch block.
fn read_record_batch_header(file: &mut File, block: &BlockInfo) -> Result<RecordBatchHeader> {
    let mut buf = Vec::new();
    let message = read_block_message(file, block, &mut buf)?;
    let rb = message.header_as_record_batch().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "message at offset {} is not a record batch",
            block.offset
        ))
    })?;
    let (codec, method) = match rb.compression() {
        Some(c) => (
            Some(format!("{:?}", c.codec())),
            Some(format!("{:?}", c.method())),
        ),
        None => (None, None),
    };
    Ok(RecordBatchHeader {
        num_rows: rb.length(),
        compression_codec: codec,
        compression_method: method,
    })
}

/// Read the `DictionaryBatch` message header for a dictionary block.
fn read_dictionary_header(file: &mut File, block: &BlockInfo) -> Result<DictionaryBatchHeader> {
    let mut buf = Vec::new();
    let message = read_block_message(file, block, &mut buf)?;
    let db = message.header_as_dictionary_batch().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "message at offset {} is not a dictionary batch",
            block.offset
        ))
    })?;
    Ok(DictionaryBatchHeader {
        id: db.id(),
        is_delta: db.isDelta(),
        num_rows: db.data().map(|d| d.length()).unwrap_or(0),
    })
}

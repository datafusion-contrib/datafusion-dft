use arrow::{
    array::{Array, ArrayRef, RecordBatch},
    datatypes::{Field, Schema, SchemaRef},
};
pub use paste;
use std::sync::Arc;
pub use wasmedge_bindgen;
pub use wasmedge_bindgen_macro;

/// packs slice of arrays to a batch
/// with schema generated from array types
pub fn pack_array(args: &[ArrayRef]) -> RecordBatch {
    let fields = args
        .iter()
        .enumerate()
        .map(|(i, f)| Field::new(format!("c{}", i), f.data_type().clone(), false))
        .collect::<Vec<_>>();

    let schema = Arc::new(Schema::new(fields));

    RecordBatch::try_new(schema, args.to_vec()).unwrap()
}

/// packs slice of arrays to a batch
/// with external schema
pub fn pack_array_with_schema(args: &[ArrayRef], schema: SchemaRef) -> RecordBatch {
    RecordBatch::try_new(schema, args.to_vec()).unwrap()
}

/// creates a arrow ipc blob
pub fn to_ipc(schema: &Schema, batch: RecordBatch) -> Vec<u8> {
    let blob = vec![];
    let mut stream_writer = arrow::ipc::writer::StreamWriter::try_new(blob, schema).unwrap();
    stream_writer.write(&batch).unwrap();

    stream_writer.into_inner().unwrap()
}

/// creates arrow arrays from arrow ipc blob
pub fn from_ipc(payload: &[u8]) -> RecordBatch {
    let mut batch = arrow::ipc::reader::StreamReader::try_new(payload, None).unwrap();
    batch.next().unwrap().unwrap()
}

/// exports wasm function and performs all required
/// arrow ipc serialization/deserialization
///
/// macro will create new function prefixed with `__wasm_udf_`
///
// TODO: make this a proc macro maybe ?
#[macro_export]
macro_rules! export_udf_function {
    ($name:ident) => {
        paste::item! {
            #[wasmedge_bindgen_macro::wasmedge_bindgen]
            pub fn [<__wasm_udf_$name>](payload: Vec<u8>) -> Result<Vec<u8>,String> {
                let args_batch = from_ipc(&payload);
                let result = $name(args_batch.columns());
                // let batch = pack_array(&vec![result]);
                // to_ipc(&batch.schema(), batch)
                result.map(|result| pack_array(&vec![result]))
                    .map(|batch| to_ipc(&batch.schema(), batch))
                    .map_err(|e| e.to_string())
            }
        }
    };
}

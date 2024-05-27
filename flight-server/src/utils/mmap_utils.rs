use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use arrow::array::{StringArray, ArrayRef, Int64Array, Float64Array};
use arrow::datatypes::{Field, DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::{Ticket, FlightDescriptor};
use serde_json::Value;
use tonic::Status;
use uuid::Uuid;

pub fn data_to_record_batch(data: Value) -> Result<RecordBatch, Status> {
    // Extract the inner "data" object directly
    let map = data.get("data").and_then(|v| v.as_object()).ok_or_else(|| Status::internal("Missing 'data' field or not an object"))?;

    let mut instrument_names: Vec<String> = Vec::new();
    let mut timestamps: Vec<Option<i64>> = Vec::new();
    let mut indicator_values: HashMap<String, Vec<Option<f64>>> = HashMap::new();

    // Process each instrument in the data
    for (instrument, indicators) in map {
        instrument_names.push(instrument.clone());
        let indicators_map = indicators.as_object().ok_or_else(|| Status::internal("Indicators should be an object"))?;

        // Process each indicator, ensuring 'timestamp' is also considered
        let timestamp = indicators_map.get("timestamp").and_then(|v| v.as_i64());
        timestamps.push(timestamp);

        for (indicator, value) in indicators_map {
            if indicator != "timestamp" {
                let val = value.as_f64();
                indicator_values.entry(indicator.clone()).or_insert_with(Vec::new).push(val);
            }
        }
    }

    // Ensure all vectors in indicator_values are of the same length
    for values in indicator_values.values_mut() {
        while values.len() < instrument_names.len() {
            values.push(None); // Pad with None if any indicators are missing
        }
    }

    // Create the schema for the RecordBatch
    let mut fields = vec![
        Field::new("instrument", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, true),
    ];
    for indicator in indicator_values.keys() {
        fields.push(Field::new(indicator, DataType::Float64, true));
    }
    let schema = Schema::new(fields);

    // Create arrays for each column
    let instrument_array = Arc::new(StringArray::from(instrument_names)) as ArrayRef;
    let timestamp_array = Arc::new(Int64Array::from(timestamps)) as ArrayRef;
    let mut arrays: Vec<ArrayRef> = vec![instrument_array, timestamp_array];
    for (_indicator, values) in indicator_values.iter() {
        let array = Arc::new(Float64Array::from(values.clone())) as ArrayRef;
        arrays.push(array);
    }

    // Create the record batch
    let batch = RecordBatch::try_new(Arc::new(schema), arrays)
        .map_err(|e| Status::internal(format!("Failed to create RecordBatch: {}", e)));

    batch
}

pub fn descriptor_to_key(descriptor: &FlightDescriptor) -> (i32, Option<String>, Vec<String>) {

    let descriptor_type = descriptor.r#type() as i32;
    let command = match descriptor.r#type() {
        DescriptorType::Cmd => Some(String::from_utf8(descriptor.cmd.to_vec()).unwrap_or_default()),
        _ => None,
    };
    let path = descriptor.path.clone();

    (descriptor_type, command, path)
}

//TODO: IMPLEMENT AND TEST
/// Serialize a RecordBatch to a memory-mapped file and return a Ticket with the file path.
pub fn batch_to_mmap(batch: &RecordBatch) -> Result<Ticket, Status> {
    let unique_id = Uuid::new_v4().to_string();
    let path = env::var("DATA_PATH").unwrap_or_else(|_| "./data".to_string());
    let file_path = format!("{}/batch_{}.mmap",path,unique_id);

    match write_record_batch_to_file(batch, &file_path) {
        Ok(_) => Ok(Ticket::new(file_path)),
        Err(e) => Err(Status::internal("Failed to write RecordBatch to memory-mapped file"))
    }
}

//TODO: IMPLEMENT AND TEST
/// Helper function to write a RecordBatch to a file using memory mapping.
fn write_record_batch_to_file(batch: &RecordBatch, file_path: &str) -> Result<(),Status> {
    unimplemented!()
}

//TODO: IMPLEMENT AND TEST
// Load a RecordBatch from a memory-mapped file.
pub fn mmap_to_batch(ticket: &Ticket) -> Result<RecordBatch,Status> {
   unimplemented!()
}

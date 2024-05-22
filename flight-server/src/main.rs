// Standard library imports
use std::collections::HashMap;
use std::sync::Arc;
// Serialization
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
// Async and futures handling
use futures::{TryStreamExt, stream::BoxStream};
use tokio::sync::Mutex;
// Arrow and Arrow Flight related imports
use arrow::{
    array::{ArrayRef, StringArray, Int32Array, BooleanArray, Float64Array, Int64Array},
    datatypes::{Schema, Field, DataType},
    record_batch::RecordBatch,
};
use arrow_flight::{
    FlightEndpoint, FlightDescriptor,
    flight_service_server::{FlightService, FlightServiceServer},
    PollInfo, Action, Criteria, Empty, FlightData, FlightInfo, 
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket, ActionType
};
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::encode::FlightDataEncoderBuilder;
// Tonic gRPC framework
use tonic::{Request, Response, Status, Streaming, Code};
//Env variable imports
use dotenv::dotenv;
use std::env;


#[derive(Serialize, Deserialize, Debug)]
struct QDQueryDescriptor {
    datasource: String,
    instruments: Vec<String>,  
    indicators: Vec<String>,
    start_date: String,  
    end_date: String,
}

//The descriptor_type (an integer value indicating whether the descriptor is a path or command),
//The command (a string, if applicable, otherwise None), and
//The path (a list of strings, if applicable).
type FlightKey = (i32, Option<String>, Vec<String>);

pub struct MyFlightService {
    //hash map which will store record batches – in memory data storage
    flights: Arc<Mutex<HashMap<FlightKey, Arc<RecordBatch>>>>,
}

impl MyFlightService {
    //utility function
    fn descriptor_to_key(descriptor: &FlightDescriptor) -> (i32, Option<String>, Vec<String>) {

        let descriptor_type = descriptor.r#type() as i32;
        let command = match descriptor.r#type() {
            DescriptorType::Cmd => Some(String::from_utf8(descriptor.cmd.to_vec()).unwrap_or_default()),
            _ => None,
        };
        let path = descriptor.path.clone();

        (descriptor_type, command, path)
    }

    //utility function
    fn make_flight_info(&self, key: &FlightKey, batch: &Arc<RecordBatch>, descriptor: FlightDescriptor) -> FlightInfo  {

        let (descriptor_type, command, path) = key;

        //unsure of what a ticket would actually consist of.
        let ticket_contents = json!({
            "descriptor_type": descriptor_type,
            "command": command,
            "path": path
        }).to_string();

        let flight_info = FlightInfo::new()
            .try_with_schema(batch.schema().as_ref())
            .expect("encoding failed")
            .with_endpoint(
                FlightEndpoint::new().with_ticket(Ticket::new(ticket_contents.as_bytes().to_vec()))
            )
            .with_descriptor(descriptor);

        flight_info

    }

    //utility function
    async fn make_new_flight_info(&self, key: &FlightKey, batch: RecordBatch, descriptor: FlightDescriptor) -> FlightInfo  {

        let (descriptor_type, command, path) = key;

        //unsure of what a ticket would actually consist of.
        let ticket_contents = json!({
            "descriptor_type": descriptor_type,
            "command": command,
            "path": path
        }).to_string();

        let flight_info = FlightInfo::new()
            .try_with_schema(batch.schema().as_ref())
            .expect("encoding failed")
            .with_endpoint(
                FlightEndpoint::new().with_ticket(Ticket::new(ticket_contents.as_bytes().to_vec()))
            )
            .with_descriptor(descriptor);

        //Adding new record batch to flights hashmap 
        let arc_batch = Arc::new(batch);
        let mut flights_lock = self.flights.lock().await; // Acquire the lock
        flights_lock.insert(key.clone(), arc_batch); // Insert the batch with the key into the hashmap
        flight_info

    }

    //utility function
    fn process_data(data: Value) -> Result<RecordBatch, Status> {
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
    
}

#[tonic::async_trait]
impl FlightService for MyFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        unimplemented!()
    }

    //look into making this function yield each flight descriptor, rather than waiting and returning
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        
        let flights = self.flights.lock().await;
    
        let flight_infos: Vec<Result<FlightInfo, Status>> = flights.iter().map(|(key, batch)| {
   
            let (descriptor_type, command, path) = key;

            let descriptor = match DescriptorType::try_from(*descriptor_type){
                Ok(DescriptorType::Cmd) => match command {
                    Some(cmd) => FlightDescriptor::new_cmd(cmd.clone().into_bytes()),
                    None => return Err(Status::invalid_argument("Command is required for CMD descriptor type")),
                },
                Ok(DescriptorType::Path) => FlightDescriptor::new_path(path.clone()),
                _ => return Err(Status::invalid_argument("Unknown descriptor type")),
            };

            Ok(self.make_flight_info(key, batch, descriptor))
        }).collect();

        let output_stream: futures::stream::Iter<std::vec::IntoIter<Result<FlightInfo, Status>>> = futures::stream::iter(flight_infos);
        Ok(Response::new(Box::pin(output_stream)))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {

        let descriptor = _request.into_inner();
        
        // Use descriptor_to_key to convert the FlightDescriptor to a key
        let key = Self::descriptor_to_key(&descriptor);

        let batch = {
            let flights = self.flights.lock().await;
            flights.get(&key).cloned()
        };

        match batch {
            Some(batch) => {
                //Means batch already exists in dataserver
                return Ok(Response::new(self.make_flight_info(&key, &batch, descriptor)));
            },
            None => {
                // If the key does not exist, go fetch info from datalake
                let cmd = String::from_utf8(descriptor.cmd.to_vec()).unwrap();

                // Parse the command
                let qd_query: QDQueryDescriptor = serde_json::from_str(&cmd).unwrap();
                let instruments = qd_query.instruments.join(",");
                let indicators = qd_query.indicators.join(",");

                //Send request to toy datalake
                dotenv().ok();
                let url_address = env::var("DATA_LAKE_ADDRESS").expect("DATA_LAKE_ADDRESS must be set");
                let client = reqwest::Client::new();
                let url = format!("{}/api/data?instruments={}&indicators={}", url_address, instruments, indicators);
                println!("Request URL: {}", url); 
                
                let response = client.get(&url).send().await;

                
                match response {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            let data: Value = resp.json().await.unwrap(); // handle JSON response
                            //tranform JSON data into record batch
                            let new_batch =  MyFlightService::process_data(data);

                            match new_batch {
                                Ok(batch) => {
                                    Ok(Response::new(self.make_new_flight_info(&key, batch, descriptor).await))
                                }
                                _ => {
                                    Err(Status::internal("Failed to create flight info"))
                                }
                            }                            
                        } else {
                            Err(Status::internal("Failed to fetch data from API"))
                        }
                    },
                    Err(_) => Err(Status::internal("API request failed"))
                }
            },
        }
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        unimplemented!()
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        unimplemented!()
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // Deserialize the ticket to JSON
        let ticket = request.into_inner();
        let ticket_str = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|_| Status::invalid_argument("Ticket contains invalid UTF-8"))?;
        let ticket_json: serde_json::Value = serde_json::from_str(&ticket_str)
            .map_err(|_| Status::invalid_argument("Ticket cannot be deserialized"))?;
        
        // Convert ticket JSON to FlightKey
        let key = (
            ticket_json["descriptor_type"].as_i64().unwrap_or_default() as i32,
            ticket_json["command"].as_str().map(|s| s.to_string()),
            ticket_json["path"].as_array().map_or(vec![], |v| {
                v.iter().filter_map(|p| p.as_str().map(|s| s.to_string())).collect()
            }),
        );
        let flights = self.flights.lock().await;
        // Lock the flights map and attempt to find the corresponding batch
        
        if let Some(arc_batch) = flights.get(&key) {
            let batch = Arc::clone(arc_batch);

            // CLONING RECORD BATCH HERE??? Is there a way to do this without cloning record batch??
            // Seems like this could be expensive for large record batches
            let input_stream = futures::stream::iter(vec![Ok(batch.as_ref().clone())]);
            
            // Build a stream of `Result<FlightData, Status>` using FlightDataEncoderBuilder
            let flight_data_stream = FlightDataEncoderBuilder::new()
            .build(input_stream)
            .map_err(|e| {
                // Convert errors to tonic::Status
                Status::internal(format!("Internal error processing flight data: {:?}", e))
            });

    
            Ok(Response::new(Box::pin(flight_data_stream)))

        } else {
            // Handle the case where the key is not present.
            Err(Status::not_found("Flight not found"))
        }
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        unimplemented!()
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        unimplemented!()
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        unimplemented!()
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        unimplemented!()
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let addr = "[::1]:50051".parse().unwrap();
    let service = MyFlightService {
        flights: Arc::new(Mutex::new(HashMap::new())),
    };
    let server = FlightServiceServer::new(service);
    
    tonic::transport::Server::builder()
        .add_service(server)
        .serve(addr)
        .await?;
    Ok(())
    
}
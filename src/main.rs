use arrow::array::{ArrayRef, StringArray, Int32Array, BooleanArray};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use arrow_flight::{FlightEndpoint};
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::encode::FlightDataEncoderBuilder;
use futures::{TryStreamExt};
use tonic::{Request, Response, Status, Streaming};
use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer}, 
    PollInfo,Action, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket, ActionType
};
use futures::stream::BoxStream;
use std::collections::HashMap;
use std::sync::Arc;
use serde_json::json;
use tokio::sync::Mutex;


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

        let flights_lock = self.flights.lock().await;
    
        match flights_lock.get(&key) {
            Some(batch) => {
                return Ok(Response::new(self.make_flight_info(&key, batch, descriptor)));
            },
            None => {
                // If the key does not exist, return an error status
                // Note for future: if not found in memory, use flight descriptor to query data lake using JSON DSL from "cmd" field.
                Err(Status::not_found("Flight not found"))
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
        flights: initialize_test_flights(),
    };
    let server = FlightServiceServer::new(service);
    
    tonic::transport::Server::builder()
        .add_service(server)
        .serve(addr)
        .await?;
    Ok(())
    
}

fn initialize_test_flights() -> Arc<Mutex<HashMap<FlightKey, Arc<RecordBatch>>>> {
    let string_values = vec!["a", "b", "c"];
    let string_array: ArrayRef = Arc::new(StringArray::from(string_values));

    let int_values = vec![1, 2, 3];
    let int_array: ArrayRef = Arc::new(Int32Array::from(int_values));

    let bool_values = vec![true, false, true];
    let bool_array: ArrayRef = Arc::new(BooleanArray::from(bool_values));

    let string_values1 = vec!["v", "i", "c"];
    let string_array1: ArrayRef = Arc::new(StringArray::from(string_values1));

    let int_value1 = vec![7, 27, 0];
    let int_array1: ArrayRef = Arc::new(Int32Array::from(int_value1));

    let bool_values1 = vec![false, false, false];
    let bool_array1: ArrayRef = Arc::new(BooleanArray::from(bool_values1));

    // Define a schema for the RecordBatch
    let schema = Schema::new(vec![
        Field::new("chars", DataType::Utf8, false),
        Field::new("numbers", DataType::Int32, false),
        Field::new("booleans", DataType::Boolean, false),
    ]);
    let schema1 = Schema::new(vec![
        Field::new("chars", DataType::Utf8, false),
        Field::new("numbers", DataType::Int32, false),
        Field::new("booleans", DataType::Boolean, false),
    ]);


    // Create the RecordBatch
    let rc = RecordBatch::try_new(Arc::new(schema), vec![string_array, int_array, bool_array]);
    let rc1 = RecordBatch::try_new(Arc::new(schema1), vec![string_array1, int_array1, bool_array1]);
    
    let batch = rc.map_err(|e| Box::new(e) as Box<dyn std::error::Error>).unwrap();
    let batch1 = rc1.map_err(|e| Box::new(e) as Box<dyn std::error::Error>).unwrap();

    let cmd = "record batch".as_bytes().to_vec();
    let desc = FlightDescriptor::new_cmd(cmd);
    let key = MyFlightService::descriptor_to_key(&desc);
    let val = Arc::new(batch);

    let cmd1 = "record batch 1".as_bytes().to_vec();
    let desc1 = FlightDescriptor::new_cmd(cmd1);
    let key1 = MyFlightService::descriptor_to_key(&desc1);
    let val1 = Arc::new(batch1);
    
    let mut hash_map = HashMap::new();

    hash_map.insert(key, val);
    hash_map.insert(key1, val1);
    Arc::new(Mutex::new(hash_map))
}



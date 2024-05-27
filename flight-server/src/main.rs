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
use arrow::record_batch::RecordBatch;
use arrow_flight::{
    FlightEndpoint, FlightDescriptor,
    flight_service_server::{FlightService, FlightServiceServer},
    PollInfo, Action, Criteria, Empty, FlightData, FlightInfo, 
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket, ActionType
};
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::encode::FlightDataEncoderBuilder;
// Tonic gRPC framework
use tonic::{Request, Response, Status, Streaming};
//Env variable imports
use dotenv::dotenv;
use std::env;
mod utils;

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
        let key = utils::mmap_utils::descriptor_to_key(&descriptor);

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
                            let new_batch =  utils::mmap_utils::data_to_record_batch(data);

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
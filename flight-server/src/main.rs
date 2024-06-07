// Standard library imports
use std::collections::HashMap;
use std::sync::Arc;
// Serialization
use serde::{Deserialize, Serialize};
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
    flights: Arc<Mutex<HashMap<FlightKey, FlightInfo>>>,
}

impl MyFlightService {
    //utility function
    async fn make_new_flight_info(&self, key: &FlightKey, batch: RecordBatch, descriptor: FlightDescriptor) -> Result<FlightInfo,Status>  {

        let ticket = match utils::mmap_utils::batch_to_mmap(&batch) {
            Ok(ticket) => ticket,
            Err(e) => return Err(Status::internal(format!("Failed to serialize RecordBatch to mmap: {:?}", e))),
        };

        let flight_info = FlightInfo::new()
            .try_with_schema(batch.schema().as_ref())
            .expect("encoding failed")
            .with_endpoint(
                FlightEndpoint::new().with_ticket(ticket.clone())
            )
            .with_descriptor(descriptor);

        let mut flights_lock = self.flights.lock().await; // Acquire the lock
        flights_lock.insert(key.clone(), flight_info.clone()); // Insert the batch with the key into the hashmap
        Ok(flight_info)
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
        
        // Lock the flights HashMap and get the list of FlightInfo
        let flights = self.flights.lock().await;

        // Collect the FlightInfo instances into a Vec<Result<FlightInfo, Status>>
        let flight_infos: Vec<Result<FlightInfo, Status>> = flights
            .values()
            .cloned()
            .map(Ok)
            .collect();

        // Create a futures stream from the vector
        let output_stream = futures::stream::iter(flight_infos);

        // Return the stream as a response
        Ok(Response::new(Box::pin(output_stream)))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {

        let descriptor = _request.into_inner();
        
        // Use descriptor_to_key to convert the FlightDescriptor to a key
        let key = utils::helper::descriptor_to_key(&descriptor);

        let flight_info : Option<FlightInfo> = {
            let flights = self.flights.lock().await;
            flights.get(&key).cloned()
        };

        match flight_info {
            Some(f) => {
                //Means flight already exists in dataserver
                return Ok(Response::new(f));
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
                            let new_batch =  utils::helper::data_to_record_batch(data);

                            match new_batch {
                                Ok(batch) => {
                                    match self.make_new_flight_info(&key, batch, descriptor).await {
                                        Ok(flight_info) => Ok(Response::new(flight_info)),
                                        Err(e) => Err(e),
                                    }
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
        let ticket = request.into_inner();

        // Load the RecordBatch from the memory-mapped file using the file path
        let batch = utils::mmap_utils::mmap_to_batch(&ticket).map_err(|e| {
            Status::internal(format!("Failed to load RecordBatch from mmap: {:?}", e))
        })?;
    
        // Create the input stream with the RecordBatch
        let input_stream = futures::stream::iter(vec![Ok(batch)]);
    
        // Build a stream of `Result<FlightData, Status>` using FlightDataEncoderBuilder
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .build(input_stream)
            .map_err(|e| {
                Status::internal(format!("Internal error processing flight data: {:?}", e))
            });
        Ok(Response::new(Box::pin(flight_data_stream)))
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
use arrow::{
    csv,
    datatypes::{DataType, Field, Schema},
};
use arrow_flight::FlightEndpoint;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::{
    Action, ActionType, Criteria, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::{FlightService, FlightServiceServer},
};
use arrow_flight::{Empty, encode::FlightDataEncoderBuilder};
use bytes::Bytes;
use futures_core::Stream;
use futures_util::stream::{self, TryStreamExt};
use std::{fs::File, pin::Pin, sync::Arc};
use tonic::{Response, Status, Streaming};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(MyFlightServer {}))
        .serve(addr)
        .await?;
    Ok(())
}

pub struct MyFlightServer {}

type BoxedStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for MyFlightServer {
    type HandshakeStream = BoxedStream<HandshakeResponse>;
    type ListFlightsStream = BoxedStream<FlightInfo>;
    type DoGetStream = BoxedStream<FlightData>;
    type DoPutStream = BoxedStream<PutResult>;
    type DoExchangeStream = BoxedStream<FlightData>;
    type DoActionStream = BoxedStream<arrow_flight::Result>;
    type ListActionsStream = BoxedStream<ActionType>;

    async fn poll_flight_info(
        &self,
        _request: tonic::Request<FlightDescriptor>,
    ) -> Result<tonic::Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }
    // Implement required methods here
    async fn handshake(
        &self,
        _request: tonic::Request<Streaming<HandshakeRequest>>,
    ) -> Result<tonic::Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not implemented"))
    }

    async fn list_flights(
        &self,
        _request: tonic::Request<Criteria>,
    ) -> Result<tonic::Response<Self::ListFlightsStream>, Status> {
        let _schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        // FlightDescriptor: path ["uk_cities"]
        let descriptor = FlightDescriptor {
            r#type: DescriptorType::Path as i32,
            cmd: Vec::new().into(),
            path: vec!["uk_cities".to_string()],
        };

        // Endpoint with ticket "uk_cities"
        let endpoint = FlightEndpoint {
            ticket: Some(Ticket {
                ticket: Bytes::from("uk_cities"),
            }),
            location: Vec::new(),
            app_metadata: Vec::new().into(),
            expiration_time: None,
        };

        // Build FlightInfo; schema is the encoded schema message header
        let flight_info = FlightInfo {
            schema: Vec::new().into(),
            flight_descriptor: Some(descriptor),
            endpoint: vec![endpoint],
            total_records: -1,
            total_bytes: -1,
            app_metadata: Vec::new().into(),
            ordered: false,
        };

        let stream = stream::iter(vec![Ok(flight_info)]);
        Ok(Response::new(Box::pin(stream) as Self::ListFlightsStream))
    }

    async fn get_flight_info(
        &self,
        _request: tonic::Request<FlightDescriptor>,
    ) -> Result<tonic::Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info not implemented"))
    }

    async fn get_schema(
        &self,
        _request: tonic::Request<FlightDescriptor>,
    ) -> Result<tonic::Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not implemented"))
    }

    async fn do_get(
        &self,
        _request: tonic::Request<Ticket>,
    ) -> Result<tonic::Response<Self::DoGetStream>, Status> {
        // 1. Define schema for the CSV
        let schema = Arc::new(Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]));

        // 2. Open CSV file
        let file = File::open("data/uk_cities.csv")
            .map_err(|e| Status::internal(format!("failed to open CSV: {e}")))?;

        // 3. Build Arrow CSV reader
        let mut reader = csv::ReaderBuilder::new(schema.clone())
            .with_header(true) // set to true if your CSV has a header row
            .with_batch_size(1024)
            .build(file)
            .map_err(|e| Status::internal(format!("failed to build CSV reader: {e}")))?;

        // 4. Create an iterator over Result<RecordBatch, arrow_csv::Error>
        let batch_iter = std::iter::from_fn(move || reader.next());

        // 5. Turn iterator into a stream of Result<RecordBatch, FlightError>-compatible values
        // FlightDataEncoderBuilder::build expects Stream<Item = Result<RecordBatch, FlightError>>
        let batch_stream = stream::iter(batch_iter)
            .map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e)));

        // 6. Build FlightData encoder over that batch stream
        //    This yields a TryStream<Item = FlightData, Error = FlightError>
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(None)
            .build(batch_stream)
            // convert FlightError -> tonic::Status so it matches BoxedStream<FlightData>
            .map_err(|e| Status::internal(format!("encode error: {e}")));

        Ok(Response::new(
            Box::pin(flight_data_stream) as Self::DoGetStream
        ))
    }

    async fn do_put(
        &self,
        _request: tonic::Request<Streaming<FlightData>>,
    ) -> Result<tonic::Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: tonic::Request<Streaming<FlightData>>,
    ) -> Result<tonic::Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }

    async fn do_action(
        &self,
        _request: tonic::Request<Action>,
    ) -> Result<tonic::Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not implemented"))
    }

    async fn list_actions(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not implemented"))
    }
}

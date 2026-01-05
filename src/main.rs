use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow_flight::Ticket;
use arrow_flight::decode::{DecodedFlightData, DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use bytes::Bytes;
use futures_util::TryStreamExt;
use futures_util::stream::StreamExt;
use tonic::transport::Channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connect to the Flight server
    let channel = Channel::from_static("http://[::1]:50051").connect().await?;

    let mut client = FlightServiceClient::new(channel);

    // 2. Prepare a Ticket (server currently ignores content)
    let ticket = Ticket {
        ticket: Bytes::from("example_ticket"),
    };

    // 3. Call DoGet
    let response = client.do_get(ticket).await?;
    let raw_stream = response.into_inner(); // Stream<Item = Result<FlightData, Status>>

    // 4. Adapt error type: Status -> FlightError
    let flight_stream = raw_stream.map_err(|status| FlightError::ExternalError(Box::new(status)));

    // 5. Decode the FlightData stream
    let mut decoder = FlightDataDecoder::new(flight_stream);

    let mut printed_schema = false;

    while let Some(item) = decoder.next().await {
        let DecodedFlightData {
            inner: _inner_fd,
            payload,
        } = item?;

        match payload {
            DecodedPayload::Schema(schema) => {
                if !printed_schema {
                    println!("Schema: {:?}", schema);
                    printed_schema = true;
                }
            }
            DecodedPayload::RecordBatch(batch) => {
                print_batch(&batch);
            }
            // For this example, ignore dictionaries and other payloads
            _ => {}
        }
    }

    Ok(())
}

fn print_batch(batch: &RecordBatch) {
    println!("RecordBatch rows: {}", batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let mut row_vals = Vec::new();
        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx);

            let value = if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>()
            {
                if arr.is_null(row_idx) {
                    "NULL".to_string()
                } else {
                    arr.value(row_idx).to_string()
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
                if arr.is_null(row_idx) {
                    "NULL".to_string()
                } else {
                    arr.value(row_idx).to_string()
                }
            } else {
                "<unsupported>".to_string()
            };

            row_vals.push(value);
        }
        println!("{}", row_vals.join(", "));
    }
}

import pyarrow as pa
import pyarrow.flight as fl


def main():
    client = fl.FlightClient("grpc+tcp://[::1]:50051")

    print("Available flights:")
    flights = list(client.list_flights())
    for flight in flights:
        print("  descriptor:", flight.descriptor)
        print("  endpoints:", flight.endpoints)
        print("  total_records:", flight.total_records)
        print("  total_bytes:", flight.total_bytes)

    # Fetch data for the first advertised flight
    if flights:
        first = flights[0]
        endpoint = first.endpoints[0]
        print(endpoint.ticket.ticket)
        reader = client.do_get(endpoint.ticket)
        table = reader.read_all()
        print("Data for first flight:")
        print(table)


if __name__ == "__main__":
    main()

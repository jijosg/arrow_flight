# Arrow Flight playground

This is just me messing around with Apache Arrow Flight in Rust (plus a tiny Python client).

The goal: read a CSV file, serve it over Flight from a Rust server, and then read it back from:
- a Rust client
- a Python client (using `pyarrow.flight`)

Nothing here is production-grade; it’s just a sandbox.

---

## Repo layout (roughly)

- `server/`
  - Rust Flight server that:
    - advertises one dataset (`uk_cities`)
    - implements `do_get` to stream data from a CSV
    - implements a minimal `list_flights`
- `client/`
  - Rust Flight client that calls `do_get` and prints the rows
- `pyflight/`
  - Python Flight client using `pyarrow.flight`
- `data/`
  - CSV file `uk_cities.csv` used by the server

Adjust paths as needed; this is just the idea.

---

## The CSV

Expected schema:

- `city` (string)
- `lat` (float64)
- `lng` (float64)

Example file `uk_cities.csv`:

city,lat,lng  
London,51.5074,-0.1278  
Manchester,53.4808,-2.2426  
Birmingham,52.4862,-1.8904  

Make sure the server can actually find this file. Easiest is:

- Put it in `server/data/uk_cities.csv`, or
- Update the path in the server’s `do_get` implementation.

---

## Running the Rust server

From the `server` directory:

```bash
cargo run -p server
```

The server listens on `grpc://[::1]:50051` (IPv6 localhost). If that’s annoying, you can change it to `127.0.0.1:50051` in the server code.

The server implements at least:

- `list_flights`: returns a single `FlightInfo` for `uk_cities`
- `do_get`: when given a `Ticket` with `b"uk_cities"`, reads the CSV and streams Arrow `RecordBatch`es via Flight

Most of the other Flight methods are stubbed with `unimplemented`.

---

## Running the Rust client

From the `client` directory:

```bash
cargo run
```

What it does:

- Connects to `http://[::1]:50051`
- Creates a `Ticket` (e.g. `b"example_ticket"` or `b"uk_cities"` depending on how the server is written)
- Calls `do_get`
- Uses `FlightDataDecoder` to turn the Flight stream back into:
  - a schema
  - `RecordBatch`es
- Prints the schema and then every row

Make sure the server is running first.

---

## Running the Python client

From `pyflight/`:
```bash
uv run main.py
```

If everything is wired up, you should see:

- One flight with descriptor path `['uk_cities']`
- A `pyarrow.Table` with the `city`, `lat`, `lng` columns and some rows

---

## Notes / gotchas

- If the table prints with **only schema and zero rows**, check:
  - the CSV path in the server
  - whether `ReaderBuilder::with_header(true/false)` matches the file
- If `list_flights` gives “unimplemented”, the server’s `list_flights` method is still returning `Status::unimplemented`.
- If the client can’t connect, double‑check host/port and whether you’re using `[::1]` vs `localhost`.

That’s pretty much it: a tiny Arrow Flight server/client setup to get a feel for how the pieces work.

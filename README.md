# flight.mojo

Apache Arrow Flight for Mojo: an Arrow **IPC writer** and a **Flight server**
built on [flare](https://github.com/ehsanmok/flare)'s gRPC.

Both halves are verified against stock `pyarrow` — a library nobody here wrote.
A writer checked only against our own reader would prove the two agree and
nothing about whether either matches Arrow.

```
GetFlightInfo:  total_records: 5  endpoints: 1
  schema: id: int64, amount: double, flag: bool, name: string
DoGet: 5 rows x 4 cols
FLIGHT INTEROP OK
```

## Why this is a separate tin

The IPC writer could have lived in `arrow-mlake.mojo`, which already carries the
Arrow memory layout and the C Data Interface. It does not, because `arrow-mlake`
is a stopgap meant to be deleted once the community's `marrow` is usable — and
`marrow` does not cover Flight. Putting IPC there would tie a component with a
future to one built to be thrown away.

The two are also solving different problems. The C Data Interface hands pointers
to another library *in the same process*; IPC is a byte format for sending data
between processes. Flight needs the second and cannot use the first.

## What is here

| file | what it does |
|---|---|
| `src/flight/flatbuf.mojo` | A FlatBuffers **writer**. Write-only on purpose: reading arbitrary flatbuffers means honouring whatever layout a producer chose, but writing means we choose it, so the encoder only has to be self-consistent and spec-legal. |
| `src/flight/ipc.mojo` | `Schema` and `RecordBatch` messages, plus the stream framing (continuation marker, padded metadata length, 8-byte-aligned body). |
| `src/flight/writer.mojo` | Columns to body buffers, in Arrow's exact per-type buffer counts and order. |
| `src/flight/server.mojo` | The Flight service over flare's gRPC. |
| `src/flight/flight_pb.mojo` | Generated from `proto/flight.proto`; do not edit. |

Supported types are `int64`, `float64`, `bool`, `utf8` and `timestamp`.
Dictionaries, nested types and compression are not implemented, and a batch
using them is rejected rather than half-written.

A timestamp is an `int64` on the wire; only the schema knows it means a moment,
which is why `FieldSpec` carries `unit` and `tz` and the other types ignore
them. arrow-mlake's `TU_*` are Arrow's own `TimeUnit` values, so a unit read
off a scanned column passes through untranslated.

## The proto is trimmed, not different

`proto/flight.proto` uses `int32` where the official Flight uses enums, and
omits `google.protobuf.Timestamp`. This is **wire-compatible**: proto3 encodes
an enum as a varint `int32`, so a client generated from the official
`flight.proto` and this server agree on the bytes — which the pyarrow test
demonstrates. The trimming exists because flare's `proto_gen` supports scalars,
nested messages and services, but not enums, maps or `oneof`.

The generated gRPC paths are byte-identical to real Flight
(`/arrow.flight.protocol.FlightService/DoGet` and friends), which is what lets
an unmodified client connect.

## Two things worth knowing before changing this

**`FlightInfo.schema` is an encapsulated IPC message**, not a bare flatbuffer —
the continuation marker and length prefix are part of it. A bare one fails on
the client as "Invalid flatbuffers message", because the reader takes the first
four bytes as the length prefix.

**flare resolves its FFI shims through `$CONDA_PREFIX/lib` first**, falling back
to `./build`. Running a server binary outside `pixi run` leaves that unset, and
the missing `libflare_zlib.so` does not fail loudly: gRPC clients advertise
gzip, flare selects it, the encode fails, and the connection is torn down after
the response headers. It looks like a transport bug and is not one. `pixi run
verify-flight` sets it.

## Running the gates

```sh
pixi run -e verify verify-ipc       # pyarrow reads an IPC stream we wrote
pixi run -e verify verify-flight    # pyarrow.flight drives the in-memory server
pixi run -e verify verify-iceberg   # pyarrow.flight reads a real Iceberg table
pixi run -e verify verify-cluster   # ...from two worker processes behind a coordinator
```

`verify-iceberg` compares rows **keyed by id**, not by position: neither
Iceberg nor Flight promises an order, and pinning the one a run happens to
produce would fail the first time scan planning changed without anything being
wrong. Its expected values were confirmed by reading the fixture's Parquet
directly rather than transcribed from this server's output, which would only
prove it agrees with itself.

## Tickets are the unit of work

`GetFlightInfo` advertises **one endpoint per Iceberg scan task**, and a ticket
names the snapshot, the file and the byte range to read. That split is not
invented here: Iceberg's planner has already decided where the seams are —
pruning partitions, attaching delete files, computing a residual per task, and
cutting a large file at row-group boundaries — so the endpoints are its plan,
handed out.

The invariant a client depends on is that the union of every ticket is the
whole table, with nothing repeated and nothing lost. That is what entitles a
client to fetch endpoints in parallel and concatenate, and `verify-iceberg`
asserts it rather than assuming it: 6 endpoints over the fixture's 3 data
files, row counts 175+175+175+75+3+4, union of 607 rows with no duplicate
ids.

`total_records` comes from the manifests via `TableScan.count`, so an
unfiltered table is counted without opening a data file.

## Where the work happens

A server given worker URIs advertises them as the endpoints' `Location`, which
is the whole difference between one process handing out several tickets and a
fan-out across machines:

```sh
./build/serve_ice <table> --port 8816                    # a worker
./build/serve_ice <table> --port 8817                    # another
./build/serve_ice <table> --port 8815 \
    --worker grpc+tcp://127.0.0.1:8816 \
    --worker grpc+tcp://127.0.0.1:8817                   # the coordinator
```

Same binary in every role, because nothing about the protocol differs between
them. The coordinator plans the scan and answers `GetFlightInfo`; the client
connects to the locations it names and fetches from the workers directly, so
the rows never pass through the coordinator. With no `--worker` the field stays
empty, which Flight defines as "fetch from the server you asked" — still the
right answer for a single process.

Endpoints are dealt **round-robin, one location each**. Flight reads a list of
locations as "any of these can serve this ticket", and a stock client takes the
first, so advertising every worker on every endpoint would send the whole
fan-out to whichever sorted first. There is no locality to preserve — the
workers read the same object store, so any of them can serve any ticket, which
`verify-cluster` checks by reading one ticket from both and comparing.

What this is **not** is a scheduler. Nothing here retries a failed worker,
notices a slow one, or decides how many there should be. The coordinator deals
out a plan Iceberg already made, and the client does the fetching.

## Status

Early, but real. `GetFlightInfo`, `GetSchema` and `DoGet` work against a stock
client, over both a fixed in-memory source and a live Iceberg table — nulls,
timestamps and a per-file split intact. `Handshake` and `ListFlights` answer
UNIMPLEMENTED, which is a legal response a client tolerates. `FlightSource` is
the seam: a schema, a row count, the tickets, and the batches behind one
ticket.

Endpoints can be placed on other processes, and a gate proves a client reads
one table from two of them. What is missing above that is everything a
scheduler does: no retries, no straggler handling, no membership, and no
shuffle — so joins and high-cardinality group-by are out of scope rather than
slow.

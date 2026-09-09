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

Supported types are `int64`, `float64`, `bool` and `utf8`. Dictionaries, nested
types and compression are not implemented, and a batch using them is rejected
rather than half-written.

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
pixi run -e verify verify-ipc      # pyarrow reads an IPC stream we wrote
pixi run -e verify verify-flight   # pyarrow.flight drives the server
```

## Status

Early. `GetFlightInfo`, `GetSchema` and `DoGet` work against a real client with
a fixed in-memory source. `Handshake` and `ListFlights` answer UNIMPLEMENTED,
which is a legal response a client tolerates. The `FlightSource` trait is the
seam an Iceberg scan plugs into — deliberately small: a schema, a row count and
the batches.

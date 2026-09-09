"""An Arrow Flight server over flare's gRPC.

## One handler for every method

Flight mixes unary methods (`GetFlightInfo`, `GetSchema`) with server-streaming
ones (`DoGet`, `ListFlights`), and flare exposes those as two different
`Handler` types — `GrpcService` and `GrpcStreamingService` — each with its own
associated `BodyType`. A dispatcher holding both could not name a single return
type, so combining them that way does not work.

It does not need to. On the wire, gRPC unary and server-streaming are the
*same* framing: a sequence of length-prefixed messages in DATA frames followed
by trailers. A streaming reply carrying exactly one message is byte-identical
to a unary response, and a unary client is happy to read it. So every Flight
method is served by one `GrpcServerStreaming` handler that switches on
`ctx.path` and returns one message or many.

That also keeps method dispatch in one obvious place, which matters because
Flight's paths are fixed strings a client will send verbatim.

## What is implemented

`GetFlightInfo`, `GetSchema` and `DoGet` against a fixed in-memory batch —
enough to prove interoperability with a stock client. `Handshake` and
`ListFlights` return UNIMPLEMENTED, which is a legal answer a real client
tolerates.
"""

from std.collections.span import Span

from flare.grpc import (
    GrpcCallContext,
    GrpcServerStreamReply,
    GrpcServerStreaming,
    GrpcStatus,
    GrpcUnary,
    GrpcUnaryReply,
)

from .flight_pb import (
    FlightData,
    FlightDescriptor,
    FlightEndpoint,
    FlightInfo,
    SchemaResult,
    Ticket,
)
from .ipc import FieldSpec, build_schema_message, frame_message
from .writer import Column, encode_batch

comptime PATH_GET_FLIGHT_INFO: StaticString = "/arrow.flight.protocol.FlightService/GetFlightInfo"
comptime PATH_GET_SCHEMA: StaticString = "/arrow.flight.protocol.FlightService/GetSchema"
comptime PATH_DO_GET: StaticString = "/arrow.flight.protocol.FlightService/DoGet"


def _encapsulated_schema(fields: List[FieldSpec]) raises -> List[UInt8]:
    """The schema in the form `FlightInfo.schema` / `SchemaResult` expects.

    Flight carries the schema as an *encapsulated* IPC message — the
    continuation marker and length prefix included — not as a bare flatbuffer.
    A bare one decodes as "Invalid flatbuffers message" on the client, because
    the reader treats the first four bytes as the length prefix.
    """
    var msg = build_schema_message(fields)
    var empty = List[UInt8]()
    return frame_message(Span(msg), Span(empty))


def _to_list(s: Span[UInt8, _]) -> List[UInt8]:
    var out = List[UInt8]()
    for i in range(len(s)):
        out.append(s[i])
    return out^


struct FlightServer[D: Copyable & Deinitable & Movable & FlightSource](
    Copyable, GrpcServerStreaming, GrpcUnary, Movable
):
    """Serves one `FlightSource` over the Flight protocol."""

    var source: Self.D

    def __init__(out self, var source: Self.D):
        self.source = source^

    def serve_server_streaming(
        mut self,
        ctx: GrpcCallContext,
        request_bytes: Span[UInt8, _],
    ) raises -> GrpcServerStreamReply:
        var path = ctx.path

        if path == String(PATH_GET_FLIGHT_INFO):
            return self._get_flight_info(request_bytes)
        elif path == String(PATH_GET_SCHEMA):
            return self._get_schema()
        elif path == String(PATH_DO_GET):
            return self._do_get(request_bytes)

        # 12 is gRPC UNIMPLEMENTED. A client that probes an optional method
        # expects this rather than a connection error.
        return GrpcServerStreamReply.err(
            GrpcStatus.err(12, String("unimplemented: ") + path)
        )

    def serve_unary(
        mut self,
        ctx: GrpcCallContext,
        request_bytes: Span[UInt8, _],
    ) raises -> GrpcUnaryReply:
        """The unary half of the same dispatch.

        Serving the unary methods through `GrpcService` rather than through
        the streaming service is not a style choice: flare's server-streaming
        path is not currently readable by grpc-core (it sends response HEADERS
        and then closes the connection without DATA or trailers, though curl
        over h2 and flare's own client both read it fine). Unary responses use
        an inline body and interoperate, so `GetFlightInfo` and `GetSchema`
        work against a stock client today while `DoGet` waits on that fix.
        """
        var reply = self.serve_server_streaming(ctx, request_bytes)
        if not reply.status.is_ok():
            return GrpcUnaryReply.err(reply.status.copy())
        if len(reply.messages) == 0:
            return GrpcUnaryReply.ok(List[UInt8]())
        return GrpcUnaryReply.ok(reply.messages[0].copy())

    def _get_flight_info(
        mut self, request_bytes: Span[UInt8, _]
    ) raises -> GrpcServerStreamReply:
        """Answer with the schema and a single self-ticketed endpoint.

        The endpoint carries no `Location`, which Flight defines as "fetch
        from the same server you asked" — the right answer for a single-node
        server and the one that avoids inventing a hostname the client may not
        be able to reach.
        """
        var desc = FlightDescriptor()
        try:
            desc = FlightDescriptor.decode(request_bytes)
        except:
            pass  # a malformed descriptor still gets the whole dataset

        var info = FlightInfo()
        info.schema = _encapsulated_schema(self.source.fields())
        info.flight_descriptor = desc^

        var ticket = Ticket()
        ticket.ticket = _to_list(String("default").as_bytes())
        var ep = FlightEndpoint()
        ep.ticket = ticket^
        info.endpoint.append(ep^)

        info.total_records = Int64(self.source.total_records())
        info.total_bytes = Int64(-1)  # Flight's "unknown"

        var msgs = List[List[UInt8]]()
        msgs.append(info.encode())
        return GrpcServerStreamReply.ok(msgs^)

    def _get_schema(mut self) raises -> GrpcServerStreamReply:
        var res = SchemaResult()
        res.schema = _encapsulated_schema(self.source.fields())
        var msgs = List[List[UInt8]]()
        msgs.append(res.encode())
        return GrpcServerStreamReply.ok(msgs^)

    def _do_get(
        mut self, request_bytes: Span[UInt8, _]
    ) raises -> GrpcServerStreamReply:
        """Stream the schema message, then one `FlightData` per batch.

        A Flight stream always opens with the schema: the client builds its
        reader from that before any batch arrives. Each message is a
        `FlightData` whose `data_header` is the IPC flatbuffer and whose
        `data_body` is the buffers it describes — the framing that
        `frame_message` adds for a file stream is *not* used here, because
        gRPC already delimits messages.
        """
        var fields = self.source.fields()
        var msgs = List[List[UInt8]]()

        var schema_msg = FlightData()
        schema_msg.data_header = build_schema_message(fields)
        msgs.append(schema_msg.encode())

        var batches = self.source.batches()
        for i in range(len(batches)):
            var pair = encode_batch(fields, batches[i])
            var fd = FlightData()
            fd.data_header = pair[0].copy()
            fd.data_body = pair[1].copy()
            msgs.append(fd.encode())

        return GrpcServerStreamReply.ok(msgs^)


trait FlightSource(Movable):
    """What a Flight server needs from whatever is serving the data.

    Deliberately small: a schema, a row count and the batches. An Iceberg
    scan, a Parquet file or a literal in-memory table can all satisfy it, and
    the server does not care which.
    """

    def fields(self) raises -> List[FieldSpec]:
        ...

    def total_records(self) raises -> Int:
        ...

    def batches(self) raises -> List[List[Column]]:
        ...

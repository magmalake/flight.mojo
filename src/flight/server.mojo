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

## Whose mistake it was

Every answer that is not OK carries a deliberate status, because a client
decides what to do next from the code and nothing else:

| status | when | what a client should do |
|---|---|---|
| `INVALID_ARGUMENT` | the request cannot be parsed: a malformed descriptor or `Ticket` message, a ticket that is not UTF-8, or one the source cannot read (`invalid_argument`) | fix the request; retrying it is pointless |
| `NOT_FOUND` | a well-formed request names something that does not exist, such as a snapshot the table does not have (`not_found`) | plan again with `GetFlightInfo` |
| `UNIMPLEMENTED` | a Flight method this server does not serve | do without it |
| `INTERNAL` | anything else: the server failed | report it; the cause is in the server's log |

A source picks the first two by raising `flight_error` (or its
`invalid_argument` / `not_found` shorthands). Any other raise is a server
fault and becomes INTERNAL with a fixed message, its text written to the
server's stderr and not to the client, the same rule flare applies to a
handler that raises: the text can hold paths and anything else the exception
carried. Without this a typo in a ticket and a corrupt table would reach the
client as the same flare "handler raised".

## Where the work happens

A `FlightServer` built with a list of worker URIs advertises them as the
endpoints' `Location`, which is the whole difference between "one server that
happens to hand out several tickets" and a fan-out across machines. The
response shape does not change — the same `GetFlightInfo` a single process
answers, with the field filled in — so a client that already reads endpoints in
parallel starts reading them from different hosts without being told anything
new.
"""

from std.collections.span import Span
from std.os import getenv
from std.sys import stderr
from std.time import perf_counter_ns

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
    Location,
    SchemaResult,
    Ticket,
)
from .ipc import FieldSpec, build_schema_message, frame_message
from .writer import Column, encode_batch

comptime PATH_GET_FLIGHT_INFO: StaticString = "/arrow.flight.protocol.FlightService/GetFlightInfo"
comptime PATH_GET_SCHEMA: StaticString = "/arrow.flight.protocol.FlightService/GetSchema"
comptime PATH_DO_GET: StaticString = "/arrow.flight.protocol.FlightService/DoGet"


comptime _STATUS_TAG: StaticString = "flight-status:"
"""Prefix of an `Error` that names the gRPC status it should be answered with.

An `Error` carries only text, so the code travels in it:
`flight-status:<code>:<message>`. Only `flight_error` writes it and only
`status_for` reads it.
"""


def flight_error(code: Int, message: String) -> Error:
    """An error that `FlightServer` answers with status `code` and `message`.

    For a `FlightSource` to say the *client* got something wrong. `message`
    goes to the client verbatim, so it should name what was wrong with the
    request and nothing about the server's files.
    """
    return Error(String(_STATUS_TAG) + String(code) + String(":") + message)


def invalid_argument(message: String) -> Error:
    """The request cannot be parsed: answered INVALID_ARGUMENT."""
    return flight_error(GrpcStatus.INVALID_ARGUMENT, message)


def not_found(message: String) -> Error:
    """The request is well formed and names something that does not exist:
    answered NOT_FOUND."""
    return flight_error(GrpcStatus.NOT_FOUND, message)


comptime INTERNAL_MESSAGE: StaticString = (
    "flight: the server failed; the cause is in its log"
)
"""What a client is told about a server fault. The cause stays in the log."""


def status_for(e: Error) -> GrpcStatus:
    """The status to answer a raise with.

    A `flight_error` keeps its code and message. Anything else is a server
    fault: INTERNAL, with the text logged here rather than sent, because it
    can hold file paths and whatever else the exception carried. A tag naming
    OK or a code outside gRPC's range is a bug in the raiser and is a server
    fault too, rather than an error answered as success.
    """
    var text = String(e)
    var tag = String(_STATUS_TAG)
    if text.startswith(tag):
        var rest = String(text[byte = tag.byte_length() :])
        var colon = rest.find(":").or_else(-1)
        if colon > 0:
            try:
                var code = atol(rest[byte=:colon])
                if code > GrpcStatus.OK and code <= GrpcStatus.UNAUTHENTICATED:
                    return GrpcStatus.err(
                        code, String(rest[byte = colon + 1 :])
                    )
            except:
                pass
    print("flight: request failed:", text, file=stderr, flush=True)
    return GrpcStatus.err(GrpcStatus.INTERNAL, String(INTERNAL_MESSAGE))


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


def _decode_descriptor(
    request_bytes: Span[UInt8, _]
) raises -> FlightDescriptor:
    try:
        return FlightDescriptor.decode(request_bytes)
    except:
        raise invalid_argument(
            "flight: the request is not a FlightDescriptor message"
        )


def _to_list(s: Span[UInt8, _]) -> List[UInt8]:
    var out = List[UInt8]()
    for i in range(len(s)):
        out.append(s[i])
    return out^


struct FlightServer[D: Copyable & Deinitable & FlightSource](
    Copyable, GrpcServerStreaming, GrpcUnary, Movable
):
    """Serves one `FlightSource` over the Flight protocol."""

    var source: Self.D

    var locations: List[String]
    """Where the endpoints say to fetch from. Empty means "from me".

    A non-empty list turns this server into a **coordinator**: it still plans
    the scan and still serves `DoGet` to anyone who asks it, but the endpoints
    it advertises point somewhere else, so a client fetches the data from the
    workers directly and no rows pass through here.

    Placement is deliberately not the source's business. Which machines exist
    is a property of the deployment, and the ticket is opaque bytes that any
    worker reading the same table can honour — so a location is a routing
    hint, not an ownership claim.
    """

    def __init__(out self, var source: Self.D):
        self.source = source^
        self.locations = List[String]()

    def __init__(out self, var source: Self.D, var locations: List[String]):
        """A coordinator that spreads its endpoints over `locations`.

        Each URI is one a client will connect to verbatim
        (`grpc+tcp://host:port`), so it has to be reachable from the *client*,
        not merely from here.
        """
        self.source = source^
        self.locations = locations^

    def serve_server_streaming(
        mut self,
        ctx: GrpcCallContext,
        request_bytes: Span[UInt8, _],
    ) raises -> GrpcServerStreamReply:
        var path = ctx.path

        # Every raise below is answered here, with the status it names or
        # INTERNAL, so none reaches flare as its generic "handler raised".
        try:
            if path == String(PATH_GET_FLIGHT_INFO):
                return self._get_flight_info(request_bytes)
            elif path == String(PATH_GET_SCHEMA):
                return self._get_schema(request_bytes)
            elif path == String(PATH_DO_GET):
                return self._do_get(request_bytes)
        except e:
            return GrpcServerStreamReply.err(status_for(e))

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
        """Answer with the schema and one endpoint per unit of work.

        With no `locations` configured the endpoints carry no `Location`,
        which Flight defines as "fetch from the same server you asked" — the
        right answer for a single process, and the one that avoids inventing a
        hostname the client may not be able to reach.
        """
        # The descriptor is not used to choose anything -- a server holds one
        # dataset -- but bytes that are not a FlightDescriptor at all are a
        # broken client, and answering them with the dataset would hide that.
        # Unknown fields are skipped by the decoder, so only garbage fails.
        var desc = _decode_descriptor(request_bytes)

        var info = FlightInfo()
        info.schema = _encapsulated_schema(self.source.fields())
        info.flight_descriptor = desc^

        # One endpoint per ticket, dealt round-robin over the workers.
        #
        # One location each, not all of them on every endpoint: Flight reads a
        # list as "any of these can serve this ticket", which is a statement
        # about replication, and a stock client takes the first. Advertising
        # every worker on every endpoint would therefore send the whole
        # fan-out to whichever one sorted first — the opposite of the point.
        # Dealing them out spreads the work with a client that does the
        # obvious thing.
        #
        # Round-robin and not data locality, because there is none to have:
        # the workers read the same object store, so any of them can serve any
        # ticket. What placement buys here is parallelism, and the assignment
        # is stable for a given plan because the ticket order is.
        var tickets = self.source.tickets()
        for i in range(len(tickets)):
            var ticket = Ticket()
            ticket.ticket = _to_list(tickets[i].as_bytes())
            var ep = FlightEndpoint()
            ep.ticket = ticket^
            if len(self.locations) > 0:
                var loc = Location()
                loc.uri = self.locations[i % len(self.locations)]
                ep.location.append(loc^)
            info.endpoint.append(ep^)

        info.total_records = Int64(self.source.total_records())
        info.total_bytes = Int64(-1)  # Flight's "unknown"

        var msgs = List[List[UInt8]]()
        msgs.append(info.encode())
        # Never gzip a record batch. Every gRPC client advertises
        # `grpc-accept-encoding: gzip`, and flare compresses when it is
        # offered — which for Arrow buffers ran at about 25 MB/s and was 98%
        # of what a client waited for, against 16 ms of actually reading the
        # rows. Arrow's whole argument is that the consumer casts the buffers
        # where they land; deflating them on the way out gives that up twice.
        return GrpcServerStreamReply.ok(msgs^, compress=False)

    def _get_schema(
        mut self, request_bytes: Span[UInt8, _]
    ) raises -> GrpcServerStreamReply:
        _ = _decode_descriptor(request_bytes)
        var res = SchemaResult()
        res.schema = _encapsulated_schema(self.source.fields())
        var msgs = List[List[UInt8]]()
        msgs.append(res.encode())
        # Never gzip a record batch. Every gRPC client advertises
        # `grpc-accept-encoding: gzip`, and flare compresses when it is
        # offered — which for Arrow buffers ran at about 25 MB/s and was 98%
        # of what a client waited for, against 16 ms of actually reading the
        # rows. Arrow's whole argument is that the consumer casts the buffers
        # where they land; deflating them on the way out gives that up twice.
        return GrpcServerStreamReply.ok(msgs^, compress=False)

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
        # The ticket names which unit of work to read. A stream always opens
        # with the schema even when the ticket turns out to be empty: the
        # client builds its reader from that message, so a stream that skipped
        # it because there were no rows would fail rather than return nothing.
        var req: Ticket
        try:
            req = Ticket.decode(request_bytes)
        except:
            raise invalid_argument(
                "flight: the request is not a Ticket message"
            )
        # A ticket is opaque bytes on the wire, but `FlightSource` reads it as
        # text, and that text is only safe to build from valid UTF-8.
        var which: String
        try:
            which = String(from_utf8=Span(req.ticket))
        except:
            raise invalid_argument("flight: the ticket is not UTF-8 text")

        var fields = self.source.fields()
        var msgs = List[List[UInt8]]()

        var schema_msg = FlightData()
        schema_msg.data_header = build_schema_message(fields)
        msgs.append(schema_msg.encode())

        # FLIGHT_TIMING=1 splits one DoGet into its stages. What a client
        # waits for and what the server spends are different numbers, and
        # without both there is no way to tell an expensive read from an
        # expensive send.
        var timing = getenv("FLIGHT_TIMING", "") != ""
        var t0 = perf_counter_ns()
        var batches = self.source.batches(which)
        var t1 = perf_counter_ns()
        var rows = 0
        var bytes_out = 0
        for i in range(len(batches)):
            var pair = encode_batch(fields, batches[i])
            var fd = FlightData()
            fd.data_header = pair[0].copy()
            fd.data_body = pair[1].copy()
            rows += batches[i][0].length() if len(batches[i]) else 0
            bytes_out += len(fd.data_body)
            msgs.append(fd.encode())
        var t2 = perf_counter_ns()
        if timing:
            print(
                "do_get:",
                len(batches),
                "batch(es),",
                rows,
                "rows, read",
                (t1 - t0) // 1000000,
                "ms, encode",
                (t2 - t1) // 1000000,
                "ms,",
                bytes_out // (1024 * 1024),
                "MiB",
                flush=True,
            )

        # Never gzip a record batch. Every gRPC client advertises
        # `grpc-accept-encoding: gzip`, and flare compresses when it is
        # offered — which for Arrow buffers ran at about 25 MB/s and was 98%
        # of what a client waited for, against 16 ms of actually reading the
        # rows. Arrow's whole argument is that the consumer casts the buffers
        # where they land; deflating them on the way out gives that up twice.
        return GrpcServerStreamReply.ok(msgs^, compress=False)


trait FlightSource(Movable):
    """What a Flight server needs from whatever is serving the data.

    Deliberately small: a schema, a row count, a list of tickets and the
    batches behind one ticket. An Iceberg scan, a Parquet file or a literal
    in-memory table can all satisfy it, and the server does not care which.

    **Tickets are the unit of work.** `GetFlightInfo` turns each into an
    endpoint, and a client is free to fetch them independently and in
    parallel — that is the whole point of Flight advertising more than one.
    A source with nothing to split returns a single ticket; one backed by
    Iceberg returns one per data file, because the scan planner has already
    decided where the seams are.

    The union of every ticket's batches must be the whole result, and no two
    tickets may return the same row, or a client that fetches them all gets
    the wrong answer.

    **A raise is a server fault unless it says otherwise.** A source that
    finds the *request* wrong raises `invalid_argument` (a ticket it cannot
    parse) or `not_found` (a well-formed one naming something that does not
    exist), and the client gets that status and message. Anything else it
    raises is answered INTERNAL; see the module doc.
    """

    def fields(self) raises -> List[FieldSpec]:
        ...

    def total_records(self) raises -> Int:
        ...

    def tickets(self) raises -> List[String]:
        """One opaque ticket per unit of work, in no particular order."""
        ...

    def batches(self, ticket: String) raises -> List[List[Column]]:
        """The batches behind one ticket from `tickets`.

        `ticket` is whatever the client sent, which need not be one this
        source handed out: refuse it with `invalid_argument` or `not_found`
        rather than with a bare raise.
        """
        ...

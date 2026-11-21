"""The status every bad request is answered with, without a socket.

The gates in `tools/` check the same thing through a stock pyarrow client;
this checks the cases that client cannot send, such as bytes that are not a
protobuf message at all, and the one it cannot provoke: a source failing on
its own.

    pixi run test
"""

from flare.grpc import GrpcCallContext, GrpcMetadata, GrpcStatus
from std.testing import TestSuite, assert_equal, assert_true

from flight.flight_pb import FlightDescriptor, Ticket
from flight.ipc import DT_INT64, FieldSpec
from flight.server import (
    INTERNAL_MESSAGE,
    PATH_DO_GET,
    PATH_GET_FLIGHT_INFO,
    PATH_GET_SCHEMA,
    FlightServer,
    FlightSource,
    flight_error,
    invalid_argument,
    not_found,
    status_for,
)
from flight.writer import Column


struct ProbeSource(Copyable, FlightSource, Movable):
    """One ticket, `t`, and one ticket per way a source can refuse."""

    def __init__(out self):
        pass

    def fields(self) raises -> List[FieldSpec]:
        var f = List[FieldSpec]()
        f.append(FieldSpec.simple("id", DT_INT64, False))
        return f^

    def total_records(self) raises -> Int:
        return 2

    def tickets(self) raises -> List[String]:
        var t = List[String]()
        t.append(String("t"))
        return t^

    def batches(self, ticket: String) raises -> List[List[Column]]:
        if ticket == String("bad"):
            raise invalid_argument("bad ticket")
        if ticket == String("gone"):
            raise not_found("no such thing")
        if ticket == String("boom"):
            raise Error("/secret/path: disk on fire")
        if ticket == String("empty"):
            return List[List[Column]]()
        var ids = List[Int64]()
        ids.append(1)
        ids.append(2)
        var cols = List[Column]()
        cols.append(Column.int64(ids^))
        var out = List[List[Column]]()
        out.append(cols^)
        return out^


def _call(path: StaticString, var body: List[UInt8]) raises -> GrpcStatus:
    var server = FlightServer(ProbeSource())
    var ctx = GrpcCallContext(String(path), 0, GrpcMetadata(), String(""))
    var reply = server.serve_server_streaming(ctx, Span(body))
    var unary = server.serve_unary(ctx, Span(body))
    # Both halves of the dispatch answer the same way.
    assert_equal(unary.status.code, reply.status.code)
    return reply.status.copy()


def _do_get(ticket: String) raises -> GrpcStatus:
    var t = Ticket()
    for b in ticket.as_bytes():
        t.ticket.append(b)
    return _call(PATH_DO_GET, t.encode())


def _garbage() -> List[UInt8]:
    """Field 1, length-delimited, claiming five bytes and carrying one."""
    var b = List[UInt8]()
    b.append(0x0A)
    b.append(0x05)
    b.append(0x01)
    return b^


def test_do_get_ok() raises:
    assert_equal(_do_get("t").code, GrpcStatus.OK)
    # A ticket with no rows is a stream with only the schema, not an error.
    assert_equal(_do_get("empty").code, GrpcStatus.OK)


def test_do_get_source_refusals() raises:
    var s = _do_get("bad")
    assert_equal(s.code, GrpcStatus.INVALID_ARGUMENT)
    assert_equal(s.message, "bad ticket")
    s = _do_get("gone")
    assert_equal(s.code, GrpcStatus.NOT_FOUND)
    assert_equal(s.message, "no such thing")


def test_do_get_server_fault_is_internal_and_quiet() raises:
    var s = _do_get("boom")
    assert_equal(s.code, GrpcStatus.INTERNAL)
    # The raise text stays on the server.
    assert_equal(s.message, String(INTERNAL_MESSAGE))
    assert_true("secret" not in s.message)


def test_do_get_not_a_ticket_message() raises:
    assert_equal(
        _call(PATH_DO_GET, _garbage()).code, GrpcStatus.INVALID_ARGUMENT
    )


def test_do_get_ticket_not_utf8() raises:
    var t = Ticket()
    t.ticket.append(0xFF)
    t.ticket.append(0xFE)
    assert_equal(
        _call(PATH_DO_GET, t.encode()).code, GrpcStatus.INVALID_ARGUMENT
    )


def test_get_flight_info() raises:
    var d = FlightDescriptor()
    d.path.append(String("x"))
    assert_equal(_call(PATH_GET_FLIGHT_INFO, d.encode()).code, GrpcStatus.OK)
    # An empty body is a default descriptor, which is legal proto3.
    assert_equal(_call(PATH_GET_FLIGHT_INFO, List[UInt8]()).code, GrpcStatus.OK)
    assert_equal(
        _call(PATH_GET_FLIGHT_INFO, _garbage()).code,
        GrpcStatus.INVALID_ARGUMENT,
    )


def test_get_schema() raises:
    assert_equal(_call(PATH_GET_SCHEMA, List[UInt8]()).code, GrpcStatus.OK)
    assert_equal(
        _call(PATH_GET_SCHEMA, _garbage()).code, GrpcStatus.INVALID_ARGUMENT
    )


def test_unimplemented() raises:
    assert_equal(
        _call(
            "/arrow.flight.protocol.FlightService/ListFlights", List[UInt8]()
        ).code,
        GrpcStatus.UNIMPLEMENTED,
    )


def test_status_for() raises:
    var s = status_for(flight_error(GrpcStatus.NOT_FOUND, "a: b"))
    assert_equal(s.code, GrpcStatus.NOT_FOUND)
    assert_equal(s.message, "a: b")
    # A tag naming OK, or no code at all, is the raiser's bug: INTERNAL.
    assert_equal(
        status_for(flight_error(GrpcStatus.OK, "x")).code, GrpcStatus.INTERNAL
    )
    assert_equal(
        status_for(Error("flight-status:x:y")).code, GrpcStatus.INTERNAL
    )
    assert_equal(status_for(Error("plain")).code, GrpcStatus.INTERNAL)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

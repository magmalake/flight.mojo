"""Serve a fixed batch over Flight, for a stock pyarrow client to read."""

from flare.grpc import GrpcStreamingService
from flare.http import HttpServer
from flare.net import SocketAddr

from flight.ipc import DT_BOOL, DT_FLOAT64, DT_INT64, DT_UTF8, FieldSpec
from flight.server import FlightServer, FlightSource
from flight.writer import Column


struct DemoSource(Copyable, FlightSource, Movable):
    """A fixed batch, standing in for whatever really produces the data."""

    def __init__(out self):
        pass

    def fields(self) raises -> List[FieldSpec]:
        var f = List[FieldSpec]()
        f.append(FieldSpec("id", DT_INT64, False))
        f.append(FieldSpec("amount", DT_FLOAT64, False))
        f.append(FieldSpec("flag", DT_BOOL, False))
        f.append(FieldSpec("name", DT_UTF8, False))
        return f^

    def total_records(self) raises -> Int:
        return 5

    def batches(self) raises -> List[List[Column]]:
        var ids = List[Int64]()
        var amounts = List[Float64]()
        var flags = List[Bool]()
        var names = List[String]()
        for i in range(5):
            ids.append(Int64(100 + i))
            amounts.append(Float64(i) * 1.5)
            flags.append(i % 2 == 0)
            names.append(String("row-") + String(i))

        var cols = List[Column]()
        cols.append(Column.int64(ids^))
        cols.append(Column.float64(amounts^))
        cols.append(Column.bool_(flags^))
        cols.append(Column.utf8(names^))

        var out = List[List[Column]]()
        out.append(cols^)
        return out^


def main() raises:
    var srv = HttpServer.bind(SocketAddr.localhost(8815))
    print("flight server on 127.0.0.1:8815", flush=True)
    srv.serve(GrpcStreamingService(FlightServer(DemoSource())))

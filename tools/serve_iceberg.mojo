"""Serve a real Iceberg table over Flight.

This is the demo that makes the `FlightSource` trait mean something: the
in-memory source proved the protocol, and this proves the seam. A stock
`pyarrow.flight` client asks for a table it knows nothing about, and gets rows
that came off Parquet files through Iceberg scan planning.

The glue lives in `tools/` rather than `src/` on purpose. `flight.mojo`'s
library half depends only on flare; making it depend on iceberg and parquet as
well would force that on every consumer, including ones serving something else
entirely. `FlightSource` is deliberately small — a schema, a row count, the
batches — so the dependency belongs to whoever implements it.

## Linking flare and the Iceberg stack

This needed a fix elsewhere. flare and `threads.mojo` each bound
`pthread_create` (and `pthread_join`, `pthread_self`,
`pthread_setaffinity_np`) with ABI-identical but differently spelled
signatures — `UnsafePointer` against `Pointer`, the same machine pointer
behind different wrappers — and Mojo declares an extern *per signature*, so a
binary needing both refused to lower. Iceberg's scan parallelism is
`threads.mojo` and the server is flare, so this file could not be built at
all.

flare's threading is now an adapter over `threads.mojo`, which owns the
bindings, so there is one declaration per symbol and the two link cleanly.

Run:
    pixi run serve-iceberg <table-dir>
    pixi run -e verify verify-iceberg <table-dir>
"""

from std.sys import argv

from arrow_mlake.arrow import (
    AT_BOOL,
    AT_FLOAT64,
    AT_INT64,
    AT_UTF8,
    ArrayData,
    bit_get,
    load_f64,
    load_i64,
)
from flare.grpc import GrpcStreamingService
from flare.http import HttpServer
from flare.net import SocketAddr
from iceberg.catalog.filesystem import find_latest_metadata
from iceberg.io import FileIO
from iceberg.metadata import TableMetadata
from iceberg.read import ScanOptions
from iceberg.scan import TableScan
from parquet.reader import RecordBatch

from flight.ipc import DT_BOOL, DT_FLOAT64, DT_INT64, DT_UTF8, FieldSpec
from flight.server import FlightServer, FlightSource
from flight.writer import Column

def _read_text(path: String) raises -> String:
    with open(path, "r") as f:
        return f.read()


def _wanted() -> List[String]:
    """The fixture columns whose Arrow types this IPC writer covers.

    `ts` is a timestamp and is left out rather than mis-encoded: the writer
    rejects types it cannot lay out, and quietly coercing one would be worse.
    """
    var c = List[String]()
    c.append(String("id"))
    c.append(String("region"))
    c.append(String("amount"))
    c.append(String("ok"))
    return c^


def _dtype_of(a: ArrayData) raises -> Int:
    if a.type.id == AT_INT64:
        return DT_INT64
    elif a.type.id == AT_FLOAT64:
        return DT_FLOAT64
    elif a.type.id == AT_BOOL:
        return DT_BOOL
    elif a.type.id == AT_UTF8:
        return DT_UTF8
    raise Error(
        String("flight: column '")
        + a.name
        + String("' has Arrow type ")
        + String(a.type.id)
        + String(", which this IPC writer does not encode")
    )


def _to_column(a: ArrayData) raises -> Column:
    """Copy one Arrow array into a `Column`.

    A copy, not a view: `Column` holds typed lists while `ArrayData` holds raw
    buffers. Passing the buffers straight through would avoid this and is the
    obvious optimisation once the shape is settled — the bytes are already in
    Arrow layout, which is the whole point of the format.
    """
    var dt = _dtype_of(a)
    var col: Column

    if dt == DT_INT64:
        var v = List[Int64]()
        var buf = Span(a.values)
        for i in range(a.length):
            v.append(load_i64(buf, i))
        col = Column.int64(v^)
    elif dt == DT_FLOAT64:
        var v = List[Float64]()
        var buf = Span(a.values)
        for i in range(a.length):
            v.append(load_f64(buf, i))
        col = Column.float64(v^)
    elif dt == DT_BOOL:
        var v = List[Bool]()
        var buf = Span(a.values)
        for i in range(a.length):
            v.append(bit_get(buf, i))
        col = Column.bool_(v^)
    else:
        # utf8: offsets index into the value bytes; offsets[i+1] - offsets[i]
        # is the length, which is how a zero-length string stays distinct from
        # a null.
        var v = List[String]()
        for i in range(a.length):
            var start = Int(a.offsets[i])
            var end = Int(a.offsets[i + 1])
            var b = List[UInt8]()
            for k in range(start, end):
                b.append(a.values[k])
            v.append(String(unsafe_from_utf8=Span(b)))
        col = Column.utf8(v^)

    # Validity travels with the values: a batch that drops it would report
    # nulls as whatever the value buffer happened to hold.
    if a.null_count > 0:
        var valid = List[Bool]()
        var vb = Span(a.validity)
        for i in range(a.length):
            valid.append(bit_get(vb, i))
        col.valid = valid^
    return col^


struct IcebergSource(Copyable, FlightSource, Movable):
    """A `FlightSource` backed by an Iceberg table scan."""

    var table_dir: String

    def __init__(out self, var table_dir: String):
        self.table_dir = table_dir^

    def _scan(self) raises -> List[RecordBatch]:
        var io = FileIO.local()
        var meta_path = find_latest_metadata(io, self.table_dir)
        var metadata = TableMetadata.parse(_read_text(meta_path))
        var scan = TableScan(metadata^, io^).select(_wanted())
        return scan.to_batches(ScanOptions())

    def fields(self) raises -> List[FieldSpec]:
        var batches = self._scan()
        var out = List[FieldSpec]()
        if len(batches) == 0:
            return out^
        ref b = batches[0]
        for i in range(len(b.roots)):
            ref a = b.arena.nodes[b.roots[i]]
            out.append(FieldSpec(a.name, _dtype_of(a), a.nullable))
        return out^

    def total_records(self) raises -> Int:
        var n = 0
        var batches = self._scan()
        for i in range(len(batches)):
            n += batches[i].num_rows
        return n

    def batches(self) raises -> List[List[Column]]:
        var out = List[List[Column]]()
        var batches = self._scan()
        for bi in range(len(batches)):
            ref b = batches[bi]
            var cols = List[Column]()
            for i in range(len(b.roots)):
                cols.append(_to_column(b.arena.nodes[b.roots[i]]))
            out.append(cols^)
        return out^


def main() raises:
    var args = argv()
    if len(args) < 2:
        print("usage: serve_iceberg <table-dir>")
        return
    var table_dir = String(args[1])

    var src = IcebergSource(table_dir^)
    var n = src.total_records()
    print("serving", n, "rows from", src.table_dir, flush=True)

    var srv = HttpServer.bind(SocketAddr.localhost(8815))
    print("flight server on 127.0.0.1:8815", flush=True)
    srv.serve(GrpcStreamingService(FlightServer(src^)))

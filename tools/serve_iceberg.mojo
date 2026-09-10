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
    AT_TIMESTAMP,
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

from flight.ipc import (
    DT_BOOL,
    DT_FLOAT64,
    DT_INT64,
    DT_TIMESTAMP,
    DT_UTF8,
    FieldSpec,
)
from flight.server import FlightServer, FlightSource
from flight.writer import Column

def _read_text(path: String) raises -> String:
    with open(path, "r") as f:
        return f.read()


def _open(table_dir: String) raises -> TableScan:
    """A scan over the table's current metadata, with the columns projected.

    Unpinned: callers either pin a snapshot or are asking which one is
    current.
    """
    var io = FileIO.local()
    var meta_path = find_latest_metadata(io, table_dir)
    var metadata = TableMetadata.parse(_read_text(meta_path))
    return TableScan(metadata^, io^).select(_wanted())


comptime SPLIT_SIZE = 4 * 1024
"""Target bytes per endpoint.

Without a split the unit of work is the data file, so one large file is one
endpoint and one worker — the straggler that decides how long a fan-out takes.

4 KiB is absurd for real data and is set that way on purpose: the gate's table
is a few tens of kilobytes, and a production figure of 128 MiB would put every
row group in one run, so the file would not divide and a broken splitter would
pass unnoticed. Size this to the data, not to this number.
"""


def _encode_ticket(
    snapshot_id: Int64, path: String, start: Int64, length: Int64
) -> String:
    """A ticket names a snapshot, a file, *and* a byte range within it.

    Flight treats tickets as opaque bytes, so the format is ours. Carrying the
    range is what lets one file become several endpoints: two workers can hold
    tickets for the same path and still read disjoint row groups, because the
    range is what `to_batches_for_ranges` narrows on.

    Without it the ticket would name a whole file and splitting would be
    invisible to clients — the planner would divide the work and then hand out
    a unit that cannot express the division.
    """
    return (
        String(snapshot_id)
        + String("|")
        + String(start)
        + String("|")
        + String(length)
        + String("|")
        + path
    )


def _decode_ticket(ticket: String) raises -> Tuple[Int64, Int64, Int64, String]:
    """Split on the first three bars; the rest is the path.

    Only the first three, because a file path may itself contain one.
    """
    var bytes = ticket.as_bytes()
    var cuts = List[Int]()
    for i in range(len(bytes)):
        if bytes[i] == UInt8(124):  # "|"
            cuts.append(i)
            if len(cuts) == 3:
                break
    if len(cuts) < 3:
        raise Error(
            "flight: malformed ticket, expected"
            " '<snapshot>|<start>|<length>|<path>'"
        )
    var snap = Int64(atol(String(unsafe_from_utf8=bytes[: cuts[0]])))
    var start = Int64(
        atol(String(unsafe_from_utf8=bytes[cuts[0] + 1 : cuts[1]]))
    )
    var length = Int64(
        atol(String(unsafe_from_utf8=bytes[cuts[1] + 1 : cuts[2]]))
    )
    var path = String(unsafe_from_utf8=bytes[cuts[2] + 1 :])
    return (snap, start, length, path^)


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
    c.append(String("ts"))
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
    elif a.type.id == AT_TIMESTAMP:
        return DT_TIMESTAMP
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

    if dt == DT_INT64 or dt == DT_TIMESTAMP:
        var v = List[Int64]()
        var buf = Span(a.values)
        for i in range(a.length):
            v.append(load_i64(buf, i))
        col = Column.int64(v^) if dt == DT_INT64 else Column.timestamp(v^)
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
    """A `FlightSource` backed by an Iceberg table scan, pinned to a snapshot.

    **The snapshot is resolved once, at construction, and every scan uses it.**
    Without that, `GetFlightInfo` and a later `DoGet` plan independently, so a
    commit landing between them means a client fetching endpoints in parallel
    can assemble a table that never existed at any single point in time. Data
    files are immutable, so the symptom is not corruption — it is a mix of two
    snapshots, which is worse for being plausible.

    Pinning is what makes the invariant hold across *time* as well as across
    workers: the union of the endpoints is the table as of one snapshot.
    Iceberg's isolation does the work; this only has to ask for it.
    """

    var table_dir: String
    var snapshot_id: Int64
    """The snapshot every scan from this source reads. Chosen once."""

    def __init__(out self, var table_dir: String) raises:
        self.table_dir = table_dir^
        self.snapshot_id = _open(self.table_dir).snapshot().snapshot_id

    def _scan(self) raises -> TableScan:
        return (
            _open(self.table_dir)
            .use_snapshot(self.snapshot_id)
            .with_split_size(SPLIT_SIZE)
        )

    def tickets(self) raises -> List[String]:
        """One ticket per data file, straight off the scan plan.

        Iceberg has already decided where the work divides — pruning
        partitions, attaching delete files, computing a residual per task — so
        the planner's own split is the right one to hand out rather than a
        second opinion invented here.
        """
        var out = List[String]()
        for task in self._scan().plan_files():
            out.append(
                _encode_ticket(
                    self.snapshot_id,
                    task.data_file.file_path,
                    task.start,
                    task.length,
                )
            )
        return out^

    def fields(self) raises -> List[FieldSpec]:
        var batches = self._scan().to_batches(ScanOptions())
        var out = List[FieldSpec]()
        if len(batches) == 0:
            return out^
        ref b = batches[0]
        for i in range(len(b.roots)):
            ref a = b.arena.nodes[b.roots[i]]
            # unit and tz ride along from the scanned column: arrow-mlake's
            # TU_* are Arrow's TimeUnit values, so nothing is translated.
            out.append(
                FieldSpec(
                    a.name, _dtype_of(a), a.nullable, a.type.unit, a.type.tz
                )
            )
        return out^

    def total_records(self) raises -> Int:
        """Answered from the manifests where possible.

        `count` adds up `record_count` for every task whose rows all survive,
        so an unfiltered table is counted without opening a data file at all.
        """
        return Int(self._scan().count(ScanOptions()))

    def batches(self, ticket: String) raises -> List[List[Column]]:
        """The rows of the one data file this ticket names.

        The plan still applies to it — partition pruning, the residual, and
        any delete files attached to the task — so a worker returns exactly
        the rows a whole-table scan would have returned for that file. That is
        what makes the union of tickets equal the whole.
        """
        var decoded = _decode_ticket(ticket)
        var snapshot = decoded[0]
        var paths = List[String]()
        var starts = List[Int64]()
        starts.append(decoded[1])
        paths.append(decoded[3])

        # Plan at the ticket's snapshot, not at whatever is current. A ticket
        # issued before a commit still reads the table the client was told
        # about; `to_batches_for_paths` returns nothing for a file that
        # snapshot never had, rather than failing the query.
        var out = List[List[Column]]()
        var batches = (
            _open(self.table_dir)
            .use_snapshot(snapshot)
            .with_split_size(SPLIT_SIZE)
            .to_batches_for_splits(paths^, starts^, ScanOptions())
        )
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

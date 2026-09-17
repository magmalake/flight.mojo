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

## One binary, two roles

The same server is a worker or a coordinator depending on whether it is given
any `--worker` URIs. A worker serves `DoGet` for whatever ticket it is handed;
a coordinator plans the scan and points its endpoints at the workers. Nothing
about the code differs, because nothing about the *protocol* differs — which is
the argument for doing it this way rather than building a scheduler.

Run:
    pixi run serve-iceberg <table-dir>
    pixi run -e verify verify-iceberg <table-dir>

    # a three-process fan-out: two workers and a coordinator in front of them
    ./build/serve_ice <table-dir> --port 8816
    ./build/serve_ice <table-dir> --port 8817
    ./build/serve_ice <table-dir> --port 8815 \
        --worker grpc+tcp://127.0.0.1:8816 \
        --worker grpc+tcp://127.0.0.1:8817
"""

from std.memory import unsafe_memcpy
from std.sys import argv, size_of
from std.sys.info import num_logical_cores

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
from flare.http import ServerConfig
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


def _open(table_dir: String, columns: List[String]) raises -> TableScan:
    """A scan over the table's current metadata, with the columns projected.

    An empty `columns` selects everything, which is what a server told nothing
    about a table should do. A table with a column this IPC writer cannot
    encode — an `int32`, say — is then served by naming the ones it can.

    Unpinned: callers either pin a snapshot or are asking which one is
    current.
    """
    var io = FileIO.local()
    var meta_path = find_latest_metadata(io, table_dir)
    var metadata = TableMetadata.parse(_read_text(meta_path))
    var scan = TableScan(metadata^, io^)
    if len(columns) == 0:
        return scan^
    return scan.select(columns.copy())


comptime DEFAULT_SPLIT_SIZE = 4 * 1024
"""Target bytes per endpoint, unless `--split-size` says otherwise.

Without a split the unit of work is the data file, so one large file is one
endpoint and one worker — the straggler that decides how long a fan-out takes.

4 KiB is absurd for real data and is the default on purpose: the gate's table
is a few tens of kilobytes, and a production figure of 128 MiB would put every
row group in one run, so the file would not divide and a broken splitter would
pass unnoticed. Real tables pass `--split-size`.
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


def _fill_i64(mut out: List[Int64], buf: Span[UInt8, _], length: Int) raises:
    """`length` int64s out of an Arrow buffer, in one copy."""
    var n = length * size_of[Int64]()
    if n == 0:
        return
    if len(buf) < n:
        raise Error("flight: values buffer is shorter than the array")
    out.resize(unsafe_uninit_length=length)
    unsafe_memcpy(
        dest=out.unsafe_ptr().unsafe_bitcast[UInt8](),
        src=buf.unsafe_ptr(),
        count=n,
    )


def _fill_f64(mut out: List[Float64], buf: Span[UInt8, _], length: Int) raises:
    """`length` float64s out of an Arrow buffer, in one copy.

    Two functions rather than one generic: `resize(unsafe_uninit_length=)`
    wants a concrete element type to prove itself against, and there are
    exactly two fixed-width types this writer encodes.
    """
    var n = length * size_of[Float64]()
    if n == 0:
        return
    if len(buf) < n:
        raise Error("flight: values buffer is shorter than the array")
    out.resize(unsafe_uninit_length=length)
    unsafe_memcpy(
        dest=out.unsafe_ptr().unsafe_bitcast[UInt8](),
        src=buf.unsafe_ptr(),
        count=n,
    )


def _to_column(a: ArrayData) raises -> Column:
    """Copy one Arrow array into a `Column`.

    A copy, not a view: `Column` holds typed lists while `ArrayData` holds raw
    buffers. Passing the buffers straight through would avoid this and is the
    obvious optimisation once the shape is settled — the bytes are already in
    Arrow layout, which is the whole point of the format.

    Until then the copy is at least one copy: a fixed-width Arrow buffer and
    the `List` that receives it have the same little-endian layout, so this is
    a `memcpy` rather than a load and an append per value.
    """
    var dt = _dtype_of(a)
    var col: Column

    if dt == DT_INT64 or dt == DT_TIMESTAMP:
        var v = List[Int64]()
        _fill_i64(v, Span(a.values), a.length)
        col = Column.int64(v^) if dt == DT_INT64 else Column.timestamp(v^)
    elif dt == DT_FLOAT64:
        var v = List[Float64]()
        _fill_f64(v, Span(a.values), a.length)
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
    var columns: List[String]
    """What to project; empty means every column."""
    var split_size: Int
    """Target bytes per task. Planning and reading must agree on it, or a
    ticket names a task the re-plan does not have."""
    var snapshot_id: Int64
    """The snapshot every scan from this source reads. Chosen once."""
    var schema_fields: List[FieldSpec]
    """The Arrow schema, resolved once at construction.

    `fields()` is asked on every `GetFlightInfo` and again on every `DoGet` —
    a client needs the schema before it decides to read, and a stream opens
    with it. Deriving it from a scan each time cost a **scan each time**, and
    the snapshot is pinned anyway, so there is nothing to re-derive.
    """

    def __init__(
        out self,
        var table_dir: String,
        var columns: List[String],
        split_size: Int = DEFAULT_SPLIT_SIZE,
    ) raises:
        self.table_dir = table_dir^
        self.columns = columns^
        self.split_size = split_size
        self.snapshot_id = (
            _open(self.table_dir, self.columns).snapshot().snapshot_id
        )
        self.schema_fields = List[FieldSpec]()
        # One row, not a table: `ScanOptions.limit` stops the scan after the
        # first batch, and a batch carries the schema whatever its length.
        var options = ScanOptions()
        options.limit = 1
        var batches = (
            _open(self.table_dir, self.columns)
            .use_snapshot(self.snapshot_id)
            .with_split_size(self.split_size)
            .to_batches(options)
        )
        if len(batches) == 0:
            return
        ref b = batches[0]
        for i in range(len(b.roots)):
            ref a = b.arena.nodes[b.roots[i]]
            # unit and tz ride along from the scanned column: arrow-mlake's
            # TU_* are Arrow's TimeUnit values, so nothing is translated.
            self.schema_fields.append(
                FieldSpec(
                    a.name, _dtype_of(a), a.nullable, a.type.unit, a.type.tz
                )
            )

    def _scan(self) raises -> TableScan:
        return (
            _open(self.table_dir, self.columns)
            .use_snapshot(self.snapshot_id)
            .with_split_size(self.split_size)
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
        """The schema this source serves, resolved at construction."""
        return self.schema_fields.copy()

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
            _open(self.table_dir, self.columns)
            .use_snapshot(snapshot)
            .with_split_size(self.split_size)
            .to_batches_for_splits(paths^, starts^, ScanOptions())
        )
        for bi in range(len(batches)):
            ref b = batches[bi]
            var cols = List[Column]()
            for i in range(len(b.roots)):
                cols.append(_to_column(b.arena.nodes[b.roots[i]]))
            out.append(cols^)
        return out^


comptime USAGE = (
    "usage: serve_iceberg <table-dir> [--port N] [--worker URI]..."
    " [--columns a,b,c] [--split-size BYTES] [--workers N]"
)


def _split_commas(s: String) -> List[String]:
    var out = List[String]()
    var bytes = s.as_bytes()
    var start = 0
    for i in range(len(bytes)):
        if bytes[i] == UInt8(44):  # ","
            if i > start:
                out.append(String(unsafe_from_utf8=bytes[start:i]))
            start = i + 1
    if len(bytes) > start:
        out.append(String(unsafe_from_utf8=bytes[start:]))
    return out^


def main() raises:
    var args = argv()
    if len(args) < 2:
        print(USAGE)
        return

    var table_dir = String(args[1])
    var port = UInt16(8815)
    var workers = List[String]()
    var columns = List[String]()
    var split_size = DEFAULT_SPLIT_SIZE
    var num_workers = 0  # 0: one reactor per core

    var i = 2
    while i < len(args):
        var flag = String(args[i])
        if i + 1 >= len(args):
            print(USAGE)
            return
        if flag == String("--port"):
            port = UInt16(atol(String(args[i + 1])))
        elif flag == String("--workers"):
            num_workers = atol(String(args[i + 1]))
        elif flag == String("--split-size"):
            split_size = atol(String(args[i + 1]))
        elif flag == String("--columns"):
            columns = _split_commas(String(args[i + 1]))
        elif flag == String("--worker"):
            # Repeated, not comma-separated: a URI is a thing a shell can
            # quote wrongly once, and this way each one stands alone.
            workers.append(String(args[i + 1]))
        else:
            print(USAGE)
            return
        i += 2

    var src = IcebergSource(table_dir^, columns^, split_size)
    var n = src.total_records()
    print("serving", n, "rows from", src.table_dir, flush=True)
    if len(src.columns) > 0:
        print("  columns:", len(src.columns), "projected", flush=True)

    # A scan is not a web page. flare's defaults bound a request at
    # write_timeout_ms = 5 s, which a fan-out of million-row splits exceeds
    # while the client is still draining an earlier one, and the connection
    # is reaped mid-response.
    var cfg = ServerConfig()
    cfg.write_timeout_ms = 120_000
    cfg.idle_timeout_ms = 60_000
    var srv = HttpServer.bind(SocketAddr.localhost(port), cfg^)
    if len(workers) == 0:
        print("flight worker on 127.0.0.1:", port, sep="", flush=True)
    else:
        # Say it out loud in the log: a coordinator that hands out endpoints
        # nobody can reach looks identical to a working one until the client
        # tries to connect, and the URI it prints is the URI it advertises.
        print(
            "flight coordinator on 127.0.0.1:",
            port,
            ", endpoints over ",
            len(workers),
            " worker(s)",
            sep="",
            flush=True,
        )
        for w in workers:
            print("  worker:", w, flush=True)

    # One reactor per core by default. A single worker answers DoGets
    # strictly one at a time, so a client fanning out over N endpoints gets
    # no parallelism from the fan-out at all: 87 splits of the taxi table at
    # ~740 ms each is a minute of wall clock spent almost entirely waiting in
    # line. Each worker takes its own copy of the handler, and the source it
    # holds is a table path, a projection and a snapshot id -- values, not
    # shared state -- so a copy per worker costs nothing and shares nothing.
    var reactors = num_workers if num_workers > 0 else num_logical_cores()
    print("  reactors:", reactors, flush=True)
    srv.serve(GrpcStreamingService(FlightServer(src^, workers^)), reactors)

"""Turn columns into an Arrow IPC stream.

`ipc.mojo` encodes the metadata; this lays out the body it describes and
drives the framing. The split matters because the metadata has to state each
buffer's offset and length *before* the body exists, so the body is laid out
first and measured, then described.

## Buffer layout

Arrow fixes how many buffers each type contributes, and the order:

| type      | buffers                                  |
|-----------|------------------------------------------|
| int64     | validity, values (8 bytes each)          |
| float64   | validity, values (8 bytes each)          |
| bool      | validity, values (1 *bit* each)          |
| utf8      | validity, offsets (int32), data          |

Every buffer is padded to 8 bytes, because readers cast them in place.

A column carries its values in the list matching its type and leaves the rest
empty. A tagged union would be tidier, but this stays legible in a format
where a misplaced buffer produces silent garbage rather than an error.
"""

from std.memory import bitcast

from .ipc import (
    BufferSpec,
    DT_BOOL,
    DT_FLOAT64,
    DT_INT64,
    DT_UTF8,
    FieldSpec,
    build_record_batch_message,
    build_schema_message,
    frame_message,
    stream_end,
)


struct Column(Copyable, Movable):
    """One column: its type, its validity, and values in the matching list."""

    var dtype: Int
    var valid: List[Bool]
    """Per-row validity. Empty means "every row is valid"."""
    var i64: List[Int64]
    var f64: List[Float64]
    var b: List[Bool]
    var s: List[String]

    def __init__(out self, dtype: Int):
        self.dtype = dtype
        self.valid = List[Bool]()
        self.i64 = List[Int64]()
        self.f64 = List[Float64]()
        self.b = List[Bool]()
        self.s = List[String]()

    @staticmethod
    def int64(var values: List[Int64]) -> Column:
        var c = Column(DT_INT64)
        c.i64 = values^
        return c^

    @staticmethod
    def float64(var values: List[Float64]) -> Column:
        var c = Column(DT_FLOAT64)
        c.f64 = values^
        return c^

    @staticmethod
    def bool_(var values: List[Bool]) -> Column:
        var c = Column(DT_BOOL)
        c.b = values^
        return c^

    @staticmethod
    def utf8(var values: List[String]) -> Column:
        var c = Column(DT_UTF8)
        c.s = values^
        return c^

    def length(self) raises -> Int:
        if self.dtype == DT_INT64:
            return len(self.i64)
        elif self.dtype == DT_FLOAT64:
            return len(self.f64)
        elif self.dtype == DT_BOOL:
            return len(self.b)
        elif self.dtype == DT_UTF8:
            return len(self.s)
        raise Error("writer: unsupported dtype " + String(self.dtype))

    def null_count(self) -> Int:
        var n = 0
        for i in range(len(self.valid)):
            if not self.valid[i]:
                n += 1
        return n


def _pad_to8(mut body: List[UInt8]):
    while len(body) % 8 != 0:
        body.append(0)


def _push_bitmap(mut body: List[UInt8], bits: List[Bool], rows: Int):
    """Write `rows` bits, LSB first within each byte, padded to 8 bytes.

    An empty `bits` means "all set", which is how a column with no nulls gets
    an all-ones validity bitmap rather than an absent one.
    """
    var nbytes = (rows + 7) // 8
    for byte_i in range(nbytes):
        var acc: UInt8 = 0
        for bit in range(8):
            var row = byte_i * 8 + bit
            if row >= rows:
                break
            var set = True if len(bits) == 0 else bits[row]
            if set:
                acc |= UInt8(1 << bit)
        body.append(acc)
    _pad_to8(body)


def _push_i64(mut body: List[UInt8], v: Int64):
    var u = UInt64(v)
    for i in range(8):
        body.append(UInt8((u >> UInt64(i * 8)) & 0xFF))


def _push_i32(mut body: List[UInt8], v: Int):
    var u = UInt32(v)
    for i in range(4):
        body.append(UInt8((u >> UInt32(i * 8)) & 0xFF))


def _push_f64(mut body: List[UInt8], v: Float64):
    _push_i64(body, Int64(bitcast[DType.int64](v)))


def encode_batch(
    fields: List[FieldSpec], columns: List[Column]
) raises -> Tuple[List[UInt8], List[UInt8]]:
    """Lay out the body and encode the matching `RecordBatch` message.

    Returns `(metadata, body)`, unframed.
    """
    if len(fields) != len(columns):
        raise Error("writer: field/column count mismatch")
    if len(columns) == 0:
        raise Error("writer: a batch needs at least one column")

    var rows = columns[0].length()
    for i in range(len(columns)):
        if columns[i].length() != rows:
            raise Error("writer: columns have differing lengths")

    var body = List[UInt8]()
    var specs = List[BufferSpec]()
    var null_counts = List[Int]()

    for ci in range(len(columns)):
        var col = columns[ci].copy()
        null_counts.append(col.null_count())

        var start = len(body)
        _push_bitmap(body, col.valid, rows)
        specs.append(BufferSpec(start, len(body) - start))

        if col.dtype == DT_INT64:
            start = len(body)
            for i in range(rows):
                _push_i64(body, col.i64[i])
            _pad_to8(body)
            specs.append(BufferSpec(start, len(body) - start))
        elif col.dtype == DT_FLOAT64:
            start = len(body)
            for i in range(rows):
                _push_f64(body, col.f64[i])
            _pad_to8(body)
            specs.append(BufferSpec(start, len(body) - start))
        elif col.dtype == DT_BOOL:
            start = len(body)
            _push_bitmap(body, col.b, rows)
            specs.append(BufferSpec(start, len(body) - start))
        elif col.dtype == DT_UTF8:
            # offsets carry rows+1 entries: the trailing one is the total
            # byte length, which is how a reader sizes the last string.
            start = len(body)
            var running = 0
            _push_i32(body, 0)
            for i in range(rows):
                running += len(col.s[i].as_bytes())
                _push_i32(body, running)
            _pad_to8(body)
            specs.append(BufferSpec(start, len(body) - start))

            start = len(body)
            for i in range(rows):
                var sb = col.s[i].as_bytes()
                for k in range(len(sb)):
                    body.append(sb[k])
            _pad_to8(body)
            specs.append(BufferSpec(start, len(body) - start))
        else:
            raise Error("writer: unsupported dtype " + String(col.dtype))

    var meta = build_record_batch_message(rows, null_counts, specs, len(body))
    return (meta^, body^)


def write_stream(
    fields: List[FieldSpec], batches: List[List[Column]]
) raises -> List[UInt8]:
    """Encode a complete IPC stream: schema, each batch, end marker."""
    var out = List[UInt8]()

    var schema = build_schema_message(fields)
    var empty = List[UInt8]()
    var framed = frame_message(Span(schema), Span(empty))
    for i in range(len(framed)):
        out.append(framed[i])

    for bi in range(len(batches)):
        var pair = encode_batch(fields, batches[bi])
        var meta = pair[0].copy()
        var body = pair[1].copy()
        var f = frame_message(Span(meta), Span(body))
        for i in range(len(f)):
            out.append(f[i])

    var tail = stream_end()
    for i in range(len(tail)):
        out.append(tail[i])
    return out^

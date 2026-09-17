"""Arrow IPC stream writing: `Schema` and `RecordBatch` messages.

This is what Flight actually puts on the wire. `FlightData.data_header` is one
of these flatbuffer `Message`s and `data_body` is the buffers it describes, so
without an IPC writer a Flight server has nothing to send — the C Data
Interface that `arrow-mlake` provides hands pointers to another library *in the
same process*, which is a different problem.

## The framing

Each message in a stream is::

    ffffffff                  continuation marker
    <int32 metadata_size>     little-endian, and a multiple of 8
    <flatbuffer Message>      padded out to metadata_size
    <body>                    RecordBatch only; padded to a multiple of 8

and the stream ends with a continuation marker followed by a zero length. The
8-byte padding is not decoration: readers memory-map the body and cast buffers
in place, so every buffer must land on an 8-byte boundary.

## What is supported

`int64`, `float64`, `bool`, `utf8` and `timestamp` — the types
`arrow-mlake`'s `RecordBatch` carries. Each column is written as the two or three buffers Arrow specifies:
a validity bitmap, then values, plus offsets for `utf8`. Nulls are always
given a bitmap even when the column has none, because the alternative — a
length-zero buffer — is legal but has caught readers out.

Dictionaries, nested types and compression are not implemented, and a batch
using them is rejected rather than half-written.
"""


from std.memory import unsafe_memcpy
from .flatbuf import FlatBufferBuilder

# org.apache.arrow.flatbuf.MetadataVersion
comptime METADATA_V5: Int = 4

# org.apache.arrow.flatbuf.MessageHeader union tags
comptime HEADER_SCHEMA: UInt8 = 1
comptime HEADER_RECORD_BATCH: UInt8 = 3

# org.apache.arrow.flatbuf.Type union tags
comptime TYPE_INT: UInt8 = 2
comptime TYPE_FLOATING_POINT: UInt8 = 3
comptime TYPE_UTF8: UInt8 = 5
comptime TYPE_BOOL: UInt8 = 6
comptime TYPE_TIMESTAMP: UInt8 = 10

# org.apache.arrow.flatbuf.Precision
comptime PRECISION_DOUBLE: Int = 2

comptime ENDIANNESS_LITTLE: Int = 0

# The column types this writer understands.
comptime DT_INT64: Int = 0
comptime DT_FLOAT64: Int = 1
comptime DT_BOOL: Int = 2
comptime DT_UTF8: Int = 3
comptime DT_TIMESTAMP: Int = 4
"""Microsecond-or-whatever-unit epoch offsets, stored exactly like `int64`.

The unit and timezone live in the *schema*, not the values, which is why this
needs a `FieldSpec` that carries them while the other types do not.
"""

# Arrow TimeUnit, and identical to arrow-mlake's TU_* — so a unit read off a
# scanned column passes straight through without translation.
comptime TU_SECOND: Int = 0
comptime TU_MILLI: Int = 1
comptime TU_MICRO: Int = 2
comptime TU_NANO: Int = 3


@fieldwise_init
struct FieldSpec(Copyable, ImplicitlyCopyable, Movable):
    """One column's name, type and nullability, as the schema will state it.

    `unit` and `tz` are only read for `DT_TIMESTAMP`; every other type ignores
    them. They are fields rather than a separate timestamp-only spec because a
    schema is a list of columns, and a list wants one element type.
    """

    var name: String
    var dtype: Int
    var nullable: Bool
    var unit: Int
    """Arrow `TimeUnit`, read only for `DT_TIMESTAMP`."""
    var tz: String
    """IANA zone name, or empty for a timestamp without one. Iceberg's
    `timestamptz` is UTC and its `timestamp` has no zone; the difference is
    carried here rather than by shifting the values."""

    @staticmethod
    def simple(name: String, dtype: Int, nullable: Bool) -> FieldSpec:
        """A column whose type needs no unit or timezone.

        Everything except `DT_TIMESTAMP`. The unit is filled with microseconds
        so the field is never uninitialised, and is ignored for these types.
        """
        return FieldSpec(name, dtype, nullable, TU_MICRO, String(""))


def _pad8(n: Int) -> Int:
    """Round up to the next multiple of 8."""
    return (n + 7) & ~7


def _append(mut out: List[UInt8], src: Span[UInt8, _]):
    """Append `src` whole.

    On the framing path `src` is an entire record-batch body, so a byte at a
    time is a scalar pass over everything the server is about to send — it
    cost more than reading the rows did.
    """
    var n = len(src)
    if n == 0:
        return
    var base = len(out)
    out.resize(unsafe_uninit_length=base + n)
    unsafe_memcpy(
        dest=out.unsafe_ptr().unsafe_offset(base), src=src.unsafe_ptr(), count=n
    )


def _append_i32_le(mut out: List[UInt8], v: Int):
    var u = UInt32(v)
    for i in range(4):
        out.append(UInt8((u >> UInt32(i * 8)) & 0xFF))


def _pad_to8(mut out: List[UInt8]):
    while len(out) % 8 != 0:
        out.append(0)


# ── schema ───────────────────────────────────────────────────────────────


def _build_type(
    mut b: FlatBufferBuilder, dtype: Int, unit: Int, tz: String
) raises -> Int:
    """Write the type table for one column and return its offset."""
    if dtype == DT_TIMESTAMP:
        # Timestamp { unit: TimeUnit, timezone: string }. The string has to be
        # written before the table that points at it — offsets only ever point
        # backwards — and an empty zone is left absent rather than written as
        # "", because absent is how Arrow spells "no timezone" and an empty
        # string would be a zone whose name happens to be blank.
        var tz_off = 0
        if len(tz.as_bytes()) > 0:
            tz_off = b.create_string(tz)
        b.start_table(2)
        b.add_i16(0, unit, TU_SECOND)
        b.add_offset(1, tz_off)
        return b.end_table()
    if dtype == DT_INT64:
        b.start_table(2)  # Int { bitWidth, is_signed }
        b.add_i32(0, 64)
        b.add_bool(1, True)
        return b.end_table()
    elif dtype == DT_FLOAT64:
        b.start_table(1)  # FloatingPoint { precision }
        b.add_i16(0, PRECISION_DOUBLE)
        return b.end_table()
    elif dtype == DT_UTF8:
        b.start_table(0)  # Utf8 {}
        return b.end_table()
    elif dtype == DT_BOOL:
        b.start_table(0)  # Bool {}
        return b.end_table()
    raise Error("ipc: unsupported dtype " + String(dtype))


def _type_tag(dtype: Int) raises -> UInt8:
    if dtype == DT_INT64:
        return TYPE_INT
    elif dtype == DT_FLOAT64:
        return TYPE_FLOATING_POINT
    elif dtype == DT_UTF8:
        return TYPE_UTF8
    elif dtype == DT_BOOL:
        return TYPE_BOOL
    elif dtype == DT_TIMESTAMP:
        return TYPE_TIMESTAMP
    raise Error("ipc: unsupported dtype " + String(dtype))


def build_schema_message(fields: List[FieldSpec]) raises -> List[UInt8]:
    """Encode a `Message` wrapping a `Schema`, without stream framing."""
    var b = FlatBufferBuilder()

    # Children first: every offset must already exist before the table that
    # refers to it, because offsets only ever point backwards.
    var field_offsets = List[Int]()
    for i in range(len(fields)):
        var f = fields[i]
        var name_off = b.create_string(f.name)
        var type_off = _build_type(b, f.dtype, f.unit, f.tz)
        # Field { name, nullable, type_type, type, dictionary, children, ... }
        b.start_table(7)
        b.add_offset(0, name_off)
        b.add_bool(1, f.nullable)
        b.add_u8(2, _type_tag(f.dtype))
        b.add_offset(3, type_off)
        field_offsets.append(b.end_table())

    var fields_vec = b.create_offset_vector(field_offsets)

    # Schema { endianness, fields, custom_metadata, features }
    b.start_table(4)
    b.add_i16(0, ENDIANNESS_LITTLE)
    b.add_offset(1, fields_vec)
    var schema_off = b.end_table()

    # Message { version, header_type, header, bodyLength, custom_metadata }
    b.start_table(5)
    b.add_i16(0, METADATA_V5)
    b.add_u8(1, HEADER_SCHEMA)
    b.add_offset(2, schema_off)
    b.add_i64(3, 0)
    var msg = b.end_table()

    return b.finish(msg)


# ── record batch ─────────────────────────────────────────────────────────


@fieldwise_init
struct BufferSpec(Copyable, ImplicitlyCopyable, Movable):
    """Where one buffer sits in the message body."""

    var offset: Int
    var length: Int


def build_record_batch_message(
    length: Int,
    null_counts: List[Int],
    buffers: List[BufferSpec],
    body_length: Int,
) raises -> List[UInt8]:
    """Encode a `Message` wrapping a `RecordBatch`.

    `nodes` and `buffers` are vectors of *structs*, which FlatBuffers stores
    inline rather than by reference — so they are written directly into the
    vector, back to front, with no child tables.
    """
    var b = FlatBufferBuilder()

    # buffers: [Buffer { offset: long, length: long }]
    b.start_vector(16, len(buffers), 8)
    for i in range(len(buffers) - 1, -1, -1):
        b.prepend_i64(buffers[i].length)
        b.prepend_i64(buffers[i].offset)
    var buffers_vec = b.end_vector()

    # nodes: [FieldNode { length: long, null_count: long }]
    b.start_vector(16, len(null_counts), 8)
    for i in range(len(null_counts) - 1, -1, -1):
        b.prepend_i64(null_counts[i])
        b.prepend_i64(length)
    var nodes_vec = b.end_vector()

    # RecordBatch { length, nodes, buffers, compression, variadic }
    b.start_table(5)
    b.add_i64(0, length)
    b.add_offset(1, nodes_vec)
    b.add_offset(2, buffers_vec)
    var rb_off = b.end_table()

    b.start_table(5)
    b.add_i16(0, METADATA_V5)
    b.add_u8(1, HEADER_RECORD_BATCH)
    b.add_offset(2, rb_off)
    b.add_i64(3, body_length)
    var msg = b.end_table()

    return b.finish(msg)


# ── stream framing ───────────────────────────────────────────────────────


def frame_message(
    metadata: Span[UInt8, _], body: Span[UInt8, _]
) raises -> List[UInt8]:
    """Wrap one encoded message in the stream framing.

    The metadata length written is the *padded* length, so that the body — and
    therefore every buffer in it — starts 8-byte aligned.
    """
    var out = List[UInt8]()
    var padded = _pad8(len(metadata))

    for _ in range(4):
        out.append(0xFF)  # continuation marker
    _append_i32_le(out, padded)
    _append(out, metadata)
    while len(out) % 8 != 0:
        out.append(0)
    if len(body) > 0:
        _append(out, body)
        _pad_to8(out)
    return out^


def stream_end() -> List[UInt8]:
    """The end-of-stream marker: continuation followed by a zero length."""
    var out = List[UInt8]()
    for _ in range(4):
        out.append(0xFF)
    _append_i32_le(out, 0)
    return out^

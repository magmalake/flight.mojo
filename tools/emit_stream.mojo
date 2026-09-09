"""Emit an Arrow IPC stream to a file, for pyarrow to read back.

The verifier is pyarrow rather than our own reader on purpose: a writer
checked only against a reader we also wrote proves the two agree and nothing
about whether either matches Arrow.
"""

from flight.ipc import DT_BOOL, DT_FLOAT64, DT_INT64, DT_UTF8, FieldSpec
from flight.writer import Column, write_stream


def main() raises:
    var fields = List[FieldSpec]()
    fields.append(FieldSpec("id", DT_INT64, False))
    fields.append(FieldSpec("amount", DT_FLOAT64, False))
    fields.append(FieldSpec("flag", DT_BOOL, False))
    fields.append(FieldSpec("name", DT_UTF8, False))

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

    var batches = List[List[Column]]()
    batches.append(cols^)

    var stream = write_stream(fields, batches)

    with open("build/stream.arrows", "w") as f:
        f.write(String(unsafe_from_utf8=Span(stream)))
    print("wrote", len(stream), "bytes")

"""Read the Mojo-written IPC stream with pyarrow.

The gate is that a library we did not write accepts the bytes: a writer
checked only against our own reader would prove the two agree and nothing
about whether either matches Arrow.
"""
import sys
import pyarrow as pa

with open("build/stream.arrows", "rb") as f:
    raw = f.read()

reader = pa.ipc.open_stream(pa.BufferReader(raw))
table = reader.read_all()

print("schema:")
print(table.schema)
print(f"rows: {table.num_rows}  cols: {table.num_columns}")
print(table.to_pydict())

expected = {
    "id": [100, 101, 102, 103, 104],
    "amount": [0.0, 1.5, 3.0, 4.5, 6.0],
    "flag": [True, False, True, False, True],
    "name": ["row-0", "row-1", "row-2", "row-3", "row-4"],
}
got = table.to_pydict()
ok = True
for k, v in expected.items():
    if got.get(k) != v:
        print(f"MISMATCH {k}: expected {v}, got {got.get(k)}")
        ok = False
print("ROUND TRIP OK" if ok else "ROUND TRIP FAILED")
sys.exit(0 if ok else 1)

"""Read an Iceberg table through the Mojo Flight server, with pyarrow.

Two things this checks that a shape assertion would not:

**Nulls.** A validity bitmap that is dropped or misaligned produces plausible
numbers rather than an error, so `amount` keeping its two Nones is the point of
the test rather than a detail of it.

**Timestamps.** `ts` has to arrive as `timestamp[us]`, not as the int64 it is
on the wire. Comparing only values would pass while losing what they mean.

Rows are compared **keyed by id**, because neither Iceberg nor Flight promises
an order: the scan visits data files in its own sequence, and pinning the order
one run happens to produce would fail the first time planning changes without
anything being wrong.
"""

import sys
from datetime import datetime

import pyarrow as pa
import pyarrow.flight as fl

client = fl.connect("grpc://127.0.0.1:8815")
info = client.get_flight_info(fl.FlightDescriptor.for_path("t"))
print(f"GetFlightInfo: {info.total_records} records, {len(info.endpoints)} endpoint(s)")
print("schema:", str(info.schema).replace("\n", " | "))

table = client.do_get(info.endpoints[0].ticket).read_all()
print(f"DoGet: {table.num_rows} rows x {table.num_columns} cols")

# The fixture, keyed by id. Confirmed by reading its Parquet files directly
# with pyarrow — not transcribed from this server's own output, which would
# only prove it agrees with itself.
expected = {
    1: ("eu", 1.5, True, datetime(2023, 11, 14, 0, 0)),
    2: ("us", None, False, datetime(2023, 11, 15, 0, 0)),
    3: ("eu", 3.5, True, datetime(2023, 11, 16, 12, 0)),
    4: ("us", 4.5, False, datetime(2023, 11, 17, 0, 0)),
    5: ("apac", 5.5, True, datetime(2023, 12, 1, 0, 0)),
    6: ("eu", 6.5, True, datetime(2024, 1, 1, 0, 0)),
    7: ("apac", None, False, datetime(2023, 11, 14, 0, 0)),
}

got = table.to_pydict()
rows = {
    got["id"][i]: (
        got["region"][i],
        got["amount"][i],
        got["ok"][i],
        got["ts"][i],
    )
    for i in range(table.num_rows)
}
print("rows by id:", dict(sorted(rows.items())))

ok = True

if table.num_rows != info.total_records:
    print(f"MISMATCH: GetFlightInfo said {info.total_records}, DoGet gave {table.num_rows}")
    ok = False

if set(rows) != set(expected):
    print(f"MISMATCH ids: expected {sorted(expected)}, got {sorted(rows)}")
    ok = False

for key in sorted(set(rows) & set(expected)):
    if rows[key] != expected[key]:
        print(f"MISMATCH id={key}:\n  expected {expected[key]}\n  got      {rows[key]}")
        ok = False

ts_type = table.schema.field("ts").type
if ts_type != pa.timestamp("us"):
    print(f"MISMATCH ts type: expected timestamp[us], got {ts_type}")
    ok = False

# The schema advertised up front must match the one the stream carries, or a
# client that builds its reader from GetFlightInfo gets a surprise.
if info.schema != table.schema:
    print(f"MISMATCH schema:\n  info   {info.schema}\n  stream {table.schema}")
    ok = False

print("ICEBERG FLIGHT OK" if ok else "ICEBERG FLIGHT FAILED")
sys.exit(0 if ok else 1)

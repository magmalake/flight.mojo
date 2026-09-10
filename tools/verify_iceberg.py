"""Read an Iceberg table through the Mojo Flight server, with pyarrow.

Two things this checks that a shape assertion would not:

**Nulls.** A validity bitmap that is dropped or misaligned produces plausible
numbers rather than an error, so `amount` keeping its two Nones is the point of
the test rather than a detail of it.

**Timestamps.** `ts` has to arrive as `timestamp[us]`, not as the int64 it is
on the wire. Comparing only values would pass while losing what they mean.

**The partition.** `GetFlightInfo` advertises one endpoint per Iceberg data
file, and the union of them must be the whole table — nothing repeated,
nothing lost. That is what entitles a client to fetch the endpoints in
parallel, so the test fetches all of them and concatenates rather than reading
the first.

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

# Fetch every endpoint and union them. This is the property that makes the
# split worth having: each ticket names one data file, and the union has to be
# the whole table with nothing repeated and nothing lost. A client is entitled
# to fetch them in parallel and concatenate, so if that does not hold, a
# correct client gets a wrong answer.
parts = []
for i, ep in enumerate(info.endpoints):
    part = client.do_get(ep.ticket).read_all()
    name = ep.ticket.ticket.decode().rsplit("/", 1)[-1]
    print(f"  endpoint {i}: {part.num_rows} rows <- {name}")
    parts.append(part)

table = pa.concat_tables(parts)
print(f"union: {table.num_rows} rows x {table.num_columns} cols")

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

# The plan really was split: one endpoint would still pass every value check
# below while proving nothing about the partitioning.
if len(info.endpoints) < 2:
    print(f"MISMATCH: expected the scan plan to split, got {len(info.endpoints)} endpoint(s)")
    ok = False

# No row may come back from two tickets. Comparing lengths catches a
# duplicate that the keyed comparison below would silently collapse.
ids = table.column("id").to_pylist()
if len(ids) != len(set(ids)):
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    print(f"MISMATCH: ids returned by more than one endpoint: {dupes}")
    ok = False

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

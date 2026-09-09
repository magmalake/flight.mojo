"""Read an Iceberg table through the Mojo Flight server, with pyarrow.

Asserts on shape and on the values the fixture is known to hold, including
nulls: a validity bitmap that is dropped or misaligned shows up as plausible
numbers rather than an error, so checking that `amount` still has its two
Nones is the point of the test rather than a detail of it.
"""
import sys
import pyarrow.flight as fl

client = fl.connect("grpc://127.0.0.1:8815")
info = client.get_flight_info(fl.FlightDescriptor.for_path("t"))
print(f"GetFlightInfo: {info.total_records} records, {len(info.endpoints)} endpoint(s)")
print("schema:", str(info.schema).replace("\n", " | "))

table = client.do_get(info.endpoints[0].ticket).read_all()
got = table.to_pydict()
print(f"DoGet: {table.num_rows} rows x {table.num_columns} cols")
print(got)

expected = {
    "id": [1, 3, 2, 4, 5, 6, 7],
    "region": ["eu", "eu", "us", "us", "apac", "eu", "apac"],
    "amount": [1.5, 3.5, None, 4.5, 5.5, 6.5, None],
    "ok": [True, True, False, False, True, True, False],
}

ok = True
if table.num_rows != info.total_records:
    print(f"MISMATCH: GetFlightInfo said {info.total_records}, DoGet gave {table.num_rows}")
    ok = False
for k, v in expected.items():
    if got.get(k) != v:
        print(f"MISMATCH {k}:\n  expected {v}\n  got      {got.get(k)}")
        ok = False

# The schema advertised up front must match the one the stream carries,
# or a client that builds its reader from GetFlightInfo gets a surprise.
if info.schema != table.schema:
    print(f"MISMATCH schema:\n  info   {info.schema}\n  stream {table.schema}")
    ok = False

print("ICEBERG FLIGHT OK" if ok else "ICEBERG FLIGHT FAILED")
sys.exit(0 if ok else 1)

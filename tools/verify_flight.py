"""Talk to the Mojo Flight server with a stock pyarrow.flight client.

Nothing here knows the server is written in Mojo: if this passes, the wire
protocol is real Flight rather than something only our own client accepts.
"""
import sys
import pyarrow.flight as fl

client = fl.connect("grpc://127.0.0.1:8815")

desc = fl.FlightDescriptor.for_path("demo")
info = client.get_flight_info(desc)
print("GetFlightInfo:")
print(f"  total_records: {info.total_records}")
print(f"  endpoints: {len(info.endpoints)}")
print(f"  schema: {info.schema}")

ticket = info.endpoints[0].ticket
reader = client.do_get(ticket)
table = reader.read_all()
print(f"DoGet: {table.num_rows} rows x {table.num_columns} cols")
print(table.to_pydict())

expected = {
    "id": [100, 101, 102, 103, 104],
    "amount": [0.0, 1.5, 3.0, 4.5, 6.0],
    "flag": [True, False, True, False, True],
    "name": ["row-0", "row-1", "row-2", "row-3", "row-4"],
}
got = table.to_pydict()
ok = got == expected
if not ok:
    print(f"MISMATCH\n expected {expected}\n got      {got}")
print("FLIGHT INTEROP OK" if ok else "FLIGHT INTEROP FAILED")
sys.exit(0 if ok else 1)

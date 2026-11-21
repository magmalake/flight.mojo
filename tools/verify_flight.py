"""Talk to the Mojo Flight server with a stock pyarrow.flight client.

Nothing here knows the server is written in Mojo: if this passes, the wire
protocol is real Flight rather than something only our own client accepts.
"""
import sys

import pyarrow as pa
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

# A bad ticket is the client's mistake and must be answered as one, never as
# INTERNAL. pyarrow raises ArrowKeyError for NOT_FOUND and ArrowInvalid for
# INVALID_ARGUMENT; INTERNAL would be FlightInternalError.
for ticket, want, status in (
    (b"nope", pa.lib.ArrowKeyError, "NOT_FOUND"),  # not one the server issued
    (b"\xff\xfe", pa.lib.ArrowInvalid, "INVALID_ARGUMENT"),  # not UTF-8
):
    try:
        client.do_get(fl.Ticket(ticket)).read_all()
        print(f"MISMATCH ticket {ticket!r}: answered, expected {status}")
        ok = False
    except want as e:
        print(f"ticket {ticket!r}: {status} ({str(e).split('. gRPC')[0]})")
    except Exception as e:  # noqa: BLE001
        print(f"MISMATCH ticket {ticket!r}: {type(e).__name__}, expected {status}: {e}")
        ok = False
print("FLIGHT INTEROP OK" if ok else "FLIGHT INTEROP FAILED")
sys.exit(0 if ok else 1)

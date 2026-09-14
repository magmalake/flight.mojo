"""Read one Iceberg table from two worker processes, with pyarrow.

`verify_iceberg.py` proves the table divides. This proves the division can be
*placed*: the coordinator answers `GetFlightInfo` with endpoints whose
`Location` names a different process, and the client goes there for the data.

The client is what makes this a real test rather than a shape assertion. It
opens its own connection to each location, so every row counted below came off
a socket to that worker — the coordinator could not have served them, because
nobody asked it to.

Four properties, each of which can fail independently:

**Placement.** Every endpoint carries a location, they are the two workers, and
neither is the coordinator. A server that quietly dropped the field would
advertise "fetch from me" and still return correct rows.

**Spread.** The endpoints are dealt evenly. One worker holding all of them is a
working fan-out with no fan, and no value check would notice.

**Stability.** Two `GetFlightInfo` calls agree on ticket and location, which is
what lets a client plan once and fetch later.

**Interchangeability.** A ticket issued for worker A reads identically from
worker B. That is the assumption round-robin placement rests on — the workers
share an object store, so a location is a routing hint and not an ownership
claim — and if it were false, the placement policy would have to change rather
than the client.
"""

import sys

import pyarrow as pa
import pyarrow.flight as fl

COORDINATOR = "grpc+tcp://127.0.0.1:8815"
WORKERS = ["grpc+tcp://127.0.0.1:8816", "grpc+tcp://127.0.0.1:8817"]


def uri_of(location):
    """pyarrow hands back `uri` as bytes on some versions, str on others."""
    uri = location.uri
    return uri.decode() if isinstance(uri, bytes) else str(uri)


def plan(client):
    """The endpoints as (ticket bytes, [location uri]) — comparable values."""
    info = client.get_flight_info(fl.FlightDescriptor.for_path("t"))
    return info, [
        (ep.ticket.ticket, [uri_of(loc) for loc in ep.locations])
        for ep in info.endpoints
    ]


ok = True
coordinator = fl.connect(COORDINATOR)
info, endpoints = plan(coordinator)
print(
    f"GetFlightInfo: {info.total_records} records, {len(endpoints)} endpoint(s)"
)

# --- placement ---------------------------------------------------------------
unplaced = [i for i, (_, locs) in enumerate(endpoints) if not locs]
if unplaced:
    print(f"MISMATCH: endpoints with no Location: {unplaced}")
    ok = False

advertised = {loc for _, locs in endpoints for loc in locs}
print("locations:", sorted(advertised))
if advertised != set(WORKERS):
    print(f"MISMATCH locations: expected {sorted(WORKERS)}, got {sorted(advertised)}")
    ok = False
if COORDINATOR in advertised:
    print("MISMATCH: the coordinator advertised itself; no work was handed out")
    ok = False

# --- spread ------------------------------------------------------------------
by_location = {}
for ticket, locs in endpoints:
    by_location.setdefault(locs[0] if locs else COORDINATOR, []).append(ticket)
for loc in sorted(by_location):
    print(f"  {loc}: {len(by_location[loc])} endpoint(s)")

counts = [len(v) for v in by_location.values()]
if len(by_location) < 2:
    print(f"MISMATCH: every endpoint went to one location ({sorted(by_location)})")
    ok = False
elif max(counts) - min(counts) > 1:
    print(f"MISMATCH: endpoints dealt unevenly: {sorted(counts)}")
    ok = False

# --- stability ---------------------------------------------------------------
# Same question, same answer: a client is entitled to plan now and fetch later,
# and a coordinator that reshuffled would hand two clients overlapping work.
_, again = plan(coordinator)
if again != endpoints:
    print("MISMATCH: a second GetFlightInfo returned a different plan")
    ok = False

# --- the rows ----------------------------------------------------------------
# One connection per worker, and each fetches only the tickets it was given.
parts = []
for loc in sorted(by_location):
    with fl.connect(loc) as worker:
        rows = 0
        for ticket in by_location[loc]:
            part = worker.do_get(fl.Ticket(ticket)).read_all()
            rows += part.num_rows
            parts.append(part)
        print(f"  {loc} served {rows} rows")

table = pa.concat_tables(parts)
print(f"union: {table.num_rows} rows x {table.num_columns} cols")

if table.num_rows != info.total_records:
    print(
        f"MISMATCH: GetFlightInfo said {info.total_records},"
        f" the workers gave {table.num_rows}"
    )
    ok = False

ids = table.column("id").to_pylist()
if len(ids) != len(set(ids)):
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    print(f"MISMATCH: ids returned by more than one worker: {dupes}")
    ok = False

# --- interchangeability ------------------------------------------------------
first_ticket, first_locs = endpoints[0]
# Skipped rather than crashed when placement already failed: a traceback here
# would bury the mismatches printed above, which are the actual diagnosis.
if not first_locs:
    print("SKIPPED interchangeability: endpoint 0 has no location to compare")
else:
    elsewhere = [w for w in WORKERS if w != first_locs[0]][0]
    with fl.connect(first_locs[0]) as a, fl.connect(elsewhere) as b:
        here = a.do_get(fl.Ticket(first_ticket)).read_all()
        there = b.do_get(fl.Ticket(first_ticket)).read_all()
    if here != there:
        print(
            f"MISMATCH: ticket read from {first_locs[0]} ({here.num_rows} rows)"
            f" differs from {elsewhere} ({there.num_rows} rows)"
        )
        ok = False
    else:
        print(f"ticket 0 reads the same from both workers: {here.num_rows} rows")

print("CLUSTER FLIGHT OK" if ok else "CLUSTER FLIGHT FAILED")
sys.exit(0 if ok else 1)

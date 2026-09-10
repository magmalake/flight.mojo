"""Write a small Iceberg table with PyIceberg, for the Flight gate to serve.

Generated rather than checked in, for two reasons.

**Portability.** Iceberg metadata records absolute paths — in `location`, in
`metadata-log`, and inside the Avro manifests. A checked-in fixture therefore
only resolves on the machine that made it, which is how the CI failure that
prompted this looked: green locally, "Failed to open file '/Users/…'" on every
runner.

**Provenance.** The table is written by PyIceberg, so the Flight server is read
against a table produced by the reference implementation rather than by us. A
fixture we wrote and then read back would prove the two agree and nothing more.

Deterministic: same rows, same order, every run.
"""

import os
import shutil
import sys
from datetime import datetime

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

warehouse = sys.argv[1]
shutil.rmtree(warehouse, ignore_errors=True)
# Before the catalog: SqlCatalog opens its sqlite file inside the warehouse,
# and sqlite will not create the directory for it.
os.makedirs(warehouse, exist_ok=True)

catalog = SqlCatalog(
    "gate",
    **{
        "uri": f"sqlite:///{warehouse}/catalog.db",
        "warehouse": f"file://{warehouse}",
    },
)

# The columns the IPC writer covers: int64, utf8, double, bool, timestamp.
# `amount` carries nulls on purpose — a dropped or misaligned validity bitmap
# produces plausible numbers rather than an error, so it is the value worth
# asserting on.
schema = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("region", pa.string(), nullable=False),
        pa.field("amount", pa.float64(), nullable=True),
        pa.field("ok", pa.bool_(), nullable=False),
        pa.field("ts", pa.timestamp("us"), nullable=True),
    ]
)
table_data = pa.table(
    {
        "id": [1, 2, 3, 4, 5, 6, 7],
        "region": ["eu", "us", "eu", "us", "apac", "eu", "apac"],
        "amount": [1.5, None, 3.5, 4.5, 5.5, 6.5, None],
        "ok": [True, False, True, False, True, True, False],
        "ts": [
            datetime(2023, 11, 14),
            datetime(2023, 11, 15),
            datetime(2023, 11, 16, 12, 0),
            datetime(2023, 11, 17),
            datetime(2023, 12, 1),
            datetime(2024, 1, 1),
            datetime(2023, 11, 14),
        ],
    },
    schema=schema,
)

catalog.create_namespace_if_not_exists("db")
# Uncompressed Parquet. PyIceberg defaults to zstd, and every codec in
# parquet.mojo is a dlopened shim that lives in its own tin's environment —
# dragging four of them in would test the codecs, which this gate is not for.
table = catalog.create_table(
    "db.flightgate",
    schema=schema,
    properties={"write.parquet.compression-codec": "uncompressed"},
)

# Two appends, so the scan plans more than one data file and the Flight server
# has a real split to advertise. One file would let a broken partition pass.
table.append(table_data.slice(0, 4))
table.append(table_data.slice(4, 3))

# Print the table directory, not the metadata file: the caller wants somewhere
# to point a scan at, and deriving it here keeps the layout PyIceberg chose
# from being duplicated (and drifting) in the shell script.
print(os.path.dirname(os.path.dirname(table.metadata_location.removeprefix("file://"))))

"""Print the absolute path an Iceberg fixture's metadata says it lives at.

The fixtures under iceberg.mojo record absolute paths — in `location`, in
`metadata-log`, and inside the Avro manifests. Their PROVENANCE.md says a
consumer must either rewrite that prefix or put the warehouse where the
metadata points. Rewriting would mean editing Avro, so callers place it, and
this tells them where. Deriving it from the fixture beats hardcoding a home
directory that only exists on one machine.
"""
import glob
import json
import os
import sys

table = sys.argv[1]
newest = sorted(glob.glob(os.path.join(table, "metadata", "*.metadata.json")))[-1]
location = json.load(open(newest))["location"]
print(location.removeprefix("file://"))

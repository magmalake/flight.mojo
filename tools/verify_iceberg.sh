#!/usr/bin/env bash
# Serve an Iceberg fixture over Flight and read it back with pyarrow.
#
# CONDA_PREFIX is set for the child because flare resolves its FFI shims
# through $CONDA_PREFIX/lib before falling back to ./build; unset, the missing
# zlib shim surfaces as a torn-down connection rather than a clear error.
set -euo pipefail

TABLE="${1:-../iceberg.mojo/tests/fixtures/ident_part}"
: "${CONDA_PREFIX:?run this through pixi so the FFI shims resolve}"

./build/serve_ice "$TABLE" > build/iceberg-server.log 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null || true' EXIT
sleep 5
python tools/verify_iceberg.py

#!/usr/bin/env bash
# Start the demo server, point a stock pyarrow.flight client at it, tear down.
#
# CONDA_PREFIX is set explicitly because flare resolves its FFI shims
# (libflare_tls, libflare_zlib) via $CONDA_PREFIX/lib first and falls back to
# ./build. Running the binary outside `pixi run` leaves it unset, and the
# missing zlib shim shows up as a torn-down connection rather than a clear
# error: grpc clients advertise gzip, flare picks it, and the encode fails.
set -euo pipefail

: "${CONDA_PREFIX:?run this through pixi so the FFI shims resolve}"

./build/serve > build/server.log 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null || true' EXIT
sleep 4
python tools/verify_flight.py

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
LOG="build/server.log"
trap 'kill $SRV 2>/dev/null || true' EXIT
# Wait for the port rather than sleeping a guessed number of seconds: the
# server plans the scan before it binds, so how long that takes depends on the
# table and the machine. A fixed sleep passes locally and fails on a slower
# runner, which is the least useful way for a gate to fail.
wait_for_port() {
  for _ in $(seq 1 60); do
    if command -v nc >/dev/null 2>&1; then
      nc -z 127.0.0.1 8815 2>/dev/null && return 0
    else
      (exec 3<>/dev/tcp/127.0.0.1/8815) 2>/dev/null && return 0
    fi
    kill -0 "$SRV" 2>/dev/null || break   # died: stop waiting, show the log
    sleep 1
  done
  echo "--- server did not come up; its log:" >&2
  cat "$LOG" >&2
  return 1
}

wait_for_port
python tools/verify_flight.py || { echo "--- server log:" >&2; cat "$LOG" >&2; exit 1; }

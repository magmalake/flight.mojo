#!/usr/bin/env bash
# Serve an Iceberg fixture over Flight and read it back with pyarrow.
#
# CONDA_PREFIX is set for the child because flare resolves its FFI shims
# through $CONDA_PREFIX/lib before falling back to ./build; unset, the missing
# zlib shim surfaces as a torn-down connection rather than a clear error.
set -euo pipefail

# The table is generated rather than checked in: Iceberg metadata records
# absolute paths, so a checked-in fixture resolves only on the machine that
# made it. See tools/make_fixture.py -- it is also written by PyIceberg, which
# makes this a cross-implementation read rather than us checking our own work.
WAREHOUSE="$PWD/build/warehouse"
TABLE="$(python tools/make_fixture.py "$WAREHOUSE" | tail -1)"
: "${CONDA_PREFIX:?run this through pixi so the FFI shims resolve}"

./build/serve_ice "$TABLE" > build/iceberg-server.log 2>&1 &
SRV=$!
LOG="build/iceberg-server.log"
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
python tools/verify_iceberg.py || { echo "--- server log:" >&2; cat "$LOG" >&2; exit 1; }

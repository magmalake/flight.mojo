#!/usr/bin/env bash
# Three processes: two workers and a coordinator, over one Iceberg table.
#
# The point is that nothing here is a simulation. The coordinator plans the
# scan and advertises endpoints located on the workers; the client connects to
# those workers itself and the rows never pass through the coordinator. If
# `Location` were ignored or wrong, this gate cannot pass by accident — the
# worker ports are different processes.
#
# CONDA_PREFIX is set for the children because flare resolves its FFI shims
# through $CONDA_PREFIX/lib before falling back to ./build; unset, the missing
# zlib shim surfaces as a torn-down connection rather than a clear error.
#
# Plain strings rather than bash arrays with negative indices: macOS runners
# still ship bash 3.2, where `${a[-1]}` is a syntax error rather than the last
# element.
set -euo pipefail

WAREHOUSE="$PWD/build/warehouse-cluster"
TABLE="$(python tools/make_fixture.py "$WAREHOUSE" | tail -1)"
: "${CONDA_PREFIX:?run this through pixi so the FFI shims resolve}"

COORD_PORT=8815
WORKER_A=8816
WORKER_B=8817

PIDS=""
LOGS=""
trap 'for p in $PIDS; do kill "$p" 2>/dev/null || true; done' EXIT

# Wait for the port rather than sleeping a guessed number of seconds: each
# server plans the scan before it binds, so how long that takes depends on the
# table and the machine. A fixed sleep passes locally and fails on a slower
# runner, which is the least useful way for a gate to fail.
wait_for_port() {
  local port="$1" pid="$2" log="$3"
  for _ in $(seq 1 60); do
    if command -v nc >/dev/null 2>&1; then
      nc -z 127.0.0.1 "$port" 2>/dev/null && return 0
    else
      (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null && return 0
    fi
    kill -0 "$pid" 2>/dev/null || break   # died: stop waiting, show the log
    sleep 1
  done
  echo "--- server on $port did not come up; its log:" >&2
  cat "$log" >&2
  return 1
}

start_worker() {
  local port="$1"
  local log="build/cluster-worker-$port.log"
  ./build/serve_ice "$TABLE" --port "$port" > "$log" 2>&1 &
  local pid=$!
  PIDS="$PIDS $pid"
  LOGS="$LOGS $log"
  wait_for_port "$port" "$pid" "$log"
}

start_worker "$WORKER_A"
start_worker "$WORKER_B"

# The coordinator is started last so a client that reaches it can assume the
# workers it names are already listening.
COORD_LOG="build/cluster-coordinator.log"
./build/serve_ice "$TABLE" --port "$COORD_PORT" \
  --worker "grpc+tcp://127.0.0.1:$WORKER_A" \
  --worker "grpc+tcp://127.0.0.1:$WORKER_B" \
  > "$COORD_LOG" 2>&1 &
COORD_PID=$!
PIDS="$PIDS $COORD_PID"
LOGS="$LOGS $COORD_LOG"
wait_for_port "$COORD_PORT" "$COORD_PID" "$COORD_LOG"

python tools/verify_cluster.py || {
  for log in $LOGS; do echo "--- $log:" >&2; cat "$log" >&2; done
  exit 1
}

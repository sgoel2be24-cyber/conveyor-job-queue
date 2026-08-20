#!/usr/bin/env bash
#
# Worker-crash demo.
#
# Shows what happens when a worker dies holding a job: nothing is lost, and the
# work finishes somewhere else.
#
#   1. worker-1 leases a long-running job and starts it
#   2. worker-1 is SIGKILLed mid-job -- no chance to report anything
#   3. its lease goes unrenewed and expires
#   4. the broker reclaims the job and charges the attempt
#   5. worker-2 picks it up and completes it
#
# No coordination between the workers is involved. The lease expiring is the
# only signal the broker needs, which is why it also covers a worker that hangs,
# loses its network, or gets stopped by the OS -- cases where the worker is
# still alive and cannot be asked to hand anything back.

set -euo pipefail

PORT="${PORT:-7802}"
QUEUE="${QUEUE:-crashdemo}"
LEASE="${LEASE:-3s}"
JOB_SECONDS="${JOB_SECONDS:-6}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin/conveyor"
BROKER="localhost:$PORT"
WORKDIR="$(mktemp -d)"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill -9 "$pid" 2>/dev/null || true
  done
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

say "Building..."
(cd "$ROOT" && go build -o "$BIN" ./cmd/conveyor)

say "Starting broker"
"$BIN" broker start --listen "$BROKER" --data-dir "$WORKDIR/data" >"$WORKDIR/broker.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 100); do
  "$BIN" status --broker "$BROKER" >/dev/null 2>&1 && break
  sleep 0.1
done

say "Submitting one job that takes ${JOB_SECONDS}s"
JOB_ID=$("$BIN" submit --broker "$BROKER" --queue "$QUEUE" --handler shell \
  --payload "sleep $JOB_SECONDS; echo finished" --max-retries 5)
echo "job: $JOB_ID"

say "Starting worker-1 (lease $LEASE)"
"$BIN" worker --broker "$BROKER" --queue "$QUEUE" --concurrency 1 \
  --worker-id worker-1 --lease-duration "$LEASE" >"$WORKDIR/worker-1.log" 2>&1 &
WORKER1=$!
PIDS+=("$WORKER1")

sleep 2
echo "worker-1 has it:"
"$BIN" get --broker "$BROKER" "$JOB_ID" | sed 's/^/  /'

say "kill -9 worker-1 (mid-job, no chance to report)"
kill -9 "$WORKER1" 2>/dev/null || true
wait "$WORKER1" 2>/dev/null || true
echo "killed."

say "Waiting for the lease to expire and the broker to reclaim it"
for _ in $(seq 1 100); do
  state=$("$BIN" get --broker "$BROKER" "$JOB_ID" 2>/dev/null | awk '$1=="state"{print $2}')
  [[ "$state" == "retry_wait" ]] && break
  sleep 0.2
done
"$BIN" get --broker "$BROKER" "$JOB_ID" | sed 's/^/  /'

say "Starting worker-2"
"$BIN" worker --broker "$BROKER" --queue "$QUEUE" --concurrency 1 \
  --worker-id worker-2 --lease-duration 30s >"$WORKDIR/worker-2.log" 2>&1 &
PIDS+=($!)

say "Waiting for worker-2 to finish the job"
for _ in $(seq 1 200); do
  state=$("$BIN" get --broker "$BROKER" "$JOB_ID" 2>/dev/null | awk '$1=="state"{print $2}')
  [[ "$state" == "done" ]] && break
  sleep 0.2
done

"$BIN" get --broker "$BROKER" "$JOB_ID" | sed 's/^/  /'
echo
if [[ "${state:-}" == "done" ]]; then
  echo "PASS: the job survived its worker being killed and completed on another."
else
  echo "FAIL: job ended in state '${state:-unknown}'"
  exit 1
fi

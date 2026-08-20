#!/usr/bin/env bash
#
# Crash-recovery harness.
#
# Repeatedly kills the broker with SIGKILL in the middle of a heavy submission
# burst, restarts it, and checks that every job the broker acknowledged before
# dying is still there afterwards.
#
# The invariant under test is the one producers are actually promised:
#
#   a job whose Submit returned successfully is durable.
#
# A submission still in flight when the process died may or may not survive --
# that is not a violation. So the check is `recovered >= acknowledged`, plus an
# exact lookup of the final acknowledged ID. Because the WAL is append-only and
# ordered, the last acknowledged record surviving implies every earlier one did
# too, which makes that single lookup a whole-prefix check rather than a spot
# check.
#
# Usage: scripts/crash_test.sh [trials]
#   TRIALS=100 JOBS=4000 scripts/crash_test.sh

set -euo pipefail

TRIALS="${1:-${TRIALS:-50}}"
JOBS="${JOBS:-3000}"
PORT="${PORT:-7799}"
QUEUE="${QUEUE:-crashtest}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin/conveyor"
BROKER="localhost:$PORT"
WORKDIR="$(mktemp -d)"
BROKER_PID=""

cleanup() {
  [[ -n "$BROKER_PID" ]] && kill -9 "$BROKER_PID" 2>/dev/null || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

start_broker() {
  local data_dir="$1"
  "$BIN" broker start --listen "$BROKER" --data-dir "$data_dir" >>"$WORKDIR/broker.log" 2>&1 &
  BROKER_PID=$!

  for _ in $(seq 1 100); do
    if "$BIN" status --broker "$BROKER" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "FATAL: broker did not become ready; see $WORKDIR/broker.log" >&2
  return 1
}

stop_broker() {
  if [[ -n "$BROKER_PID" ]]; then
    kill -9 "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
    BROKER_PID=""
  fi
}

echo "Building..."
(cd "$ROOT" && go build -o "$BIN" ./cmd/conveyor)

echo "Running $TRIALS crash trials ($JOBS jobs per trial, kill -9 mid-burst)"
echo

failures=0
total_acked=0

for trial in $(seq 1 "$TRIALS"); do
  data_dir="$WORKDIR/data-$trial"
  acked_file="$WORKDIR/acked-$trial.txt"
  mkdir -p "$data_dir"
  : >"$acked_file"

  start_broker "$data_dir"

  # Submit hard; each accepted job ID lands in acked_file as the broker
  # confirms it.
  "$BIN" submit --broker "$BROKER" --queue "$QUEUE" --count "$JOBS" \
    --payload "trial-$trial" >"$acked_file" 2>/dev/null &
  submitter=$!

  # Kill at an unpredictable point so the tear lands at a different place each
  # trial, rather than always at a clean boundary.
  sleep "0.$(( (RANDOM % 15) + 2 ))"
  kill -9 "$BROKER_PID" 2>/dev/null || true
  wait "$submitter" 2>/dev/null || true
  wait "$BROKER_PID" 2>/dev/null || true
  BROKER_PID=""

  acked=$(grep -c . "$acked_file" || true)
  if [[ "$acked" -eq 0 ]]; then
    echo "trial $trial: SKIP (broker died before acknowledging anything)"
    continue
  fi
  last_id=$(tail -n 1 "$acked_file" | cut -f1)

  # Restart and see what survived.
  start_broker "$data_dir"

  recovered=$("$BIN" status --broker "$BROKER" --queue "$QUEUE" 2>/dev/null \
    | awk '/^'"$QUEUE"'/ {print $2}')
  recovered="${recovered:-0}"

  ok=1
  if [[ "$recovered" -lt "$acked" ]]; then
    echo "trial $trial: FAIL — acknowledged $acked jobs, recovered only $recovered"
    ok=0
  fi
  if ! "$BIN" get --broker "$BROKER" "$last_id" >/dev/null 2>&1; then
    echo "trial $trial: FAIL — last acknowledged job $last_id is missing after restart"
    ok=0
  fi

  stop_broker

  if [[ "$ok" -eq 1 ]]; then
    printf 'trial %d: ok — %d acknowledged, %d recovered\n' "$trial" "$acked" "$recovered"
    total_acked=$((total_acked + acked))
  else
    failures=$((failures + 1))
  fi
done

echo
if [[ "$failures" -eq 0 ]]; then
  echo "PASS: $TRIALS trials, $total_acked acknowledged jobs, zero lost."
else
  echo "FAIL: $failures of $TRIALS trials lost acknowledged jobs."
  exit 1
fi

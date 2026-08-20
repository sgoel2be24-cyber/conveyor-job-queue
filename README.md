# Conveyor

A durable, distributed job-processing pipeline — a from-scratch, scoped-down
version of the class of system Celery, Sidekiq, and SQS+workers occupy.
Producers submit jobs from the CLI; a broker persists them to a hand-rolled
write-ahead log; a pool of workers leases and executes them with at-least-once
delivery, retries with backoff, and dead-lettering.

The interesting part is not the happy path — it is what happens when things
die. Conveyor's central claim is that **a job the broker has acknowledged
survives a `kill -9`**, and that claim is tested rather than asserted.

**Status:** feature-complete. Jobs are durably queued, dispatched to workers,
retried with backoff, and dead-lettered, with Prometheus metrics and a
load-generator for measuring it. Broker high availability and a chaos-testing
harness remain as optional extensions — see [docs/DESIGN.md](docs/DESIGN.md).

## Try it

```sh
go build -o bin/conveyor ./cmd/conveyor

# Terminal 1 — the broker
./bin/conveyor broker start --data-dir ./data

# Terminal 2 — a pool of 4 workers
./bin/conveyor worker --queue emails --concurrency 4

# Terminal 3 — submit work
./bin/conveyor submit --queue emails --handler shell --payload 'echo hello'

# A job that always fails: watch it retry, then land in the dead-letter queue
./bin/conveyor submit --queue emails --handler shell \
  --payload 'echo "payment API down" >&2; exit 1' --max-retries 2

./bin/conveyor status
./bin/conveyor dlq list
./bin/conveyor dlq replay <job-id>     # after fixing the cause

# Prometheus metrics, and a load generator to move them
curl -s localhost:7777/metrics | grep '^conveyor_'
./bin/conveyor bench --concurrency 64 --duration 10s
```

Two things worth doing yourself, because they are the whole point:

```sh
scripts/crash_test.sh          # kill -9 the broker mid-burst, 50x — no acknowledged job is lost
scripts/worker_crash_demo.sh   # kill -9 a worker mid-job — another worker finishes it
```

Idempotent submission works too — the second call returns the first job rather
than enqueueing a duplicate:

```sh
./bin/conveyor submit --queue emails --payload x --idempotency-key order-42
./bin/conveyor submit --queue emails --payload x --idempotency-key order-42
```

## Architecture

```
producer (CLI submit) --> WAL (length-prefixed, CRC32C, F_FULLFSYNC) --> in-memory
                                                                          index
                                                                            |
                                                                  dispatcher: per-queue
                                                                  priority + delay heaps,
                                                                  lease expiry sweep
                                                                            |
                                                          Lease (streaming Connect RPC)
                                                                            v
                                                                     worker pool
                                                                (bounded concurrency,
                                                                 heartbeat renewal)
                                                                            |
                                                        Ack / Nack (carrying a fencing token)
                                                                            v
                                                    Done | Retry(backoff+jitter) | DeadLetter
```

Every mutation a caller waits on is written to the log and flushed to stable
storage *before* it is applied in memory and acknowledged. On startup the broker
rebuilds its state from the newest snapshot plus the records written after it.

Four details carry most of the correctness weight:

- **Torn writes.** A process killed mid-append leaves a partial record. Each
  record is length-prefixed and checksummed, so replay recognizes the first
  unreadable record as the moment of the crash and stops there — and `Open`
  truncates the file back to that boundary, so later appends can't land behind
  a gap that would hide them from the next replay.
- **`F_FULLFSYNC`, not `fsync`.** On macOS, `fsync(2)` only pushes data to the
  drive; it can still sit in a volatile write cache that a power loss wipes.
  `F_FULLFSYNC` flushes that cache. It is ~2,900× more expensive than the write
  itself (see below), and that cost is the single most important fact about
  this system's performance.
- **Fencing tokens.** A worker that merely stalls is indistinguishable from a
  dead one. Its lease expires, the job goes to somebody else, and then it wakes
  up and reports success for work that is no longer its own. Every lease carries
  a monotonic token, and a report carrying a stale one is refused. Tokens are
  reserved in durable blocks so this costs one flush per *million* leases rather
  than one per lease — see [docs/DESIGN.md](docs/DESIGN.md).
- **Snapshots.** Without periodically snapshotting state and dropping the
  segments it covers, the log grows forever even though most jobs end up
  terminal — and recovery time grows with it.

### Retries and the dead-letter queue

A failed job is retried with exponential backoff and *equal jitter* — half the
delay fixed, half random. The jitter is the part that matters: jobs that fail
together usually fail for the same reason, and without it they would retry in
lockstep forever, hammering whatever is already unhealthy at exactly the same
instants.

A job that exhausts its retry budget is dead-lettered rather than retried
forever. Crucially, **a lease that times out consumes the retry budget exactly
as an explicit failure does.** If it didn't, a job whose handler reliably
outlives its lease would cycle `leased → timeout → leased` forever, never
dead-letter, and occupy a worker slot every time round. Both paths run through
one function so that invariant is structural rather than remembered.

## Measured results

Apple M5, macOS 26.6, APFS on internal SSD. Reproduce with
`go test -bench . -benchtime 2s -run '^$' ./internal/wal/`.

### Durability costs what it costs

Encoding and writing a record costs **1.29 µs**. Making it survive a power loss
costs **3.78 ms**. That is the single most important fact about this system:
**99.97% of a durable write is waiting for the drive.** No amount of faster
serialization moves that number.

### Group commit turns it into throughput

A flush makes *every* record written before it durable, so N concurrent
submitters need one flush between them rather than N. Measured end to end
through the RPC stack with `conveyor bench`:

| Concurrent submitters | Throughput | p50 latency | Submissions per flush |
| --- | --- | --- | --- |
| 1 | 276/s | 3.9 ms | 1.0 |
| 8 | 1,227/s | 6.3 ms | 4.0 |
| 64 | 8,938/s | 7.1 ms | 31.6 |
| 256 | **32,232/s** | 7.8 ms | 121.4 |

**117× more throughput, and latency barely moves.** The disk is doing the same
work throughout — roughly 280 flushes per second at ~3.5 ms each, whether
throughput is 276/s or 32,000/s. All the extra capacity comes from amortizing a
cost that was already being paid.

A single submitter sees no gain, which is correct: there is nobody to share a
flush with. Conveyor deliberately does **not** wait to accumulate a batch —
waiting would trade latency for batch size, and the batch fills on its own
exactly when there is load to fill it.

Getting here took two fixes that only showed up under measurement, both
described in [docs/DESIGN.md](docs/DESIGN.md): a lock held across the flush, and
`write(2)` on the caller's path. Reproduce with:

```sh
conveyor bench --concurrency 1     # then compare
conveyor bench --concurrency 256
```

### Recovery is fast

100,000 records replayed and applied in **7.7 ms** (~77 ns/record). Recovery
time is bounded by snapshotting, not by total jobs ever submitted.

### Crash recovery holds up

`scripts/crash_test.sh` starts a broker, submits jobs as fast as it can,
`kill -9`s the broker at an unpredictable point mid-burst, restarts it, and
checks that every acknowledged job is still there.

```
PASS: 50 trials, 4003 acknowledged jobs, zero lost.
```

Recovered counts frequently come out one *higher* than acknowledged counts.
That is not a bug: it is a submission that was durably logged in the instant
before the process died, whose response never made it back to the client. A
producer that retries it will get a duplicate unless it supplies an idempotency
key — which is exactly the at-least-once contract, visible in practice.

### Worker crashes are a non-event

`scripts/worker_crash_demo.sh` kills a worker mid-job with `SIGKILL` — no
chance to hand anything back — and watches the job finish elsewhere:

```
kill -9 worker-1 (mid-job, no chance to report)

  state       retry_wait
  attempt     1 of 5
  epoch       1
  last-error  lease expired      <- broker reclaimed it

  state       done
  attempt     2 of 5
  epoch       2                  <- fencing token bumped on reassignment
```

The lease expiring is the only signal the broker needs, which is why the same
mechanism also covers a worker that hangs, loses its network, or gets stopped by
the OS — cases where the worker is still alive and cannot be asked to hand
anything back.

## Testing

```sh
go test -race ./...          # unit + end-to-end tests, race detector on
scripts/crash_test.sh        # 50 kill -9 broker trials (TRIALS=100 JOBS=5000 to go harder)
scripts/worker_crash_demo.sh # kill -9 a worker mid-job
```

The WAL's crash-safety tests simulate a process killed at **every possible byte
offset** within the final record, asserting each time that complete records
survive, the partial one is discarded, and the log is immediately writable
again.

The invariants the whole design rests on were verified by mutation — a test is
only worth having if reintroducing the bug makes it fail:

| Bug reintroduced | Test that caught it |
| --- | --- |
| Fencing checks the token but not the lease state | `TestZombieWorkerAckIsFenced` |
| A timed-out lease doesn't consume the retry budget | `TestLeaseTimeoutConsumesRetryBudget` |
| The append lock is held across the flush | `TestGroupCommitSharesFlushes` |

End-to-end tests run the real stack — HTTP server, streaming RPC, worker pool —
including a deliberately wedged worker that loses its job to another and then
has its late report refused.

## Development

Requires Go 1.27+ and [buf](https://buf.build) for protobuf codegen.

```sh
buf lint && buf generate    # after editing proto/conveyor/v1/queue.proto
```

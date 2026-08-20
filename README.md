# Conveyor

A durable, distributed job-processing pipeline — a from-scratch, scoped-down
version of the class of system Celery, Sidekiq, and SQS+workers occupy.
Producers submit jobs from the CLI; a broker persists them to a hand-rolled
write-ahead log; a pool of workers leases and executes them with at-least-once
delivery, retries with backoff, and dead-lettering.

The interesting part is not the happy path — it is what happens when things
die. Conveyor's central claim is that **a job the broker has acknowledged
survives a `kill -9`**, and that claim is tested rather than asserted.

**Status:** Phase 1 of 5 complete. The broker durably accepts, stores, and
recovers jobs. Leasing and execution (Phase 2) are not implemented yet, so
`worker`, `dlq`, and `bench` are still stubs. See
[docs/DESIGN.md](docs/DESIGN.md) for the full design and roadmap.

## What works today

```sh
go build -o bin/conveyor ./cmd/conveyor

# Start the broker
./bin/conveyor broker start --data-dir ./data

# Submit jobs (each accepted job's ID is printed once it is durably logged)
./bin/conveyor submit --queue emails --payload '{"to":"a@b.com"}' --count 3

# Idempotent submission — the second call returns the first job
./bin/conveyor submit --queue emails --payload x --idempotency-key order-42
./bin/conveyor submit --queue emails --payload x --idempotency-key order-42

# Inspect
./bin/conveyor status
./bin/conveyor get <job-id>
```

Kill the broker with `kill -9`, start it again against the same `--data-dir`,
and `status` reports the same jobs.

## Architecture

```
producer (CLI submit) --> WAL (length-prefixed, CRC32C, F_FULLFSYNC) --> in-memory
                                                                          index
                                                                            |
                                                                  dispatcher issues
                                                                  leases (Connect RPC)   [Phase 2]
                                                                            v
                                                                     worker pool          [Phase 2]
                                                                            |
                                                        Ack / Nack (carrying a fencing token)
                                                                            v
                                                    Done | Retry(backoff) | DeadLetter
```

Every mutation is written to the log and flushed to stable storage *before* it
is applied in memory and acknowledged to the producer. On startup the broker
rebuilds its index from the newest snapshot plus the records written after it.

Three details carry most of the correctness weight:

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
- **Snapshots.** Without periodically snapshotting the index and dropping the
  segments it covers, the log grows forever even though most jobs end up
  terminal — and recovery time grows with it.

## Measured results

Apple M5, macOS 26.6, APFS on internal SSD. Reproduce with
`go test -bench . -benchtime 2s -run '^$' ./internal/wal/`.

### Durability costs what it costs

| Commit mode | Per record | Throughput | vs. per-record flush |
| --- | --- | --- | --- |
| `F_FULLFSYNC` every record | 3.78 ms | ~264 rec/s | 1× |
| Group commit, batch 8 | 465 µs | ~2,150 rec/s | 8.1× |
| Group commit, batch 32 | 121 µs | ~8,260 rec/s | 31× |
| Group commit, batch 128 | 29.4 µs | ~34,000 rec/s | 129× |
| Group commit, batch 512 | 10.6 µs | ~94,300 rec/s | 357× |
| No flush (encode + write only) | 1.29 µs | ~777,000 rec/s | 2,940× |

Encoding and writing a record costs 1.29 µs; making it survive a power loss
costs 3.78 ms. **99.97% of a durable write is waiting for the drive**, which is
why batching commits — not a faster serialization format — is the lever that
matters. Amortizing one flush across 512 records buys a 357× throughput gain in
exchange for a bounded latency window.

The broker currently flushes once per submission (the 264 rec/s row). Wiring
group commit into the submit path is Phase 3; the numbers above are the ceiling
it is aiming at.

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

## Testing

```sh
go test -race ./...          # unit tests, race detector on
scripts/crash_test.sh        # 50 kill -9 trials (TRIALS=100 JOBS=5000 to go harder)
```

The WAL's crash-safety tests simulate a process killed at **every possible byte
offset** within the final record, asserting each time that complete records
survive, the partial one is discarded, and the log is immediately writable
again.

## Development

Requires Go 1.27+ and [buf](https://buf.build) for protobuf codegen.

```sh
buf lint && buf generate    # after editing proto/conveyor/v1/queue.proto
```

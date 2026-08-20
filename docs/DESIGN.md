# Conveyor — Design

## Overview

Conveyor is a durable, distributed job-processing pipeline: producers submit
jobs over a CLI, a broker persists them safely to disk, and a pool of worker
processes leases and executes them with at-least-once delivery, automatic
retries, and dead-lettering. It occupies the same niche as Celery, Sidekiq,
or SQS+workers, built from first principles so its durability and
correctness claims are real, not assumed.

## Goals / non-goals

**Goals:** real single-node durability (survives crashes with zero job loss);
horizontal worker scaling; full observability via Prometheus + CLI; broker
high availability as a stretch goal.

**Non-goals (for now):** multi-region replication, exactly-once execution
(delivery is at-least-once by design — see "Delivery semantics" below), a
web UI, Linux support (the WAL's durability path is currently Darwin-specific;
see below).

## Data path

```
producer (CLI submit) --> WAL (length-prefixed, checksummed, fsync'd) --> in-memory
                                                                           per-queue
                                                                           priority/delay
                                                                           index (rebuilt
                                                                           from snapshot +
                                                                           WAL at startup)
                                                                                |
                                                                    dispatcher issues
                                                                    leases (Connect RPC,
                                                                    server-streaming)
                                                                                v
                                                                    worker pool (N workers,
                                                                    pluggable handlers)
                                                                                |
                                                          Ack(id, epoch) / Nack(id, epoch, reason)
                                                                                v
                                                    Done | Retry-wait(backoff+jitter) | DeadLetter
```

## WAL format

Each record: `[4-byte length][4-byte CRC32 checksum][payload]`, appended to
size-rotated segment files, single writer.

**Replay / crash recovery:** read records sequentially from the last
snapshot. A checksum mismatch or a truncated record at EOF means the process
died mid-write; treat it as "this is where the crash happened" and stop
cleanly rather than erroring out. This torn-write case is the most common bug
in hand-rolled WALs and is an explicit, tested code path, not an
afterthought.

**Durability on macOS:** plain `fsync()` on Darwin only flushes to the
drive's write cache, not stable storage — it does not provide the durability
guarantee the whole system's correctness claim rests on. The WAL writer uses
`fcntl(fd, F_FULLFSYNC)` instead, behind a small platform-specific
abstraction (`internal/wal/sync_darwin.go` using `F_FULLFSYNC`; a
`sync_other.go` fallback using plain `f.Sync()` for portability, untested).
`F_FULLFSYNC` is slower than a plain `fsync` — this cost is exactly what
motivates group commit (see "Benchmarks to capture" below).

**Snapshotting is required, not optional**, even in the minimal build:
periodically serialize the in-memory index plus the WAL offset it covers,
then delete fully-covered older segments. Without this the WAL grows forever
even though most jobs are terminal, and crash-recovery time grows unboundedly
with it.

**Fallback:** if WAL crash-recovery tests aren't reliably green within the
first stretch of implementation, switch the job log to `etcd-io/bbolt` and
keep everything else (leasing, fencing, retry, DLQ, observability)
unchanged. Most of the project's distributed-systems-correctness value lives
above the storage layer, not inside it.

## Job state machine

```
Pending --lease(epoch E)--> Leased
Leased  --Ack(id, E)------> Done
Leased  --Nack(id, E)------> Retry-wait --backoff timer--> Pending
Leased  --lease timeout----> Retry-wait --backoff timer--> Pending   (same counter as Nack)
Retry-wait --max retries exceeded--> DeadLetter                      (from either path)
any Ack/Nack/Heartbeat with epoch < job's current epoch --> rejected, no state change
```

**Unified retry counting is a required invariant:** a lease-timeout reclaim
must increment the *same* counter an explicit `Nack` does. If it doesn't, a
poison-pill payload (or a handler bug) loops
`Pending → Leased → timeout → Pending` forever and never reaches the DLQ —
a real liveness bug and a stuck-queue resource leak, not a hypothetical.

## Delivery semantics: idempotency keys vs. fencing tokens

Two mechanisms, easy to conflate, protecting different seams — the system
needs both:

- **Idempotency keys** (producer-supplied, at `submit` time): dedup a
  logically-identical job submitted twice. Protects the *submission*
  boundary.
- **Fencing tokens** (broker-issued monotonic `epoch`, bumped on every lease
  issuance including reclaim): if a worker was merely slow — a GC pause, a
  scheduler delay, a slow network — not actually dead, its lease can time
  out and be reassigned while it's still working. If its stale `Ack` arrives
  after reclaim, the broker rejects it by epoch comparison instead of
  corrupting job state. Protects the *execution/lease* boundary. This is the
  zombie-worker / stale-lease hazard (distinct from "split-brain," which is
  the broker-leader-election failure mode in the HA phase) — see
  Kleppmann's "How to do distributed locking" for the canonical treatment.

Fencing does **not** retroactively un-fire a side effect the zombie worker
already triggered (e.g. a webhook it already POSTed) — that's inherent to
at-least-once delivery with non-idempotent handlers, not a bug. This is
demonstrated deliberately, not just documented: the HTTP-webhook handler
forwards the idempotency key as a request header so a well-behaved receiver
can dedup; the shell-exec handler uses `exec.CommandContext` so lease-expiry
cancellation gives a best-effort abort (best-effort only — it cannot help
once a webhook request is already in flight server-side).

## RPC contract

See [`proto/conveyor/v1/queue.proto`](../proto/conveyor/v1/queue.proto).
`Lease` is a server-streaming RPC handing out job assignments as they become
available; `Ack`/`Nack`/`Heartbeat` all carry the job's `epoch` and are
rejected if it's stale. Generated code lives in `internal/genproto/` (run
`buf generate` after editing the `.proto`).

## Observability

Prometheus `/metrics`: enqueue rate, dispatch latency histogram, queue depth
gauge, retry/failure counters. CLI: `status`, `dlq list`, `dlq replay`.

## Benchmarks

Measured on Apple M5 / macOS 26.6 / APFS. Full tables in the
[README](../README.md).

1. **Crash-recovery integrity.** ✅ `scripts/crash_test.sh` — 50 `kill -9`
   trials mid-burst: 4,003 acknowledged jobs, zero lost. Recovered counts
   sometimes exceed acknowledged ones by exactly one (a submission logged in
   the instant before death whose response never returned) — the at-least-once
   contract, observed rather than assumed.
2. **Recovery speed.** ✅ 100,000 records replayed in 7.7 ms (~77 ns/record).
3. **Group-commit throughput.** ⏳ baseline measured, not yet wired in. One
   `F_FULLFSYNC` per record costs 3.78 ms (~264 rec/s); encoding and writing
   that record costs 1.29 µs. 99.97% of a durable write is waiting on the
   drive. Amortizing one flush over 512 records drops the per-record cost to
   10.6 µs — a 357× gain for a bounded latency window. Wiring this into the
   submit path is Phase 3.
4. **Zombie-worker fencing test.** ⏳ Phase 2. A deliberate test that delays a
   worker's `Ack` past its lease expiry via a test hook, asserting the
   broker's epoch check rejects the stale `Ack` rather than corrupting job
   state.

## Build phases

- [x] **Phase 0** — repo scaffolding, module, CI, proto schema, CLI skeleton.
- [x] **Phase 1** — WAL writer/reader (CRC32C checksums, torn-write handling,
      `F_FULLFSYNC`, segment rotation, snapshotting), in-memory index,
      idempotency keys, `broker start` / `submit` / `status` / `get`.
      Acceptance bar met: torn-tail tests at every byte offset, plus 50 real
      `kill -9` trials with zero acknowledged jobs lost.
- [ ] **Phase 2** — Connect RPC `Lease`/`Ack`/`Nack`/`Heartbeat` with
      fencing, worker pool, lease timeout + heartbeat renewal, retry+backoff
      into the unified counter, DLQ, shell-exec handler (+ webhook handler).
- [ ] **Phase 3** — Prometheus metrics, `status`/`dlq list`/`dlq replay`, the
      three benchmark deliverables above, README with real measured numbers.
- [ ] **Phase 4 (stretch)** — broker HA via `hashicorp/raft`-backed leader
      election, WAL replication, automatic failover.
- [ ] **Phase 5 (stretch)** — chaos harness: random `SIGKILL` of
      workers/leader under load, asserting delivery guarantees hold.

# Conveyor

A durable, distributed job-processing pipeline — a from-scratch, scoped-down
version of the class of system Celery, Sidekiq, and SQS+workers occupy.
Producers submit jobs from the CLI; a broker persists them to a hand-rolled
write-ahead log; a pool of workers leases and executes them with at-least-once
delivery, retries with backoff, and dead-lettering. Everything is observable
and controllable from one binary.

**Status:** early scaffolding. See [docs/DESIGN.md](docs/DESIGN.md) for the
full design — WAL format, delivery-semantics state machine, fencing tokens —
and the build-phase roadmap.

## Build

```sh
go build -o bin/conveyor ./cmd/conveyor
./bin/conveyor --help
```

## Planned usage

```sh
conveyor broker start --data-dir ./data
conveyor worker start --queue emails --concurrency 8
conveyor submit --queue emails --payload '{"to":"a@b.com"}' --max-retries 5
conveyor status
conveyor dlq list
```

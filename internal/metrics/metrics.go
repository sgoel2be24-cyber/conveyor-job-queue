// Package metrics defines the Prometheus instrumentation exposed by the broker
// at /metrics.
//
// The metric set is chosen to answer the questions an on-call operator actually
// asks: is work piling up, is it failing, and is the broker itself the
// bottleneck. The commit histograms exist because a durable write is by far the
// most expensive thing the broker does -- if throughput is disappointing,
// conveyor_wal_commit_seconds and conveyor_wal_commit_batch_records together
// say whether the disk is slow or the batching simply has nothing to batch.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "conveyor"

var (
	// JobsSubmitted counts accepted submissions.
	JobsSubmitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "jobs_submitted_total",
		Help:      "Jobs durably enqueued.",
	}, []string{"queue"})

	// JobsDeduplicated counts submissions collapsed onto an existing job by
	// their idempotency key.
	JobsDeduplicated = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "jobs_deduplicated_total",
		Help:      "Submissions that matched an existing idempotency key and did not create a job.",
	}, []string{"queue"})

	// JobsLeased counts deliveries to a worker. This exceeds jobs_submitted
	// whenever jobs are being retried.
	JobsLeased = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "jobs_leased_total",
		Help:      "Job deliveries to a worker, including redeliveries.",
	}, []string{"queue"})

	// JobsCompleted counts successful executions.
	JobsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "jobs_completed_total",
		Help:      "Jobs acknowledged as successful.",
	}, []string{"queue"})

	// JobsFailed counts failed deliveries. The cause label separates a handler
	// reporting failure from a lease the broker had to reclaim, which is the
	// difference between "the work is broken" and "the worker is".
	JobsFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "jobs_failed_total",
		Help:      "Failed deliveries, by cause.",
	}, []string{"queue", "cause"})

	// JobsDeadLettered counts jobs that exhausted their retry budget.
	JobsDeadLettered = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "jobs_dead_lettered_total",
		Help:      "Jobs that exhausted their retry budget.",
	}, []string{"queue"})

	// JobsReplayed counts dead-lettered jobs returned to their queue.
	JobsReplayed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "jobs_replayed_total",
		Help:      "Dead-lettered jobs requeued by an operator.",
	}, []string{"queue"})

	// FencedRequests counts Ack/Nack/Heartbeat calls refused because the caller
	// no longer held the lease. A non-zero rate here means workers are stalling
	// past their leases -- healthy, in that the fence is doing its job, but
	// worth investigating.
	FencedRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "fenced_requests_total",
		Help:      "Requests rejected because the caller's lease had been reclaimed.",
	}, []string{"op"})

	// QueueDepth reports how many jobs sit in each state.
	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "queue_jobs",
		Help:      "Jobs currently in each state.",
	}, []string{"queue", "state"})

	// DispatchDelay measures how long a job waited between becoming eligible
	// and being handed to a worker -- queueing delay, with retry backoff
	// excluded by construction.
	DispatchDelay = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "dispatch_delay_seconds",
		Help:      "Time from a job becoming eligible to being leased.",
		// Spans sub-millisecond (idle broker, worker already waiting) to tens
		// of seconds (backlogged).
		Buckets: prometheus.ExponentialBuckets(0.0005, 3, 12),
	}, []string{"queue"})

	// CommitDuration measures a durable commit: the F_FULLFSYNC plus whatever
	// batching waited on it.
	CommitDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "wal_commit_seconds",
		Help:      "Duration of a write-ahead log commit (flush to stable storage).",
		// A flush costs single-digit milliseconds on a healthy SSD; the upper
		// buckets catch a disk in trouble.
		Buckets: prometheus.ExponentialBuckets(0.0001, 3, 12),
	})

	// CommitBatchSize records how many callers shared one flush. This is the
	// number that explains throughput: a batch size stuck at 1 means submitters
	// are arriving one at a time and each is paying for its own flush.
	CommitBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "wal_commit_batch_records",
		Help:      "Number of waiters that shared a single flush.",
		Buckets:   []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
	})

	// Commits counts flushes issued.
	Commits = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "wal_commits_total",
		Help:      "Flushes to stable storage.",
	})

	// RecoveryDuration reports how long the last startup recovery took.
	RecoveryDuration = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "recovery_seconds",
		Help:      "Time taken to rebuild state from snapshot and log at startup.",
	})

	// RecoveredJobs reports how many jobs that recovery restored.
	RecoveredJobs = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "recovered_jobs",
		Help:      "Jobs restored at startup.",
	})
)

// Causes for JobsFailed.
const (
	CauseHandler = "handler"
	CauseTimeout = "lease_expired"
)

package broker

import (
	"sync"
	"time"

	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/metrics"
	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/wal"
)

// committer amortizes one flush to stable storage across everyone waiting for
// durability at that moment.
//
// Flushing is the dominant cost in this system by three orders of magnitude:
// encoding and writing a record takes microseconds, forcing it onto the disk
// takes milliseconds. A broker that flushes once per submission therefore tops
// out near a few hundred submissions a second no matter how fast the rest of it
// is -- the CPU spends essentially all its time waiting on the drive.
//
// The fix is that a flush is not per-record. Flushing the file makes *every*
// record written before it durable, so N concurrent submitters need one flush
// between them, not N. Callers append their record, release the store lock, and
// arrive here; whoever arrives first starts a flush, and everyone who arrives
// while it runs is served by the next one.
//
// There is deliberately no timer forcing a batch to accumulate. Waiting would
// trade latency for a bigger batch, and it is not needed: the batch fills on its
// own exactly when there is load to fill it. A single sequential submitter gets
// batches of one and pays a full flush every time -- which is correct, because
// there is nobody to share it with, and adding a delay would only make that
// submitter slower.
type committer struct {
	log *wal.WAL

	// onFlush, when set, is called with the batch size after each flush. Tests
	// use it to assert that batching is actually happening; it is nil in
	// production.
	onFlush func(batch int)

	mu      sync.Mutex
	waiters []chan error
	running bool
}

func newCommitter(log *wal.WAL) *committer {
	return &committer{log: log}
}

// commit blocks until every record appended before this call is on stable
// storage.
//
// The ordering that makes this safe: a caller appends its record *before*
// calling commit, so by the time it joins the waiter list its bytes are already
// in the file. Any flush that starts afterwards necessarily covers them.
func (c *committer) commit() error {
	ch := make(chan error, 1)

	c.mu.Lock()
	c.waiters = append(c.waiters, ch)
	if !c.running {
		c.running = true
		go c.flushLoop()
	}
	c.mu.Unlock()

	return <-ch
}

// flushLoop drains waiters one batch at a time until none are left. Callers that
// arrive during a flush are picked up by the next pass rather than being folded
// into the one already in progress -- their records may have been written after
// it started, so it cannot speak for them.
func (c *committer) flushLoop() {
	for {
		c.mu.Lock()
		batch := c.waiters
		c.waiters = nil
		if len(batch) == 0 {
			c.running = false
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()

		start := time.Now()
		err := c.log.Sync()

		metrics.CommitDuration.Observe(time.Since(start).Seconds())
		metrics.CommitBatchSize.Observe(float64(len(batch)))
		metrics.Commits.Inc()
		if c.onFlush != nil {
			c.onFlush(len(batch))
		}

		for _, ch := range batch {
			ch <- err
		}
	}
}

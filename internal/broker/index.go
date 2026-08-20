package broker

import (
	"container/heap"
	"time"

	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/job"
)

// indexEntry is a job's scheduling key. It deliberately holds a copy rather
// than a pointer: entries can go stale, and a stale *copy* is harmless where a
// stale pointer invites reading a job that has since moved on.
type indexEntry struct {
	id         string
	priority   job.Priority
	enqueuedAt time.Time
	eligibleAt time.Time
}

// readyHeap orders jobs that can run right now: highest priority first, oldest
// first within a priority. The ID tiebreak keeps ordering total, so behavior is
// reproducible rather than dependent on heap internals.
type readyHeap []indexEntry

func (h readyHeap) Len() int      { return len(h) }
func (h readyHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h readyHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority
	}
	if !h[i].enqueuedAt.Equal(h[j].enqueuedAt) {
		return h[i].enqueuedAt.Before(h[j].enqueuedAt)
	}
	return h[i].id < h[j].id
}
func (h *readyHeap) Push(x any) { *h = append(*h, x.(indexEntry)) }
func (h *readyHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}

// delayedHeap orders jobs that are waiting out a delay or a retry backoff,
// soonest-eligible first.
type delayedHeap []indexEntry

func (h delayedHeap) Len() int      { return len(h) }
func (h delayedHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h delayedHeap) Less(i, j int) bool {
	if !h[i].eligibleAt.Equal(h[j].eligibleAt) {
		return h[i].eligibleAt.Before(h[j].eligibleAt)
	}
	return h[i].id < h[j].id
}
func (h *delayedHeap) Push(x any) { *h = append(*h, x.(indexEntry)) }
func (h *delayedHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}

type queueIndex struct {
	ready   readyHeap
	delayed delayedHeap
}

// scheduleIndex decides which job a queue hands out next.
//
// It is a derived structure, not a source of truth: the job map in Store is
// authoritative. That distinction is what makes the design tractable, because
// heaps cannot cheaply remove an arbitrary element. Rather than trying to keep
// the heaps exactly in sync with every transition, entries are allowed to go
// stale and are discarded when popped -- so a job that was dead-lettered,
// acked, or re-leased while sitting in a heap simply falls out on its way past.
//
// Callers MUST re-check the popped job against the authoritative map before
// acting on it. Store.leaseLocked is the only caller, and does exactly that.
//
// On recovery the whole index is rebuilt from the job map instead of being
// replayed, which keeps replay from accumulating stale entries for every
// transition in the log's history.
type scheduleIndex struct {
	queues map[string]*queueIndex
}

func newScheduleIndex() *scheduleIndex {
	return &scheduleIndex{queues: make(map[string]*queueIndex)}
}

func (s *scheduleIndex) forQueue(name string) *queueIndex {
	q, ok := s.queues[name]
	if !ok {
		q = &queueIndex{}
		s.queues[name] = q
	}
	return q
}

// push files a job into the ready or delayed heap depending on whether its
// eligibility time has arrived.
func (s *scheduleIndex) push(j *job.Job, now time.Time) {
	e := indexEntry{
		id:         j.ID,
		priority:   j.Priority,
		enqueuedAt: j.EnqueuedAt,
		eligibleAt: j.EligibleAt,
	}
	q := s.forQueue(j.Queue)
	if e.eligibleAt.After(now) {
		heap.Push(&q.delayed, e)
		return
	}
	heap.Push(&q.ready, e)
}

// promote moves everything whose delay has elapsed into the ready heap.
func (s *scheduleIndex) promote(queue string, now time.Time) {
	q, ok := s.queues[queue]
	if !ok {
		return
	}
	for q.delayed.Len() > 0 && !q.delayed[0].eligibleAt.After(now) {
		e := heap.Pop(&q.delayed).(indexEntry)
		heap.Push(&q.ready, e)
	}
}

// pop returns the next candidate entry for a queue. The result is a candidate
// only: it may name a job that has since changed state, and the caller must
// verify it against the authoritative job map before acting on it.
func (s *scheduleIndex) pop(queue string) (indexEntry, bool) {
	q, ok := s.queues[queue]
	if !ok || q.ready.Len() == 0 {
		return indexEntry{}, false
	}
	return heap.Pop(&q.ready).(indexEntry), true
}

// nextDeadline returns the earliest time at which a delayed job in any queue
// becomes eligible, so the dispatcher can sleep until then instead of polling.
func (s *scheduleIndex) nextDeadline() (time.Time, bool) {
	var earliest time.Time
	for _, q := range s.queues {
		if q.delayed.Len() == 0 {
			continue
		}
		if earliest.IsZero() || q.delayed[0].eligibleAt.Before(earliest) {
			earliest = q.delayed[0].eligibleAt
		}
	}
	return earliest, !earliest.IsZero()
}

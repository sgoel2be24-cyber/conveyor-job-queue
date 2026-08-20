package broker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"conveyor/internal/job"
)

// DefaultDispatchTick is how often the dispatcher wakes on its own. Submissions
// and completions signal it directly, so this only bounds how late a lease
// expiry or an elapsed retry backoff can be noticed.
const DefaultDispatchTick = 200 * time.Millisecond

// workerStream is one connected worker's Lease stream.
type workerStream struct {
	id          uint64
	queue       string
	workerID    string
	maxInFlight int
	leaseDur    time.Duration
	ctx         context.Context
	// ch is buffered to maxInFlight, so handing a job over never blocks the
	// dispatch loop on a slow worker.
	ch chan *job.Job

	// inFlight is guarded by Dispatcher.mu.
	inFlight int
}

// Dispatcher matches ready jobs to connected workers and reclaims leases whose
// holders stopped responding.
//
// It is the only component that leases jobs, which keeps the "who gets what
// next" policy in one place; the Store just enforces that whatever it is told
// to do is legal and durable.
type Dispatcher struct {
	store  *Store
	logger *slog.Logger
	tick   time.Duration

	mu       sync.Mutex
	streams  map[uint64]*workerStream
	jobOwner map[string]*workerStream
	nextID   uint64

	wake chan struct{}
}

// NewDispatcher returns a dispatcher for store.
func NewDispatcher(store *Store, logger *slog.Logger) *Dispatcher {
	return &Dispatcher{
		store:    store,
		logger:   logger,
		tick:     DefaultDispatchTick,
		streams:  make(map[uint64]*workerStream),
		jobOwner: make(map[string]*workerStream),
		wake:     make(chan struct{}, 1),
	}
}

// Run drives dispatch until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	timer := time.NewTimer(d.tick)
	defer timer.Stop()

	for {
		d.reclaimExpired()
		d.dispatch()

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(d.sleepFor())

		select {
		case <-ctx.Done():
			return
		case <-d.wake:
		case <-d.store.Notify():
		case <-timer.C:
		}
	}
}

// sleepFor returns how long to wait before the next unprompted pass: the
// regular tick, or sooner if a delayed job becomes eligible before then.
func (d *Dispatcher) sleepFor() time.Duration {
	wait := d.tick
	if deadline, ok := d.store.NextDelayedDeadline(); ok {
		if until := time.Until(deadline); until > 0 && until < wait {
			wait = until
		}
	}
	if wait < time.Millisecond {
		wait = time.Millisecond
	}
	return wait
}

func (d *Dispatcher) signal() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// reclaimExpired takes back leases whose deadline has passed. The jobs go back
// through the same retry accounting an explicit failure would use.
func (d *Dispatcher) reclaimExpired() {
	ids, err := d.store.ReclaimExpired(time.Now().UTC())
	if err != nil {
		d.logger.Error("reclaim expired leases", "err", err)
	}
	for _, id := range ids {
		d.logger.Warn("lease expired, job requeued", "job", id)
		d.release(id)
	}
}

// dispatch hands out as many jobs as connected workers have room for.
func (d *Dispatcher) dispatch() {
	d.mu.Lock()
	streams := make([]*workerStream, 0, len(d.streams))
	for _, st := range d.streams {
		streams = append(streams, st)
	}
	d.mu.Unlock()

	if len(streams) == 0 {
		return
	}

	// Loop until nothing more can be placed, taking one job per stream per
	// pass so a single greedy worker cannot starve its peers.
	for {
		placed := false
		for _, st := range streams {
			if st.ctx.Err() != nil {
				continue
			}

			d.mu.Lock()
			hasRoom := st.inFlight < st.maxInFlight
			d.mu.Unlock()
			if !hasRoom {
				continue
			}

			j, err := d.store.Lease(st.queue, st.workerID, st.leaseDur)
			if err != nil {
				d.logger.Error("lease failed", "queue", st.queue, "worker", st.workerID, "err", err)
				continue
			}
			if j == nil {
				continue // nothing eligible on this queue
			}

			d.mu.Lock()
			st.inFlight++
			d.jobOwner[j.ID] = st
			d.mu.Unlock()

			select {
			case st.ch <- j:
				placed = true
			case <-st.ctx.Done():
				// The worker disconnected between leasing and handoff. Leave
				// the lease alone: it will expire and be reclaimed like any
				// other lease whose holder stopped talking.
			}
		}
		if !placed {
			return
		}
	}
}

// release records that a job is no longer occupying a worker slot.
func (d *Dispatcher) release(jobID string) {
	d.mu.Lock()
	if st, ok := d.jobOwner[jobID]; ok {
		delete(d.jobOwner, jobID)
		if st.inFlight > 0 {
			st.inFlight--
		}
	}
	d.mu.Unlock()
	d.signal()
}

// register adds a worker's Lease stream.
func (d *Dispatcher) register(ctx context.Context, queue, workerID string, maxInFlight int, leaseDur time.Duration) *workerStream {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.nextID++
	st := &workerStream{
		id:          d.nextID,
		queue:       queue,
		workerID:    workerID,
		maxInFlight: maxInFlight,
		leaseDur:    leaseDur,
		ctx:         ctx,
		ch:          make(chan *job.Job, maxInFlight),
	}
	d.streams[st.id] = st

	d.logger.Info("worker connected", "worker", workerID, "queue", queue, "max_in_flight", maxInFlight)
	d.signal()
	return st
}

// unregister drops a worker's stream.
//
// Jobs it still holds are left leased on purpose. A stream closing does not
// prove the worker is gone -- and a worker that was SIGKILLed never closes its
// stream at all -- so lease expiry stays the single mechanism for deciding a
// holder has stopped responding, rather than having two paths that could
// disagree.
func (d *Dispatcher) unregister(st *workerStream) {
	d.mu.Lock()
	delete(d.streams, st.id)
	for id, owner := range d.jobOwner {
		if owner == st {
			delete(d.jobOwner, id)
		}
	}
	d.mu.Unlock()

	d.logger.Info("worker disconnected", "worker", st.workerID, "queue", st.queue)
}

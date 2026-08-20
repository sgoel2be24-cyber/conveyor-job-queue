// Package broker implements Conveyor's durable job store and its RPC surface.
package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/job"
	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/metrics"
	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/wal"
)

const (
	walDirName      = "wal"
	snapshotDirName = "snapshots"
	snapshotPrefix  = "snapshot-"
	snapshotSuffix  = ".json"

	// snapshotsKept bounds how many old snapshots are retained. Keeping more
	// than one means a snapshot that turns out to be unreadable does not cost
	// us the whole recovery.
	snapshotsKept = 2

	// defaultEpochBlockSize is how many fencing tokens are claimed per durable
	// reservation. See reserveEpochsLocked.
	defaultEpochBlockSize = 1 << 20
)

var (
	// ErrQueueRequired is returned when a submission omits its queue.
	ErrQueueRequired = errors.New("broker: queue is required")
	// ErrJobNotFound is returned when an operation names an unknown job.
	ErrJobNotFound = errors.New("broker: job not found")
	// ErrNotDeadLettered is returned when replaying a job that is not in the
	// dead-letter queue.
	ErrNotDeadLettered = errors.New("broker: job is not dead-lettered")
)

// Config parameterizes a Store.
type Config struct {
	// Dir roots the WAL and snapshot directories.
	Dir string
	// BackoffBase and BackoffCap bound the retry delay. Zero means the defaults
	// in the job package.
	BackoffBase time.Duration
	BackoffCap  time.Duration
	// EpochBlockSize is how many fencing tokens to claim per durable
	// reservation. Zero means defaultEpochBlockSize.
	EpochBlockSize uint64
	// WALSegmentSize is the size at which log segments rotate. Zero means the
	// wal package default.
	WALSegmentSize int64
}

// Store is the durable job store: a write-ahead log plus the in-memory state
// rebuilt from it.
//
// Mutations that a caller is waiting on -- submissions, acks, failures -- are
// flushed to stable storage before they are applied in memory and
// acknowledged. Leases are not: see Lease.
type Store struct {
	dir     string
	snapDir string

	backoffBase    time.Duration
	backoffCap     time.Duration
	epochBlockSize uint64

	commit *committer

	mu      sync.RWMutex
	log     *wal.WAL
	jobs    map[string]*job.Job
	idem    map[string]string   // queue \x00 idempotency-key -> job ID
	leased  map[string]struct{} // job IDs currently out on lease
	index   *scheduleIndex
	lastLSN uint64

	// failed is set when a flush to stable storage fails, which poisons the
	// store. See commitDurable.
	failed error

	// nextEpoch is the next fencing token to hand out; epochReservedUpTo is the
	// highest one the log says we may hand out without another reservation.
	nextEpoch         uint64
	epochReservedUpTo uint64

	notifyCh chan struct{}
}

// OpenStore opens the store rooted at dir with default settings.
func OpenStore(dir string) (*Store, error) { return Open(Config{Dir: dir}) }

// Open opens the store described by cfg, recovering state from the newest
// usable snapshot plus any WAL records written after it.
func Open(cfg Config) (*Store, error) {
	if cfg.Dir == "" {
		return nil, errors.New("broker: data dir is required")
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = job.DefaultBackoffBase
	}
	if cfg.BackoffCap <= 0 {
		cfg.BackoffCap = job.DefaultBackoffCap
	}
	if cfg.EpochBlockSize == 0 {
		cfg.EpochBlockSize = defaultEpochBlockSize
	}

	snapDir := filepath.Join(cfg.Dir, snapshotDirName)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return nil, fmt.Errorf("broker: create snapshot dir: %w", err)
	}

	log, err := wal.Open(wal.Options{
		Dir:            filepath.Join(cfg.Dir, walDirName),
		MaxSegmentSize: cfg.WALSegmentSize,
	})
	if err != nil {
		return nil, err
	}

	s := &Store{
		dir:            cfg.Dir,
		snapDir:        snapDir,
		backoffBase:    cfg.BackoffBase,
		backoffCap:     cfg.BackoffCap,
		epochBlockSize: cfg.EpochBlockSize,
		log:            log,
		jobs:           make(map[string]*job.Job),
		idem:           make(map[string]string),
		leased:         make(map[string]struct{}),
		index:          newScheduleIndex(),
		notifyCh:       make(chan struct{}, 1),
		commit:         newCommitter(log),
	}
	recoveryStart := time.Now()

	snapLSN, err := s.loadLatestSnapshot()
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	s.lastLSN = snapLSN

	err = log.Replay(snapLSN+1, func(lsn uint64, payload []byte) error {
		e, err := decodeEvent(payload)
		if err != nil {
			return fmt.Errorf("lsn %d: %w", lsn, err)
		}
		if err := s.apply(e); err != nil {
			return fmt.Errorf("lsn %d: %w", lsn, err)
		}
		s.lastLSN = lsn
		return nil
	})
	if err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("broker: replay: %w", err)
	}

	now := time.Now().UTC()
	s.recoverLeasedLocked(now)
	s.rebuildIndexLocked(now)

	// Any token up to epochReservedUpTo might already have been handed out
	// before the crash, so resume strictly above it. The first lease will
	// reserve the next block.
	s.nextEpoch = s.epochReservedUpTo + 1

	metrics.RecoveryDuration.Set(time.Since(recoveryStart).Seconds())
	metrics.RecoveredJobs.Set(float64(len(s.jobs)))

	return s, nil
}

// recordLocked appends an event to the log and applies it in memory.
//
// The record is written but NOT yet durable. The caller must release s.mu and
// then call commitDurable before reporting success to anyone -- that split is
// the whole point, because it lets concurrent callers share one flush instead
// of queueing behind each other's.
//
// Callers must hold s.mu.
func (s *Store) recordLocked(e *event) error {
	if s.failed != nil {
		return s.failed
	}
	payload, err := encodeEvent(e)
	if err != nil {
		return err
	}
	lsn, err := s.log.Append(payload)
	if err != nil {
		return fmt.Errorf("broker: append: %w", err)
	}
	if err := s.apply(e); err != nil {
		return err
	}
	s.lastLSN = lsn
	return nil
}

// commitDurable flushes the log, sharing the cost with anyone else waiting.
//
// A failed flush poisons the store: every later operation fails too, and the
// broker stops accepting work rather than carrying on. That is deliberate. Once
// a flush has failed there is no way to learn which records reached the disk --
// the operating system may have already discarded the dirty pages, so a retry
// can report success while the data is gone. This is the lesson of the
// PostgreSQL "fsyncgate" discussion, and the only safe response is to stop and
// let recovery re-derive state from what is actually on disk.
//
// Callers must NOT hold s.mu.
func (s *Store) commitDurable() error {
	err := s.commit.commit()
	if err == nil {
		return nil
	}

	s.mu.Lock()
	if s.failed == nil {
		s.failed = fmt.Errorf("broker: durability lost, refusing further writes: %w", err)
	}
	err = s.failed
	s.mu.Unlock()
	return err
}

// Failed reports the error that poisoned the store, or nil if it is healthy.
func (s *Store) Failed() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.failed
}

// apply mutates in-memory state. It must be deterministic and must not touch
// disk or a clock: it runs both for live writes and for every record during
// replay, and the two paths have to agree exactly. Anything time- or
// randomness-dependent is resolved before the event is written and carried in
// the event itself.
//
// apply deliberately does not maintain the schedule index. That index is
// derived state, rebuilt wholesale after replay (see rebuildIndexLocked) so
// that replaying a long history does not accumulate an entry per transition.
func (s *Store) apply(e *event) error {
	switch e.Type {
	case eventSubmit:
		if e.Job == nil {
			return errors.New("submit event has no job")
		}
		j := e.Job
		s.jobs[j.ID] = j
		if j.IdempotencyKey != "" {
			s.idem[idemKey(j.Queue, j.IdempotencyKey)] = j.ID
		}
		return nil

	case eventLease:
		j, ok := s.jobs[e.JobID]
		if !ok {
			return fmt.Errorf("lease for unknown job %s", e.JobID)
		}
		j.State = job.StateLeased
		j.Epoch = e.Epoch
		j.LeasedBy = e.WorkerID
		j.LeaseExpiresAt = e.LeaseExpiresAt
		j.Attempt = e.Attempt
		s.leased[j.ID] = struct{}{}
		return nil

	case eventAck:
		j, ok := s.jobs[e.JobID]
		if !ok {
			return fmt.Errorf("ack for unknown job %s", e.JobID)
		}
		j.State = job.StateDone
		j.LeasedBy = ""
		j.LeaseExpiresAt = time.Time{}
		delete(s.leased, j.ID)
		return nil

	case eventFail:
		j, ok := s.jobs[e.JobID]
		if !ok {
			return fmt.Errorf("fail for unknown job %s", e.JobID)
		}
		j.State = e.NextState
		j.Attempt = e.Attempt
		j.EligibleAt = e.EligibleAt
		j.LastError = e.Reason
		j.LeasedBy = ""
		j.LeaseExpiresAt = time.Time{}
		delete(s.leased, j.ID)
		return nil

	case eventReplayJob:
		j, ok := s.jobs[e.JobID]
		if !ok {
			return fmt.Errorf("replay for unknown job %s", e.JobID)
		}
		j.State = job.StatePending
		j.Attempt = 0
		j.EligibleAt = e.EligibleAt
		j.LastError = ""
		j.LeasedBy = ""
		j.LeaseExpiresAt = time.Time{}
		delete(s.leased, j.ID)
		return nil

	case eventEpochReserve:
		s.epochReservedUpTo = e.ReservedUpTo
		return nil

	default:
		return fmt.Errorf("unknown event type %q", e.Type)
	}
}

// recoverLeasedLocked releases every lease that was outstanding when the
// previous process died.
//
// A worker still holding one of these leases is, by definition, talking to a
// process that no longer exists, so the job must become available again --
// redelivering it is exactly the at-least-once contract. The job keeps the
// attempt that was charged when it was leased: a broker crash is not the job's
// fault, but declining to count it would let a payload that reliably kills the
// broker be redelivered forever.
//
// This is not logged. It is a pure function of the replayed state, so a crash
// during recovery simply produces the same result next time.
func (s *Store) recoverLeasedLocked(now time.Time) {
	for _, j := range s.jobs {
		if j.State != job.StateLeased {
			continue
		}
		j.State = job.StateRetryWait
		j.LeasedBy = ""
		j.LeaseExpiresAt = time.Time{}
		j.EligibleAt = now
		j.LastError = "broker restarted while job was leased"
	}
	s.leased = make(map[string]struct{})
}

func (s *Store) rebuildIndexLocked(now time.Time) {
	s.index = newScheduleIndex()
	for _, j := range s.jobs {
		if j.State.Leasable() {
			s.index.push(j, now)
		}
	}
}

// Notify returns a channel that receives when work may have become available.
// Sends are non-blocking and coalesced, so a reader that is busy misses nothing
// beyond a redundant wakeup.
func (s *Store) Notify() <-chan struct{} { return s.notifyCh }

func (s *Store) signal() {
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

// ---------------------------------------------------------------- fencing ---

// nextEpochLocked returns the next fencing token.
func (s *Store) nextEpochLocked() (uint64, error) {
	if s.nextEpoch > s.epochReservedUpTo {
		if err := s.reserveEpochsLocked(); err != nil {
			return 0, err
		}
	}
	e := s.nextEpoch
	s.nextEpoch++
	return e, nil
}

// reserveEpochsLocked durably claims the next block of fencing tokens.
//
// Fencing only works if tokens never repeat across a crash. The obvious way to
// guarantee that is to flush every lease, but a lease is the most frequent
// write in the system and flushing costs milliseconds -- it would cap the whole
// broker at a few hundred leases a second.
//
// Instead we log, once, that everything up to some ceiling may be handed out,
// and then hand tokens out from that block for free. Recovery resumes above the
// last recorded ceiling, so tokens issued before a crash can never be issued
// again -- even the ones whose lease records never reached the disk. One flush
// per block (a million tokens by default) instead of one per lease.
//
// This is the same trick as a HiLo key allocator, and the same reason Raft has
// a persistent term: cheap monotonicity across restarts.
func (s *Store) reserveEpochsLocked() error {
	e := &event{Type: eventEpochReserve, ReservedUpTo: s.nextEpoch + s.epochBlockSize - 1}
	payload, err := encodeEvent(e)
	if err != nil {
		return err
	}
	lsn, err := s.log.AppendSync(payload)
	if err != nil {
		return fmt.Errorf("broker: reserve epochs: %w", err)
	}
	if err := s.apply(e); err != nil {
		return err
	}
	s.lastLSN = lsn
	return nil
}

// holdsLease reports whether a message carrying this epoch comes from the
// worker that currently holds the job.
//
// Both halves matter. The epoch check rejects a worker whose lease was
// reclaimed and reissued to someone else. The state check rejects a worker
// whose lease was reclaimed and *not* yet reissued -- a reclaimed job keeps the
// epoch of the lease that timed out, so epoch equality alone would accept a
// zombie's Ack and mark work done that nobody is doing.
func holdsLease(j *job.Job, epoch uint64) bool {
	return j.State == job.StateLeased && j.Epoch == epoch
}

// ----------------------------------------------------------- transitions ---

// SubmitParams describes a job to enqueue.
type SubmitParams struct {
	Queue          string
	Handler        string
	Payload        []byte
	IdempotencyKey string
	Priority       job.Priority
	MaxRetries     int
	Delay          time.Duration
}

// Submit durably enqueues a job, returning it along with whether an existing
// job was returned instead because the idempotency key already matched one.
func (s *Store) Submit(p SubmitParams) (*job.Job, bool, error) {
	if strings.TrimSpace(p.Queue) == "" {
		return nil, false, ErrQueueRequired
	}
	if p.Handler == "" {
		p.Handler = "shell"
	}
	if p.MaxRetries < 0 {
		p.MaxRetries = 0
	}

	s.mu.Lock()
	if s.failed != nil {
		err := s.failed
		s.mu.Unlock()
		return nil, false, err
	}

	if p.IdempotencyKey != "" {
		if id, ok := s.idem[idemKey(p.Queue, p.IdempotencyKey)]; ok {
			existing := s.jobs[id].Clone()
			s.mu.Unlock()
			metrics.JobsDeduplicated.WithLabelValues(p.Queue).Inc()
			return existing, true, nil
		}
	}

	now := time.Now().UTC()
	j := &job.Job{
		ID:             job.NewID(),
		Queue:          p.Queue,
		Handler:        p.Handler,
		Payload:        p.Payload,
		IdempotencyKey: p.IdempotencyKey,
		Priority:       p.Priority,
		MaxRetries:     p.MaxRetries,
		State:          job.StatePending,
		EnqueuedAt:     now,
		EligibleAt:     now.Add(p.Delay),
	}

	if err := s.recordLocked(&event{Type: eventSubmit, Job: j}); err != nil {
		s.mu.Unlock()
		return nil, false, err
	}
	s.index.push(j, now)
	accepted := j.Clone()
	s.mu.Unlock()

	// The flush happens with s.mu released, so concurrent submitters queue on
	// the disk together rather than behind each other's locks -- see committer.
	if err := s.commitDurable(); err != nil {
		return nil, false, err
	}

	metrics.JobsSubmitted.WithLabelValues(p.Queue).Inc()
	s.signal()
	return accepted, false, nil
}

// Lease hands the next eligible job on a queue to a worker, or returns nil if
// there is nothing to run. dur is how long the worker may hold it before the
// broker reclaims it.
//
// The lease record is appended but deliberately *not* flushed. Losing it in a
// crash costs nothing: recovery releases every outstanding lease anyway
// (recoverLeasedLocked), so the job simply becomes available again, which
// at-least-once delivery already permits. Fencing tokens stay sound across that
// loss because they are reserved separately -- see reserveEpochsLocked.
//
// Writes to a file are ordered, so the flush performed by a later Ack or fail
// also makes any lease records before it durable, for free.
func (s *Store) Lease(queue, workerID string, dur time.Duration) (*job.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	s.index.promote(queue, now)

	for {
		cand, ok := s.index.pop(queue)
		if !ok {
			return nil, nil
		}
		j, exists := s.jobs[cand.id]
		switch {
		case !exists, !j.State.Leasable():
			continue // the job moved on after this entry was filed
		case !cand.eligibleAt.Equal(j.EligibleAt):
			continue // superseded: a newer entry for this job is already filed
		case j.EligibleAt.After(now):
			// Defensive; promote should not have moved this one. Re-file it as
			// delayed rather than dropping it. This cannot loop, because the
			// ready heap only shrinks here.
			s.index.push(j, now)
			continue
		}

		epoch, err := s.nextEpochLocked()
		if err != nil {
			s.index.push(j, now)
			return nil, err
		}

		// How long this job sat waiting once it was allowed to run. Measured
		// from EligibleAt rather than EnqueuedAt so a deliberate delay or a
		// retry backoff is not counted as queueing latency.
		waited := now.Sub(j.EligibleAt).Seconds()

		if err := s.recordLocked(&event{
			Type:           eventLease,
			JobID:          j.ID,
			Epoch:          epoch,
			WorkerID:       workerID,
			LeaseExpiresAt: now.Add(dur),
			Attempt:        j.Attempt + 1,
		}); err != nil {
			s.index.push(j, now)
			return nil, err
		}

		metrics.JobsLeased.WithLabelValues(queue).Inc()
		metrics.DispatchDelay.WithLabelValues(queue).Observe(waited)
		return j.Clone(), nil
	}
}

// Ack marks a leased job complete. It reports false, without error, when the
// caller no longer holds the lease -- the fencing check.
func (s *Store) Ack(jobID string, epoch uint64) (bool, error) {
	s.mu.Lock()

	j, ok := s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return false, ErrJobNotFound
	}
	if !holdsLease(j, epoch) {
		s.mu.Unlock()
		metrics.FencedRequests.WithLabelValues("ack").Inc()
		return false, nil
	}

	queue := j.Queue
	if err := s.recordLocked(&event{Type: eventAck, JobID: jobID, Epoch: epoch}); err != nil {
		s.mu.Unlock()
		return false, err
	}
	s.mu.Unlock()

	if err := s.commitDurable(); err != nil {
		return false, err
	}

	metrics.JobsCompleted.WithLabelValues(queue).Inc()
	s.signal()
	return true, nil
}

// Nack reports a failed delivery. It returns whether the caller still held the
// lease, and whether the job was dead-lettered as a result.
func (s *Store) Nack(jobID string, epoch uint64, reason string) (accepted, deadLettered bool, err error) {
	s.mu.Lock()

	j, ok := s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return false, false, ErrJobNotFound
	}
	if !holdsLease(j, epoch) {
		s.mu.Unlock()
		metrics.FencedRequests.WithLabelValues("nack").Inc()
		return false, false, nil
	}
	if reason == "" {
		reason = "handler reported failure"
	}

	queue := j.Queue
	deadLettered, err = s.failLocked(j, reason, false, time.Now().UTC())
	if err != nil {
		s.mu.Unlock()
		return false, false, err
	}
	s.mu.Unlock()

	if err := s.commitDurable(); err != nil {
		return false, false, err
	}

	metrics.JobsFailed.WithLabelValues(queue, metrics.CauseHandler).Inc()
	if deadLettered {
		metrics.JobsDeadLettered.WithLabelValues(queue).Inc()
	}
	s.signal()
	return true, deadLettered, nil
}

// failLocked takes a job off a worker after a failed delivery, either
// scheduling a retry or dead-lettering it.
//
// This is the only path for both an explicit Nack and a lease that timed out,
// and that is deliberate. The two must consume the retry budget identically: if
// a timeout did not count as an attempt, a job whose handler reliably outlives
// its lease -- a poison payload, or a handler bug -- would cycle
// leased -> timeout -> leased forever, never reach the dead-letter queue, and
// occupy a worker slot on every pass. Routing both through one function makes
// that invariant structural rather than something two call sites must remember.
func (s *Store) failLocked(j *job.Job, reason string, timeout bool, now time.Time) (bool, error) {
	// The attempt was charged when the job was leased.
	attempt := j.Attempt

	next := job.StateRetryWait
	eligible := now.Add(job.Backoff(attempt, s.backoffBase, s.backoffCap))
	if attempt > j.MaxRetries {
		next = job.StateDeadLetter
		eligible = now
	}

	// Written but not yet flushed: whoever called us commits once we return, so
	// a sweep that reclaims many leases pays for a single flush rather than one
	// per job.
	if err := s.recordLocked(&event{
		Type:       eventFail,
		JobID:      j.ID,
		Epoch:      j.Epoch,
		Attempt:    attempt,
		Reason:     reason,
		NextState:  next,
		EligibleAt: eligible,
		Timeout:    timeout,
	}); err != nil {
		return false, err
	}

	if next == job.StateRetryWait {
		s.index.push(j, now)
	}
	return next == job.StateDeadLetter, nil
}

// Heartbeat extends a lease. It reports false when the caller no longer holds
// it, which tells a worker it has been fenced and should stop working.
//
// This writes nothing to the log. A lease expiry is not durable state --
// recovery releases every lease regardless of when it was due -- so there is
// nothing worth persisting, which is what makes heartbeats cheap enough to send
// often.
func (s *Store) Heartbeat(jobID string, epoch uint64, dur time.Duration) (bool, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[jobID]
	if !ok {
		return false, time.Time{}, ErrJobNotFound
	}
	if !holdsLease(j, epoch) {
		return false, time.Time{}, nil
	}
	j.LeaseExpiresAt = time.Now().UTC().Add(dur)
	return true, j.LeaseExpiresAt, nil
}

// ReclaimExpired releases leases whose deadline has passed, treating each one
// exactly as an explicit failure would be treated.
//
// The scan walks only jobs currently out on lease, which is bounded by the
// number of workers times their concurrency -- hundreds, typically -- not by
// the size of the queue.
func (s *Store) ReclaimExpired(now time.Time) ([]string, error) {
	s.mu.Lock()

	var (
		reclaimed    []string
		queues       []string
		deadLettered []string
	)
	for id := range s.leased {
		j, ok := s.jobs[id]
		if !ok || j.State != job.StateLeased {
			delete(s.leased, id) // bookkeeping drift; nothing to reclaim
			continue
		}
		if j.LeaseExpiresAt.After(now) {
			continue
		}
		queue := j.Queue
		dead, err := s.failLocked(j, "lease expired", true, now)
		if err != nil {
			s.mu.Unlock()
			return reclaimed, err
		}
		reclaimed = append(reclaimed, id)
		queues = append(queues, queue)
		if dead {
			deadLettered = append(deadLettered, queue)
		}
	}
	s.mu.Unlock()

	if len(reclaimed) == 0 {
		return nil, nil
	}

	// One flush for the whole sweep, however many leases it took back.
	if err := s.commitDurable(); err != nil {
		return reclaimed, err
	}

	for _, queue := range queues {
		metrics.JobsFailed.WithLabelValues(queue, metrics.CauseTimeout).Inc()
	}
	for _, queue := range deadLettered {
		metrics.JobsDeadLettered.WithLabelValues(queue).Inc()
	}
	s.signal()
	return reclaimed, nil
}

// ReplayJob returns a dead-lettered job to its queue with a fresh retry budget.
func (s *Store) ReplayJob(jobID string) error {
	s.mu.Lock()

	j, ok := s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return ErrJobNotFound
	}
	if j.State != job.StateDeadLetter {
		s.mu.Unlock()
		return ErrNotDeadLettered
	}

	now := time.Now().UTC()
	queue := j.Queue
	if err := s.recordLocked(&event{Type: eventReplayJob, JobID: jobID, EligibleAt: now}); err != nil {
		s.mu.Unlock()
		return err
	}
	s.index.push(j, now)
	s.mu.Unlock()

	if err := s.commitDurable(); err != nil {
		return err
	}

	metrics.JobsReplayed.WithLabelValues(queue).Inc()
	s.signal()
	return nil
}

// ------------------------------------------------------------- queries ---

// Get returns a job by ID.
func (s *Store) Get(id string) (*job.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	return j.Clone(), ok
}

// ListJobs returns jobs in a given state, newest first. A zero state matches
// any state, an empty queue matches any queue, and a limit of zero means no
// limit.
func (s *Store) ListJobs(queue string, state job.State, limit int) []*job.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []*job.Job
	for _, j := range s.jobs {
		if queue != "" && j.Queue != queue {
			continue
		}
		if state != job.StateUnspecified && j.State != state {
			continue
		}
		out = append(out, j.Clone())
	}
	sort.Slice(out, func(i, k int) bool { return out[i].ID > out[k].ID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// QueueStats holds per-queue job counts by state.
type QueueStats struct {
	Queue      string
	Pending    int64
	Leased     int64
	RetryWait  int64
	Done       int64
	DeadLetter int64
}

// Stats summarizes the store.
type Stats struct {
	Queues    []QueueStats
	TotalJobs int64
	LastLSN   uint64
}

// Stats reports per-queue counts, optionally restricted to one queue.
//
// This walks every job on each call. That is fine at the scale a single broker
// holds in memory, and keeping counters incrementally correct across replay,
// snapshots, and every transition is a bug surface not worth buying yet.
func (s *Store) Stats(queue string) Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	byQueue := make(map[string]*QueueStats)
	var total int64
	for _, j := range s.jobs {
		if queue != "" && j.Queue != queue {
			continue
		}
		total++
		qs, ok := byQueue[j.Queue]
		if !ok {
			qs = &QueueStats{Queue: j.Queue}
			byQueue[j.Queue] = qs
		}
		switch j.State {
		case job.StatePending:
			qs.Pending++
		case job.StateLeased:
			qs.Leased++
		case job.StateRetryWait:
			qs.RetryWait++
		case job.StateDone:
			qs.Done++
		case job.StateDeadLetter:
			qs.DeadLetter++
		}
	}

	out := Stats{TotalJobs: total, LastLSN: s.lastLSN}
	for _, qs := range byQueue {
		out.Queues = append(out.Queues, *qs)
	}
	sort.Slice(out.Queues, func(i, j int) bool { return out.Queues[i].Queue < out.Queues[j].Queue })
	return out
}

// NextDelayedDeadline reports when the soonest delayed or retrying job becomes
// eligible, so a caller can sleep until then rather than poll.
func (s *Store) NextDelayedDeadline() (time.Time, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.nextDeadline()
}

// ------------------------------------------------------------ snapshots ---

// snapshot is the serialized form of the in-memory job set. The idempotency map
// and schedule index are not stored: both are derived from the jobs on load, so
// they cannot drift from them.
type snapshot struct {
	LastLSN      uint64     `json:"last_lsn"`
	ReservedUpTo uint64     `json:"epoch_reserved_up_to"`
	Jobs         []*job.Job `json:"jobs"`
}

// Snapshot durably records current state and then drops the WAL segments it
// covers.
//
// Ordering matters: the snapshot must be fully on stable storage before any
// segment is deleted, or a crash in between would leave neither a snapshot nor
// the records needed to rebuild what it replaced.
func (s *Store) Snapshot() error {
	s.mu.RLock()
	snap := snapshot{
		LastLSN:      s.lastLSN,
		ReservedUpTo: s.epochReservedUpTo,
		Jobs:         make([]*job.Job, 0, len(s.jobs)),
	}
	for _, j := range s.jobs {
		snap.Jobs = append(snap.Jobs, j)
	}
	s.mu.RUnlock()

	sort.Slice(snap.Jobs, func(i, j int) bool { return snap.Jobs[i].ID < snap.Jobs[j].ID })

	data, err := json.Marshal(&snap)
	if err != nil {
		return fmt.Errorf("broker: encode snapshot: %w", err)
	}

	final := filepath.Join(s.snapDir, fmt.Sprintf("%s%020d%s", snapshotPrefix, snap.LastLSN, snapshotSuffix))
	tmp := final + ".tmp"

	// Write to a temp file, flush it, then rename: readers only ever see a
	// complete snapshot, because rename is atomic and the contents were already
	// durable before the name existed.
	if err := writeFileSync(tmp, data); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("broker: rename snapshot: %w", err)
	}
	if err := syncDir(s.snapDir); err != nil {
		return fmt.Errorf("broker: sync snapshot dir: %w", err)
	}

	if err := s.log.TruncateBefore(snap.LastLSN + 1); err != nil {
		return err
	}
	return s.pruneSnapshots()
}

// loadLatestSnapshot restores the newest readable snapshot and returns the LSN
// it covers, or 0 if there is none.
func (s *Store) loadLatestSnapshot() (uint64, error) {
	names, err := listSnapshots(s.snapDir)
	if err != nil {
		return 0, err
	}

	// Newest first, falling back to older ones: a snapshot truncated by a crash
	// mid-write should cost us replay time, not the whole recovery.
	for i := len(names) - 1; i >= 0; i-- {
		data, err := os.ReadFile(filepath.Join(s.snapDir, names[i]))
		if err != nil {
			continue
		}
		var snap snapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			continue
		}
		for _, j := range snap.Jobs {
			s.jobs[j.ID] = j
			if j.IdempotencyKey != "" {
				s.idem[idemKey(j.Queue, j.IdempotencyKey)] = j.ID
			}
			if j.State == job.StateLeased {
				s.leased[j.ID] = struct{}{}
			}
		}
		s.epochReservedUpTo = snap.ReservedUpTo
		return snap.LastLSN, nil
	}
	return 0, nil
}

func (s *Store) pruneSnapshots() error {
	names, err := listSnapshots(s.snapDir)
	if err != nil {
		return err
	}
	if len(names) <= snapshotsKept {
		return nil
	}
	for _, name := range names[:len(names)-snapshotsKept] {
		if err := os.Remove(filepath.Join(s.snapDir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("broker: prune snapshot: %w", err)
		}
	}
	return nil
}

// Close flushes and closes the underlying log.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.Close()
}

// --------------------------------------------------------------- helpers ---

// listSnapshots returns snapshot filenames sorted oldest to newest.
func listSnapshots(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("broker: read snapshot dir: %w", err)
	}
	type entry struct {
		name string
		lsn  uint64
	}
	var found []entry
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, snapshotPrefix) || !strings.HasSuffix(name, snapshotSuffix) {
			continue
		}
		raw := strings.TrimSuffix(strings.TrimPrefix(name, snapshotPrefix), snapshotSuffix)
		lsn, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			continue
		}
		found = append(found, entry{name: name, lsn: lsn})
	}
	sort.Slice(found, func(i, j int) bool { return found[i].lsn < found[j].lsn })

	names := make([]string, len(found))
	for i, e := range found {
		names[i] = e.name
	}
	return names, nil
}

func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("broker: create %s: %w", path, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("broker: write %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("broker: sync %s: %w", path, err)
	}
	return f.Close()
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}

func idemKey(queue, key string) string { return queue + "\x00" + key }

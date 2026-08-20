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

	"conveyor/internal/job"
	"conveyor/internal/wal"
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
)

// ErrQueueRequired is returned when a submission omits its queue.
var ErrQueueRequired = errors.New("broker: queue is required")

// Store is the durable job store: a write-ahead log plus the in-memory index
// rebuilt from it.
//
// Every mutation is written to the WAL and flushed to stable storage *before*
// it is applied in memory and acknowledged to the caller. A job the store has
// acknowledged therefore survives a crash; a job whose submission was still in
// flight may not, which is the guarantee producers are given.
type Store struct {
	dir     string
	snapDir string

	mu      sync.RWMutex
	log     *wal.WAL
	jobs    map[string]*job.Job
	idem    map[string]string // queue \x00 idempotency-key -> job ID
	lastLSN uint64
}

// OpenStore opens the store rooted at dir, recovering state from the newest
// usable snapshot plus any WAL records written after it.
func OpenStore(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("broker: data dir is required")
	}
	snapDir := filepath.Join(dir, snapshotDirName)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return nil, fmt.Errorf("broker: create snapshot dir: %w", err)
	}

	log, err := wal.Open(wal.Options{Dir: filepath.Join(dir, walDirName)})
	if err != nil {
		return nil, err
	}

	s := &Store{
		dir:     dir,
		snapDir: snapDir,
		log:     log,
		jobs:    make(map[string]*job.Job),
		idem:    make(map[string]string),
	}

	snapLSN, err := s.loadLatestSnapshot()
	if err != nil {
		log.Close()
		return nil, err
	}
	s.lastLSN = snapLSN

	// Replay everything the snapshot does not already account for.
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
		log.Close()
		return nil, fmt.Errorf("broker: replay: %w", err)
	}

	return s, nil
}

// apply mutates the in-memory index. It must be deterministic and must not
// touch disk: it runs both for live writes and for every record during replay,
// and the two paths have to agree exactly.
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
	default:
		return fmt.Errorf("unknown event type %q", e.Type)
	}
}

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
	defer s.mu.Unlock()

	if p.IdempotencyKey != "" {
		if id, ok := s.idem[idemKey(p.Queue, p.IdempotencyKey)]; ok {
			return s.jobs[id].Clone(), true, nil
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

	payload, err := encodeEvent(&event{Type: eventSubmit, Job: j})
	if err != nil {
		return nil, false, err
	}

	// Durable first, in-memory second: if the append fails, nothing has changed
	// and the caller learns the job was not accepted.
	//
	// The fsync happens under s.mu, which serializes submissions. That is the
	// throughput ceiling this design deliberately starts with, and the baseline
	// the group-commit work measures against (docs/DESIGN.md).
	lsn, err := s.log.AppendSync(payload)
	if err != nil {
		return nil, false, fmt.Errorf("broker: append: %w", err)
	}

	if err := s.apply(&event{Type: eventSubmit, Job: j}); err != nil {
		return nil, false, err
	}
	s.lastLSN = lsn
	return j.Clone(), false, nil
}

// Get returns a job by ID.
func (s *Store) Get(id string) (*job.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	return j.Clone(), ok
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

// snapshot is the serialized form of the in-memory index. The idempotency map
// is not stored: it is derived from the jobs on load, so the two cannot drift.
type snapshot struct {
	LastLSN uint64     `json:"last_lsn"`
	Jobs    []*job.Job `json:"jobs"`
}

// Snapshot durably records the current index and then drops the WAL segments it
// covers.
//
// Ordering matters: the snapshot must be fully on stable storage before any
// segment is deleted, or a crash in between would leave neither a snapshot nor
// the records needed to rebuild what it replaced.
func (s *Store) Snapshot() error {
	s.mu.RLock()
	snap := snapshot{LastLSN: s.lastLSN, Jobs: make([]*job.Job, 0, len(s.jobs))}
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
		path := filepath.Join(s.snapDir, names[i])
		data, err := os.ReadFile(path)
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
		}
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
		f.Close()
		return fmt.Errorf("broker: write %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("broker: sync %s: %w", path, err)
	}
	return f.Close()
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func idemKey(queue, key string) string { return queue + "\x00" + key }

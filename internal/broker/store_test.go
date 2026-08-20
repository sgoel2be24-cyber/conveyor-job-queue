package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/job"
)

func mustOpenStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return s
}

func submitN(t *testing.T, s *Store, queue string, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		j, dedup, err := s.Submit(SubmitParams{
			Queue:   queue,
			Payload: []byte(fmt.Sprintf("payload-%d", i)),
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		if dedup {
			t.Fatalf("submit %d was unexpectedly deduplicated", i)
		}
		ids = append(ids, j.ID)
	}
	return ids
}

// walSegments returns the store's WAL segment paths, oldest first.
func walSegments(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, walDirName, "*.wal"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	return matches
}

func TestSubmitRequiresQueue(t *testing.T) {
	s := mustOpenStore(t, t.TempDir())
	defer s.Close()

	if _, _, err := s.Submit(SubmitParams{Payload: []byte("x")}); err != ErrQueueRequired {
		t.Errorf("Submit without queue = %v, want %v", err, ErrQueueRequired)
	}
}

func TestSubmitDefaultsAndState(t *testing.T) {
	s := mustOpenStore(t, t.TempDir())
	defer s.Close()

	j, _, err := s.Submit(SubmitParams{Queue: "emails", Payload: []byte("hi")})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if j.State != job.StatePending {
		t.Errorf("state = %s, want pending", j.State)
	}
	if j.Handler != "shell" {
		t.Errorf("handler = %q, want the shell default", j.Handler)
	}
	if j.Attempt != 0 {
		t.Errorf("attempt = %d, want 0", j.Attempt)
	}
	if j.EligibleAt.Before(j.EnqueuedAt) {
		t.Error("eligible_at precedes enqueued_at")
	}
}

// TestRecoverAfterClose is the basic durability claim: every acknowledged job
// is still there after the process goes away and comes back.
func TestRecoverAfterClose(t *testing.T) {
	dir := t.TempDir()

	s := mustOpenStore(t, dir)
	ids := submitN(t, s, "emails", 500)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2 := mustOpenStore(t, dir)
	defer s2.Close()

	for _, id := range ids {
		if _, ok := s2.Get(id); !ok {
			t.Fatalf("job %s lost across restart", id)
		}
	}
	stats := s2.Stats("")
	if stats.TotalJobs != int64(len(ids)) {
		t.Errorf("recovered %d jobs, want %d", stats.TotalJobs, len(ids))
	}
	if len(stats.Queues) != 1 || stats.Queues[0].Pending != int64(len(ids)) {
		t.Errorf("stats = %+v, want all %d jobs pending on one queue", stats.Queues, len(ids))
	}
}

// TestRecoverAfterTornTail is the crash-safety claim at the store level: a
// process killed mid-append loses only the record it was in the middle of
// writing, and every job acknowledged before that survives.
func TestRecoverAfterTornTail(t *testing.T) {
	dir := t.TempDir()

	s := mustOpenStore(t, dir)
	ids := submitN(t, s, "emails", 50)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Chop a byte off the log, as a kill -9 mid-write would.
	segments := walSegments(t, dir)
	last := segments[len(segments)-1]
	info, err := os.Stat(last)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(last, info.Size()-1); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	s2 := mustOpenStore(t, dir)
	defer s2.Close()

	// Everything but the interrupted final submission must be intact.
	for _, id := range ids[:len(ids)-1] {
		if _, ok := s2.Get(id); !ok {
			t.Fatalf("job %s lost, but it was acknowledged before the crash", id)
		}
	}
	if _, ok := s2.Get(ids[len(ids)-1]); ok {
		t.Error("the torn final record was recovered, but it was never fully written")
	}
	if got := s2.Stats("").TotalJobs; got != int64(len(ids)-1) {
		t.Errorf("recovered %d jobs, want %d", got, len(ids)-1)
	}

	// The store must be writable again, and the new job must itself survive.
	j, _, err := s2.Submit(SubmitParams{Queue: "emails", Payload: []byte("after-crash")})
	if err != nil {
		t.Fatalf("submit after recovery: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s3 := mustOpenStore(t, dir)
	defer s3.Close()
	if _, ok := s3.Get(j.ID); !ok {
		t.Error("job submitted after crash recovery did not survive the next restart")
	}
}

func TestIdempotencyKeyDeduplicates(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenStore(t, dir)

	first, dedup, err := s.Submit(SubmitParams{Queue: "emails", IdempotencyKey: "order-42", Payload: []byte("a")})
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if dedup {
		t.Fatal("first submit reported as deduplicated")
	}

	second, dedup, err := s.Submit(SubmitParams{Queue: "emails", IdempotencyKey: "order-42", Payload: []byte("b")})
	if err != nil {
		t.Fatalf("second submit: %v", err)
	}
	if !dedup {
		t.Error("second submit with the same key was not deduplicated")
	}
	if second.ID != first.ID {
		t.Errorf("second submit created job %s, want the existing %s", second.ID, first.ID)
	}
	if got := s.Stats("").TotalJobs; got != 1 {
		t.Errorf("total jobs = %d, want 1", got)
	}

	// The same key on a different queue is a different job.
	other, dedup, err := s.Submit(SubmitParams{Queue: "sms", IdempotencyKey: "order-42", Payload: []byte("c")})
	if err != nil {
		t.Fatalf("cross-queue submit: %v", err)
	}
	if dedup || other.ID == first.ID {
		t.Error("idempotency keys leaked across queues")
	}

	// Dedup must still hold once the map is rebuilt from disk.
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s2 := mustOpenStore(t, dir)
	defer s2.Close()

	again, dedup, err := s2.Submit(SubmitParams{Queue: "emails", IdempotencyKey: "order-42", Payload: []byte("d")})
	if err != nil {
		t.Fatalf("submit after restart: %v", err)
	}
	if !dedup || again.ID != first.ID {
		t.Errorf("after restart, key order-42 produced %s (dedup=%v), want existing %s", again.ID, dedup, first.ID)
	}
}

// TestSnapshotTrimsLogAndPreservesState checks the half of durability that is
// easy to get wrong: state must survive recovery that reads a snapshot instead
// of replaying the records it replaced.
func TestSnapshotTrimsLogAndPreservesState(t *testing.T) {
	dir := t.TempDir()
	s := mustOpenStore(t, dir)

	before := submitN(t, s, "emails", 200)
	if err := s.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	after := submitN(t, s, "emails", 50)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	snaps, err := listSnapshots(filepath.Join(dir, snapshotDirName))
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	if len(snaps) == 0 {
		t.Fatal("no snapshot was written")
	}

	s2 := mustOpenStore(t, dir)
	defer s2.Close()

	for _, id := range append(append([]string{}, before...), after...) {
		if _, ok := s2.Get(id); !ok {
			t.Fatalf("job %s lost across snapshot + restart", id)
		}
	}
	if got, want := s2.Stats("").TotalJobs, int64(len(before)+len(after)); got != want {
		t.Errorf("recovered %d jobs, want %d", got, want)
	}
}

// TestSnapshotDeletesRedundantSegments verifies the log actually shrinks --
// without this, recovery time grows without bound no matter how many jobs have
// already reached a terminal state.
func TestSnapshotDeletesRedundantSegments(t *testing.T) {
	dir := t.TempDir()

	// Filling many segments at the default 16MiB size would take a while, so
	// shrink the segments instead of writing more.
	s, err := Open(Config{Dir: dir, WALSegmentSize: 4 << 10})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	submitN(t, s, "emails", 200)
	segmentsBefore := len(walSegments(t, dir))
	if segmentsBefore < 2 {
		t.Fatalf("got %d segments, want several so truncation has something to remove", segmentsBefore)
	}

	if err := s.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if segmentsAfter := len(walSegments(t, dir)); segmentsAfter >= segmentsBefore {
		t.Errorf("segments went from %d to %d, want the snapshot to have trimmed the log", segmentsBefore, segmentsAfter)
	}
}

func TestStatsPerQueue(t *testing.T) {
	s := mustOpenStore(t, t.TempDir())
	defer s.Close()

	submitN(t, s, "emails", 3)
	submitN(t, s, "sms", 2)

	all := s.Stats("")
	if len(all.Queues) != 2 {
		t.Fatalf("got %d queues, want 2", len(all.Queues))
	}
	if all.Queues[0].Queue != "emails" || all.Queues[0].Pending != 3 {
		t.Errorf("emails stats = %+v, want 3 pending", all.Queues[0])
	}
	if all.Queues[1].Queue != "sms" || all.Queues[1].Pending != 2 {
		t.Errorf("sms stats = %+v, want 2 pending", all.Queues[1])
	}
	if all.TotalJobs != 5 {
		t.Errorf("total = %d, want 5", all.TotalJobs)
	}

	only := s.Stats("sms")
	if len(only.Queues) != 1 || only.TotalJobs != 2 {
		t.Errorf("filtered stats = %+v, want just sms with 2 jobs", only)
	}
}

func TestGetReturnsACopy(t *testing.T) {
	s := mustOpenStore(t, t.TempDir())
	defer s.Close()

	j, _, err := s.Submit(SubmitParams{Queue: "emails", Payload: []byte("original")})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	got, ok := s.Get(j.ID)
	if !ok {
		t.Fatal("job not found")
	}
	got.Queue = "tampered"
	got.Payload[0] = 'X'

	again, _ := s.Get(j.ID)
	if again.Queue != "emails" {
		t.Error("mutating a returned job changed the store's copy")
	}
	if string(again.Payload) != "original" {
		t.Errorf("payload = %q, want the store's copy to be unchanged", again.Payload)
	}
}

package broker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/job"
	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/wal"
)

// TestConcurrentSubmissionsAreAllDurable is the correctness half of group
// commit: batching several submissions into one flush must not lose any of
// them. Every job the store acknowledged has to come back after a reopen.
func TestConcurrentSubmissionsAreAllDurable(t *testing.T) {
	dir := t.TempDir()
	s := testStore(t, dir)

	const (
		writers = 32
		each    = 25
	)

	var (
		mu  sync.Mutex
		ids []string
		wg  sync.WaitGroup
	)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			local := make([]string, 0, each)
			for i := 0; i < each; i++ {
				j, _, err := s.Submit(SubmitParams{
					Queue:   "emails",
					Payload: []byte(fmt.Sprintf("w%d-%d", w, i)),
				})
				if err != nil {
					t.Errorf("submit: %v", err)
					return
				}
				local = append(local, j.ID)
			}
			mu.Lock()
			ids = append(ids, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()

	if len(ids) != writers*each {
		t.Fatalf("acknowledged %d jobs, want %d", len(ids), writers*each)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := testStore(t, dir)
	defer reopened.Close()

	for _, id := range ids {
		if _, ok := reopened.Get(id); !ok {
			t.Fatalf("job %s was acknowledged but did not survive the restart", id)
		}
	}
	if got := reopened.Stats("").TotalJobs; got != int64(len(ids)) {
		t.Errorf("recovered %d jobs, want %d", got, len(ids))
	}
}

// TestGroupCommitSharesFlushes checks the performance half: concurrent
// submitters must actually share flushes rather than each paying for one.
//
// The assertion is deliberately loose -- the exact batch size depends on how
// the scheduler interleaves things -- but it is strict enough to fail if
// batching regresses to one record per flush, which is what happens whenever
// something reintroduces a lock held across the disk write.
func TestGroupCommitSharesFlushes(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	var flushes atomic.Int64
	s.commit.onFlush = func(int) { flushes.Add(1) }

	const (
		writers = 32
		each    = 20
		total   = writers * each
	)

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if _, _, err := s.Submit(SubmitParams{Queue: "q", Payload: []byte("x")}); err != nil {
					t.Errorf("submit: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	got := flushes.Load()
	if got == 0 {
		t.Fatal("no flushes recorded")
	}
	if got >= total {
		t.Errorf("%d submissions took %d flushes -- they are not sharing at all", total, got)
	}
	t.Logf("%d submissions in %d flushes (%.1f per flush)", total, got, float64(total)/float64(got))
}

// TestCommitFailurePoisonsStore covers the deliberate refusal to continue after
// a flush fails: once durability is in doubt, later writes must fail loudly
// rather than be silently accepted.
func TestCommitFailurePoisonsStore(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	// Point only the committer at a log that refuses to flush, leaving the
	// append path healthy. Closing the store's own log would fail the append
	// instead, which is a different -- and much less interesting -- failure:
	// nothing has been written, so nothing is in doubt. The case that has to
	// poison the store is the one where records were accepted and then could
	// not be made durable.
	broken, err := wal.Open(wal.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open scratch log: %v", err)
	}
	if err := broken.Close(); err != nil {
		t.Fatalf("close scratch log: %v", err)
	}
	s.commit.log = broken

	if _, _, err := s.Submit(SubmitParams{Queue: "q", Payload: []byte("x")}); err == nil {
		t.Fatal("submit reported success even though the flush failed")
	}
	if s.Failed() == nil {
		t.Error("store does not report itself failed after a flush error")
	}

	// Every later write must refuse too, rather than appearing to work.
	if _, _, err := s.Submit(SubmitParams{Queue: "q", Payload: []byte("y")}); err == nil {
		t.Error("a second submit succeeded on a poisoned store")
	}
}

// TestReclaimSweepCommitsOnce checks that taking back many expired leases costs
// one flush rather than one per job.
func TestReclaimSweepCommitsOnce(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	const n = 20
	for i := 0; i < n; i++ {
		submitOne(t, s, "q", 5)
	}
	for i := 0; i < n; i++ {
		expiredLease(t, s, "q", "worker-1")
	}

	var flushes atomic.Int64
	s.commit.onFlush = func(int) { flushes.Add(1) }

	reclaimed, err := s.ReclaimExpired(time.Now().UTC())
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(reclaimed) != n {
		t.Fatalf("reclaimed %d leases, want %d", len(reclaimed), n)
	}
	if got := flushes.Load(); got != 1 {
		t.Errorf("reclaiming %d leases took %d flushes, want 1", n, got)
	}
	for _, id := range reclaimed {
		j, _ := s.Get(id)
		if j.State != job.StateRetryWait {
			t.Errorf("job %s is %s after reclaim, want retry_wait", id, j.State)
		}
	}
}

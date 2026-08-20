package broker

import (
	"testing"
	"time"

	"conveyor/internal/job"
)

// testConfig makes retries effectively instant so tests can drive a job through
// its whole retry budget without sleeping, and keeps the epoch block small so
// the reservation path is exercised rather than skipped.
func testConfig(dir string) Config {
	return Config{
		Dir:            dir,
		BackoffBase:    time.Nanosecond,
		BackoffCap:     time.Microsecond,
		EpochBlockSize: 16,
	}
}

func testStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(testConfig(dir))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return s
}

func submitOne(t *testing.T, s *Store, queue string, maxRetries int) *job.Job {
	t.Helper()
	j, _, err := s.Submit(SubmitParams{Queue: queue, Payload: []byte("work"), MaxRetries: maxRetries})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return j
}

func mustLease(t *testing.T, s *Store, queue, workerID string, dur time.Duration) *job.Job {
	t.Helper()
	j, err := s.Lease(queue, workerID, dur)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if j == nil {
		t.Fatal("expected a job to be available for lease, got none")
	}
	return j
}

func expectNoLease(t *testing.T, s *Store, queue string) {
	t.Helper()
	j, err := s.Lease(queue, "probe", time.Minute)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if j != nil {
		t.Fatalf("expected nothing leasable, got %s in state %s", j.ID, j.State)
	}
}

// expiredLease leases a job whose deadline has already passed, so the next
// reclaim sweep picks it up without the test having to sleep.
func expiredLease(t *testing.T, s *Store, queue, workerID string) *job.Job {
	t.Helper()
	return mustLease(t, s, queue, workerID, -time.Millisecond)
}

func reclaim(t *testing.T, s *Store) []string {
	t.Helper()
	ids, err := s.ReclaimExpired(time.Now().UTC())
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	return ids
}

// TestZombieWorkerAckIsFenced is the headline correctness test.
//
// A worker that merely stalls -- a GC pause, a slow disk, a network hiccup --
// is indistinguishable from a dead one. Its lease expires, the job is handed to
// someone else, and then the "dead" worker wakes up and reports success for
// work that is no longer its own. Accepting that Ack would mark the job done
// while another worker is still running it.
func TestZombieWorkerAckIsFenced(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	submitOne(t, s, "emails", 5)
	zombie := expiredLease(t, s, "emails", "worker-1")

	if got := reclaim(t, s); len(got) != 1 {
		t.Fatalf("reclaimed %d leases, want 1", len(got))
	}

	// The stalled worker finally finishes and reports success.
	accepted, err := s.Ack(zombie.ID, zombie.Epoch)
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if accepted {
		t.Fatal("a reclaimed lease's Ack was accepted; the job would be marked done while another worker still owns it")
	}

	current, _ := s.Get(zombie.ID)
	if current.State == job.StateDone {
		t.Fatalf("job reached %s via a stale Ack", current.State)
	}
	if current.State != job.StateRetryWait {
		t.Errorf("state = %s, want retry_wait after reclaim", current.State)
	}
}

// TestFencingSurvivesReassignment covers the same race once the job is actually
// running somewhere else: the late Ack must lose, and the new holder's must win.
func TestFencingSurvivesReassignment(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	submitOne(t, s, "emails", 5)
	zombie := expiredLease(t, s, "emails", "worker-1")
	reclaim(t, s)

	fresh := mustLease(t, s, "emails", "worker-2", time.Minute)
	if fresh.ID != zombie.ID {
		t.Fatalf("expected the reclaimed job to be reissued, got a different one")
	}
	if fresh.Epoch <= zombie.Epoch {
		t.Fatalf("reissued epoch %d does not exceed the reclaimed epoch %d; the token is not a fence",
			fresh.Epoch, zombie.Epoch)
	}

	accepted, err := s.Ack(zombie.ID, zombie.Epoch)
	if err != nil {
		t.Fatalf("stale ack: %v", err)
	}
	if accepted {
		t.Error("stale worker's Ack was accepted while a newer worker held the lease")
	}

	accepted, err = s.Ack(fresh.ID, fresh.Epoch)
	if err != nil {
		t.Fatalf("current ack: %v", err)
	}
	if !accepted {
		t.Error("current lease holder's Ack was rejected")
	}
	if current, _ := s.Get(fresh.ID); current.State != job.StateDone {
		t.Errorf("state = %s, want done", current.State)
	}
}

// TestStaleNackAndHeartbeatAreFenced checks the other two lease-bearing calls,
// since a stale Nack would corrupt the retry budget just as badly as a stale Ack
// corrupts completion.
func TestStaleNackAndHeartbeatAreFenced(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	submitOne(t, s, "emails", 5)
	zombie := expiredLease(t, s, "emails", "worker-1")
	reclaim(t, s)
	fresh := mustLease(t, s, "emails", "worker-2", time.Minute)

	accepted, _, err := s.Nack(zombie.ID, zombie.Epoch, "stale failure")
	if err != nil {
		t.Fatalf("stale nack: %v", err)
	}
	if accepted {
		t.Error("stale Nack was accepted; it would have charged another worker's attempt")
	}

	accepted, _, err = s.Heartbeat(zombie.ID, zombie.Epoch, time.Minute)
	if err != nil {
		t.Fatalf("stale heartbeat: %v", err)
	}
	if accepted {
		t.Error("stale Heartbeat was accepted; the zombie would keep extending a lease it lost")
	}

	// A rejected heartbeat is how a worker learns it was fenced, so the current
	// holder must still get an acceptance.
	accepted, expiry, err := s.Heartbeat(fresh.ID, fresh.Epoch, time.Minute)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !accepted {
		t.Fatal("current holder's Heartbeat was rejected")
	}
	if !expiry.After(time.Now()) {
		t.Errorf("renewed expiry %s is not in the future", expiry)
	}
}

// TestLeaseTimeoutConsumesRetryBudget guards against a real liveness bug: if a
// reclaimed lease did not count as a delivery attempt, a job whose handler
// always outlives its lease -- a poison payload, or a hung dependency -- would
// cycle through workers forever, never dead-lettering and occupying a slot every
// time round.
func TestLeaseTimeoutConsumesRetryBudget(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	const maxRetries = 3
	submitted := submitOne(t, s, "emails", maxRetries)

	// Every delivery times out; nothing ever nacks explicitly.
	attempts := 0
	for i := 0; i < maxRetries+5; i++ {
		j, err := s.Lease("emails", "worker-1", -time.Millisecond)
		if err != nil {
			t.Fatalf("lease: %v", err)
		}
		if j == nil {
			break // dead-lettered, nothing left to hand out
		}
		attempts++
		reclaim(t, s)
	}

	final, _ := s.Get(submitted.ID)
	if final.State != job.StateDeadLetter {
		t.Fatalf("state = %s after %d timed-out deliveries, want dead_letter -- "+
			"a job that only ever times out must still exhaust its retry budget",
			final.State, attempts)
	}
	if want := maxRetries + 1; attempts != want {
		t.Errorf("job was delivered %d times, want %d (one initial attempt plus %d retries)",
			attempts, want, maxRetries)
	}
}

// TestTimeoutAndNackShareTheCounter checks the two failure paths are accounted
// identically, which is what makes the invariant above hold no matter how the
// failures are mixed.
func TestTimeoutAndNackShareTheCounter(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	const maxRetries = 3
	submitted := submitOne(t, s, "emails", maxRetries)

	// Alternate: timeout, nack, timeout, nack.
	for i := 0; i < maxRetries+1; i++ {
		j, err := s.Lease("emails", "worker-1", -time.Millisecond)
		if err != nil {
			t.Fatalf("lease: %v", err)
		}
		if j == nil {
			t.Fatalf("job became unleasable after %d attempts, before its budget was spent", i)
		}
		if j.Attempt != i+1 {
			t.Errorf("attempt %d recorded as %d", i+1, j.Attempt)
		}

		if i%2 == 0 {
			reclaim(t, s)
		} else if _, _, err := s.Nack(j.ID, j.Epoch, "boom"); err != nil {
			t.Fatalf("nack: %v", err)
		}
	}

	final, _ := s.Get(submitted.ID)
	if final.State != job.StateDeadLetter {
		t.Errorf("state = %s, want dead_letter after a mix of timeouts and nacks", final.State)
	}
	if final.Attempt != maxRetries+1 {
		t.Errorf("attempt = %d, want %d", final.Attempt, maxRetries+1)
	}
}

func TestMaxRetriesZeroDeadLettersOnFirstFailure(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	submitted := submitOne(t, s, "emails", 0)
	j := mustLease(t, s, "emails", "worker-1", time.Minute)

	accepted, deadLettered, err := s.Nack(j.ID, j.Epoch, "boom")
	if err != nil {
		t.Fatalf("nack: %v", err)
	}
	if !accepted {
		t.Fatal("nack rejected")
	}
	if !deadLettered {
		t.Error("job with max-retries=0 was not dead-lettered on its first failure")
	}
	if final, _ := s.Get(submitted.ID); final.State != job.StateDeadLetter {
		t.Errorf("state = %s, want dead_letter", final.State)
	}
	expectNoLease(t, s, "emails")
}

// TestEpochsNeverRepeatAcrossRestart is why lease records do not need to be
// flushed. A crash can lose the most recent lease records, but the tokens they
// carried must never be handed out again -- otherwise a worker still holding an
// old token would be indistinguishable from the current holder.
func TestEpochsNeverRepeatAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	s := testStore(t, dir)

	submitOne(t, s, "emails", 5)
	before := mustLease(t, s, "emails", "worker-1", time.Minute)

	// Close without snapshotting, so recovery has to reason from the log alone.
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s2 := testStore(t, dir)
	defer s2.Close()

	// Recovery releases leases whose holder was talking to the dead process.
	recovered, _ := s2.Get(before.ID)
	if recovered.State != job.StateRetryWait {
		t.Errorf("state after restart = %s, want retry_wait so the job can be redelivered", recovered.State)
	}

	// The pre-crash worker is still alive and reports success.
	accepted, err := s2.Ack(before.ID, before.Epoch)
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if accepted {
		t.Error("a lease held across a broker restart was still honored")
	}

	after := mustLease(t, s2, "emails", "worker-2", time.Minute)
	if after.Epoch <= before.Epoch {
		t.Fatalf("epoch after restart = %d, want greater than the pre-crash %d -- "+
			"tokens must never repeat, or fencing cannot tell the holders apart",
			after.Epoch, before.Epoch)
	}
}

func TestRecoveryMakesLeasedJobsAvailableAgain(t *testing.T) {
	dir := t.TempDir()
	s := testStore(t, dir)

	for i := 0; i < 5; i++ {
		submitOne(t, s, "emails", 5)
	}
	for i := 0; i < 5; i++ {
		mustLease(t, s, "emails", "worker-1", time.Hour)
	}
	if got := s.Stats("").Queues[0].Leased; got != 5 {
		t.Fatalf("leased = %d, want 5", got)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2 := testStore(t, dir)
	defer s2.Close()

	stats := s2.Stats("").Queues[0]
	if stats.Leased != 0 {
		t.Errorf("leased = %d after restart, want 0 -- those workers were talking to a dead process", stats.Leased)
	}
	if stats.RetryWait != 5 {
		t.Errorf("retry_wait = %d after restart, want 5", stats.RetryWait)
	}
	for i := 0; i < 5; i++ {
		mustLease(t, s2, "emails", "worker-2", time.Minute)
	}
}

func TestPriorityOrdersDispatch(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	mk := func(p job.Priority) string {
		j, _, err := s.Submit(SubmitParams{Queue: "q", Payload: []byte("x"), Priority: p})
		if err != nil {
			t.Fatalf("submit: %v", err)
		}
		return j.ID
	}
	// Submit in an order that a FIFO queue would get wrong.
	low := mk(job.PriorityLow)
	normal := mk(job.PriorityNormal)
	high := mk(job.PriorityHigh)

	for i, want := range []string{high, normal, low} {
		got := mustLease(t, s, "q", "worker-1", time.Minute)
		if got.ID != want {
			t.Fatalf("lease %d returned %s, want %s (priority order violated)", i, got.ID, want)
		}
	}
}

func TestDelayedJobIsNotLeasableYet(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	if _, _, err := s.Submit(SubmitParams{
		Queue:   "q",
		Payload: []byte("later"),
		Delay:   time.Hour,
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	expectNoLease(t, s, "q")

	deadline, ok := s.NextDelayedDeadline()
	if !ok {
		t.Fatal("NextDelayedDeadline reported nothing pending")
	}
	if !deadline.After(time.Now()) {
		t.Errorf("deadline %s is not in the future", deadline)
	}
}

func TestRetryBackoffHoldsJobBack(t *testing.T) {
	s, err := Open(Config{Dir: t.TempDir(), BackoffBase: time.Hour, BackoffCap: 2 * time.Hour})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	submitOne(t, s, "q", 5)
	j := mustLease(t, s, "q", "worker-1", time.Minute)
	if _, _, err := s.Nack(j.ID, j.Epoch, "boom"); err != nil {
		t.Fatalf("nack: %v", err)
	}

	// The job is waiting out its backoff, so it must not come straight back.
	expectNoLease(t, s, "q")
	if current, _ := s.Get(j.ID); current.State != job.StateRetryWait {
		t.Errorf("state = %s, want retry_wait", current.State)
	}
}

func TestReplayJobFromDeadLetter(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	submitted := submitOne(t, s, "q", 0)
	j := mustLease(t, s, "q", "worker-1", time.Minute)
	if _, _, err := s.Nack(j.ID, j.Epoch, "boom"); err != nil {
		t.Fatalf("nack: %v", err)
	}

	if err := s.ReplayJob(submitted.ID); err != nil {
		t.Fatalf("replay: %v", err)
	}

	replayed, _ := s.Get(submitted.ID)
	if replayed.State != job.StatePending {
		t.Errorf("state = %s, want pending", replayed.State)
	}
	if replayed.Attempt != 0 {
		t.Errorf("attempt = %d, want 0 -- a replayed job gets a fresh budget", replayed.Attempt)
	}
	if replayed.LastError != "" {
		t.Errorf("last error = %q, want it cleared", replayed.LastError)
	}
	mustLease(t, s, "q", "worker-2", time.Minute)
}

func TestReplayRejectsLiveJob(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	submitted := submitOne(t, s, "q", 5)
	if err := s.ReplayJob(submitted.ID); err != ErrNotDeadLettered {
		t.Errorf("replay of a pending job = %v, want %v", err, ErrNotDeadLettered)
	}
	if err := s.ReplayJob("nope"); err != ErrJobNotFound {
		t.Errorf("replay of an unknown job = %v, want %v", err, ErrJobNotFound)
	}
}

func TestLeaseStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s := testStore(t, dir)

	done := submitOne(t, s, "q", 5)
	j := mustLease(t, s, "q", "worker-1", time.Minute)
	if _, err := s.Ack(j.ID, j.Epoch); err != nil {
		t.Fatalf("ack: %v", err)
	}

	dead := submitOne(t, s, "q", 0)
	j2 := mustLease(t, s, "q", "worker-1", time.Minute)
	if _, _, err := s.Nack(j2.ID, j2.Epoch, "boom"); err != nil {
		t.Fatalf("nack: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2 := testStore(t, dir)
	defer s2.Close()

	if got, _ := s2.Get(done.ID); got.State != job.StateDone {
		t.Errorf("completed job recovered as %s, want done", got.State)
	}
	if got, _ := s2.Get(dead.ID); got.State != job.StateDeadLetter {
		t.Errorf("dead-lettered job recovered as %s, want dead_letter", got.State)
	}
	if listed := s2.ListJobs("q", job.StateDeadLetter, 0); len(listed) != 1 {
		t.Errorf("dead-letter listing returned %d jobs, want 1", len(listed))
	}
}

func TestAckUnknownJob(t *testing.T) {
	s := testStore(t, t.TempDir())
	defer s.Close()

	if _, err := s.Ack("nope", 1); err != ErrJobNotFound {
		t.Errorf("ack unknown job = %v, want %v", err, ErrJobNotFound)
	}
}

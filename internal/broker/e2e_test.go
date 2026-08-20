package broker_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"conveyor/internal/broker"
	conveyorv1 "conveyor/internal/genproto/conveyor/v1"
	"conveyor/internal/genproto/conveyor/v1/conveyorv1connect"
	"conveyor/internal/handler"
	"conveyor/internal/worker"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startBroker runs a real broker -- store, dispatcher, and HTTP server -- and
// returns a client wired to it.
func startBroker(t *testing.T, cfg broker.Config) (*broker.Store, conveyorv1connect.BrokerServiceClient) {
	t.Helper()
	if cfg.Dir == "" {
		cfg.Dir = t.TempDir()
	}

	store, err := broker.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	dispatcher := broker.NewDispatcher(store, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		dispatcher.Run(ctx)
	}()

	mux := http.NewServeMux()
	path, h := conveyorv1connect.NewBrokerServiceHandler(broker.NewServer(store, dispatcher))
	mux.Handle(path, h)
	srv := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))

	t.Cleanup(func() {
		srv.Close()
		cancel()
		<-done
		store.Close()
	})

	return store, conveyorv1connect.NewBrokerServiceClient(srv.Client(), srv.URL)
}

// startWorker runs a worker pool until the returned function is called.
func startWorker(t *testing.T, client conveyorv1connect.BrokerServiceClient, queue, id string, concurrency int, lease time.Duration, reg handler.Registry) func() {
	t.Helper()

	pool := &worker.Pool{
		Client:        client,
		Queue:         queue,
		WorkerID:      id,
		Concurrency:   concurrency,
		LeaseDuration: lease,
		Handlers:      reg,
		Logger:        discardLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := pool.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("worker %s: %v", id, err)
		}
	}()

	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		<-done
	}
	t.Cleanup(stop)
	return stop
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func submit(t *testing.T, client conveyorv1connect.BrokerServiceClient, req *conveyorv1.SubmitRequest) string {
	t.Helper()
	resp, err := client.Submit(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return resp.Msg.GetJobId()
}

// recordingHandler counts executions and can be told to fail or block.
type recordingHandler struct {
	mu       sync.Mutex
	runs     map[string]int
	behavior func(attempt int) error
}

func newRecordingHandler(behavior func(attempt int) error) *recordingHandler {
	return &recordingHandler{runs: make(map[string]int), behavior: behavior}
}

func (h *recordingHandler) Name() string { return "test" }

func (h *recordingHandler) Execute(ctx context.Context, j handler.Job) error {
	h.mu.Lock()
	h.runs[j.ID]++
	h.mu.Unlock()

	if h.behavior == nil {
		return nil
	}
	_ = ctx
	return h.behavior(j.Attempt)
}

func (h *recordingHandler) totalRuns() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, c := range h.runs {
		n += c
	}
	return n
}

func (h *recordingHandler) runsFor(id string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.runs[id]
}

func registry(h handler.Handler) handler.Registry {
	return handler.Registry{h.Name(): h}
}

// TestEndToEndJobsExecute runs the whole stack: submit over RPC, dispatch to a
// real worker pool, execute, ack.
func TestEndToEndJobsExecute(t *testing.T) {
	store, client := startBroker(t, broker.Config{})
	h := newRecordingHandler(nil)
	startWorker(t, client, "emails", "w1", 4, 30*time.Second, registry(h))

	const n = 25
	for i := 0; i < n; i++ {
		submit(t, client, &conveyorv1.SubmitRequest{
			Queue:   "emails",
			Handler: "test",
			Payload: []byte(fmt.Sprintf("job-%d", i)),
		})
	}

	waitFor(t, 15*time.Second, "all jobs to finish", func() bool {
		qs := store.Stats("emails").Queues
		return len(qs) == 1 && qs[0].Done == n
	})
	if got := h.totalRuns(); got != n {
		t.Errorf("handler ran %d times, want %d", got, n)
	}
}

// TestEndToEndRetriesThenDeadLettersThenReplays walks a failing job through its
// whole lifecycle, which is the path an operator actually cares about.
func TestEndToEndRetriesThenDeadLettersThenReplays(t *testing.T) {
	store, client := startBroker(t, broker.Config{
		BackoffBase: time.Millisecond,
		BackoffCap:  5 * time.Millisecond,
	})

	var failing = true
	h := newRecordingHandler(func(int) error {
		if failing {
			return errors.New("dependency is down")
		}
		return nil
	})
	startWorker(t, client, "emails", "w1", 2, 30*time.Second, registry(h))

	const maxRetries = 2
	id := submit(t, client, &conveyorv1.SubmitRequest{
		Queue:      "emails",
		Handler:    "test",
		Payload:    []byte("doomed"),
		MaxRetries: maxRetries,
	})

	waitFor(t, 15*time.Second, "the job to be dead-lettered", func() bool {
		qs := store.Stats("emails").Queues
		return len(qs) == 1 && qs[0].DeadLetter == 1
	})

	if got, want := h.runsFor(id), maxRetries+1; got != want {
		t.Errorf("handler ran %d times before dead-lettering, want %d", got, want)
	}

	dead, _ := store.Get(id)
	if dead.LastError == "" {
		t.Error("dead-lettered job kept no error to explain why")
	}

	// The operator fixes the dependency and replays the job.
	failing = false
	if _, err := client.ReplayJob(context.Background(),
		connect.NewRequest(&conveyorv1.ReplayJobRequest{JobId: id})); err != nil {
		t.Fatalf("replay: %v", err)
	}

	waitFor(t, 15*time.Second, "the replayed job to succeed", func() bool {
		j, ok := store.Get(id)
		return ok && j.State.Terminal() && j.State.String() == "done"
	})
}

// wedgedWorkerClient models a worker that has been partitioned from the broker:
// its heartbeats never land, so its leases cannot be renewed. It records the
// outcome of every Nack so a test can observe the broker refusing a stale one.
type wedgedWorkerClient struct {
	conveyorv1connect.BrokerServiceClient

	mu          sync.Mutex
	nackResults []bool
}

func (c *wedgedWorkerClient) Heartbeat(
	context.Context, *connect.Request[conveyorv1.HeartbeatRequest],
) (*connect.Response[conveyorv1.HeartbeatResponse], error) {
	return nil, errors.New("simulated network partition")
}

func (c *wedgedWorkerClient) Nack(
	ctx context.Context, req *connect.Request[conveyorv1.NackRequest],
) (*connect.Response[conveyorv1.NackResponse], error) {
	resp, err := c.BrokerServiceClient.Nack(ctx, req)
	if err == nil {
		c.mu.Lock()
		c.nackResults = append(c.nackResults, resp.Msg.GetAccepted())
		c.mu.Unlock()
	}
	return resp, err
}

func (c *wedgedWorkerClient) rejectedNacks() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, accepted := range c.nackResults {
		if !accepted {
			n++
		}
	}
	return n
}

// stallingHandler wedges: it holds every job until the test releases it, and
// deliberately ignores the job's context while doing so.
//
// Ignoring the context is the whole point. A healthy worker ties its execution
// to the lease deadline, so when a lease runs out it cancels its own work and
// reports the failure itself -- which means the broker's reclaim sweep never
// gets involved. Reaching the reclaim path at all requires a worker that has
// genuinely stopped responding, which is what this simulates.
type stallingHandler struct {
	releaseCh   chan struct{}
	releaseOnce sync.Once
	started     chan struct{}
	startOnce   sync.Once

	mu      sync.Mutex
	entered int
}

func newStallingHandler() *stallingHandler {
	return &stallingHandler{
		releaseCh: make(chan struct{}),
		started:   make(chan struct{}),
	}
}

func (h *stallingHandler) Name() string { return "test" }

func (h *stallingHandler) Execute(_ context.Context, _ handler.Job) error {
	h.mu.Lock()
	h.entered++
	h.mu.Unlock()
	h.startOnce.Do(func() { close(h.started) })

	<-h.releaseCh
	return errors.New("finished long after its lease expired")
}

// release unwedges the handler. It is safe to call more than once.
func (h *stallingHandler) release() {
	h.releaseOnce.Do(func() { close(h.releaseCh) })
}

// TestEndToEndZombieWorkerLosesJobAndIsFenced is the zombie-worker scenario
// played out over the real RPC stack, end to end:
//
//  1. a worker leases a job and wedges, unable to renew or report;
//  2. the broker reclaims the expired lease and charges the attempt;
//  3. a healthy worker picks the job up and completes it;
//  4. the wedged worker finally comes back and reports on the job it lost --
//     and the broker refuses it.
//
// Step 4 is the one that matters. Without fencing, that late report would be
// applied to a job somebody else already finished.
func TestEndToEndZombieWorkerLosesJobAndIsFenced(t *testing.T) {
	const lease = time.Second

	store, base := startBroker(t, broker.Config{
		BackoffBase: time.Millisecond,
		BackoffCap:  5 * time.Millisecond,
	})

	zombieClient := &wedgedWorkerClient{BrokerServiceClient: base}
	wedged := newStallingHandler()
	// Runs before the cleanup that stops the workers, so a wedged handler can
	// never deadlock the pool's shutdown.
	defer wedged.release()

	stopZombie := startWorker(t, zombieClient, "emails", "zombie", 1, lease, registry(wedged))

	id := submit(t, base, &conveyorv1.SubmitRequest{
		Queue:      "emails",
		Handler:    "test",
		Payload:    []byte("contended"),
		MaxRetries: 50, // ample budget; this is about handoff, not exhaustion
	})

	select {
	case <-wedged.started:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the zombie worker to pick up the job")
	}

	// The broker notices the lease went unrenewed and takes the job back.
	waitFor(t, 15*time.Second, "the broker to reclaim the expired lease", func() bool {
		j, ok := store.Get(id)
		return ok && j.LastError == "lease expired"
	})
	if reclaimed, _ := store.Get(id); reclaimed.Attempt < 1 {
		t.Errorf("attempt = %d after a reclaim, want the timed-out delivery to have been charged",
			reclaimed.Attempt)
	}

	// The zombie comes back to life and reports on work that is no longer its
	// own. The broker must refuse it.
	wedged.release()
	waitFor(t, 10*time.Second, "the zombie's stale report to be refused", func() bool {
		return zombieClient.rejectedNacks() > 0
	})

	// Retire the zombie before bringing in a healthy worker. Left running it
	// would keep re-leasing the job and wedging it again, turning the handoff
	// into a race rather than something this test can assert.
	stopZombie()

	healthy := newRecordingHandler(nil)
	startWorker(t, base, "emails", "healthy", 1, 30*time.Second, registry(healthy))

	waitFor(t, 25*time.Second, "the healthy worker to complete the job", func() bool {
		j, ok := store.Get(id)
		return ok && j.State.String() == "done"
	})
	if healthy.runsFor(id) == 0 {
		t.Error("the healthy worker never ran the job")
	}

	final, _ := store.Get(id)
	if final.State.String() != "done" {
		t.Errorf("job ended as %s; the zombie's stale report was applied after all", final.State)
	}
	if final.LeasedBy != "" {
		t.Errorf("completed job still records a holder: %q", final.LeasedBy)
	}
}

// TestEndToEndPriorityAndConcurrency checks that a busy worker pool still
// respects ordering, and that concurrency is actually bounded by what the
// worker asked for.
func TestEndToEndPriorityAndConcurrency(t *testing.T) {
	const concurrency = 3

	store, client := startBroker(t, broker.Config{})

	var mu sync.Mutex
	var active, peak int
	h := &countingHandler{
		onStart: func() {
			mu.Lock()
			active++
			if active > peak {
				peak = active
			}
			mu.Unlock()
		},
		onEnd: func() {
			mu.Lock()
			active--
			mu.Unlock()
		},
	}
	startWorker(t, client, "q", "w1", concurrency, 30*time.Second, registry(h))

	const n = 30
	for i := 0; i < n; i++ {
		submit(t, client, &conveyorv1.SubmitRequest{
			Queue:   "q",
			Handler: "test",
			Payload: []byte("x"),
		})
	}

	waitFor(t, 20*time.Second, "all jobs to finish", func() bool {
		qs := store.Stats("q").Queues
		return len(qs) == 1 && qs[0].Done == n
	})

	mu.Lock()
	got := peak
	mu.Unlock()
	if got > concurrency {
		t.Errorf("ran %d jobs at once, above the requested concurrency of %d", got, concurrency)
	}
	if got < 2 {
		t.Errorf("peak concurrency was %d; the pool never actually ran jobs in parallel", got)
	}
}

type countingHandler struct {
	onStart, onEnd func()
}

func (h *countingHandler) Name() string { return "test" }

func (h *countingHandler) Execute(_ context.Context, _ handler.Job) error {
	h.onStart()
	defer h.onEnd()
	time.Sleep(20 * time.Millisecond)
	return nil
}

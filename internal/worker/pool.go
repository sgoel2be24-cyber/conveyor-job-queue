// Package worker runs jobs leased from a broker.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"connectrpc.com/connect"

	conveyorv1 "conveyor/internal/genproto/conveyor/v1"
	"conveyor/internal/genproto/conveyor/v1/conveyorv1connect"
	"conveyor/internal/handler"
)

// reportTimeout bounds how long a worker will spend telling the broker how a
// job turned out.
const reportTimeout = 10 * time.Second

// Pool leases jobs from a broker and executes them concurrently.
type Pool struct {
	Client        conveyorv1connect.BrokerServiceClient
	Queue         string
	WorkerID      string
	Concurrency   int
	LeaseDuration time.Duration
	Handlers      handler.Registry
	Logger        *slog.Logger
}

// Run leases and executes jobs until ctx is cancelled or the stream fails.
//
// Concurrency is enforced by the broker, which will not have more than
// Concurrency jobs outstanding to this worker at once, so received jobs can be
// run as they arrive without a local semaphore.
func (p *Pool) Run(ctx context.Context) error {
	if p.Queue == "" {
		return errors.New("worker: queue is required")
	}
	if p.Concurrency < 1 {
		p.Concurrency = 1
	}
	if p.LeaseDuration <= 0 {
		p.LeaseDuration = 30 * time.Second
	}

	stream, err := p.Client.Lease(ctx, connect.NewRequest(&conveyorv1.LeaseRequest{
		Queue:           p.Queue,
		WorkerId:        p.WorkerID,
		MaxInFlight:     int32(p.Concurrency),
		LeaseDurationMs: p.LeaseDuration.Milliseconds(),
	}))
	if err != nil {
		return fmt.Errorf("open lease stream: %w", err)
	}
	defer stream.Close()

	p.Logger.Info("worker started",
		"worker", p.WorkerID, "queue", p.Queue,
		"concurrency", p.Concurrency, "lease", p.LeaseDuration)

	// Let running jobs finish reporting before Run returns.
	var wg sync.WaitGroup
	defer wg.Wait()

	for stream.Receive() {
		msg := stream.Msg()
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.execute(ctx, msg)
		}()
	}

	if err := stream.Err(); err != nil && ctx.Err() == nil {
		return fmt.Errorf("lease stream: %w", err)
	}
	return nil
}

// execute runs one job and reports the outcome.
func (p *Pool) execute(parent context.Context, msg *conveyorv1.LeaseResponse) {
	log := p.Logger.With("job", msg.GetJobId(), "attempt", msg.GetAttempt())

	// Bound the work by the lease. Past its expiry the broker may have given
	// this job to someone else, and anything produced here would be discarded.
	deadline := time.UnixMilli(msg.GetLeaseExpiresAtUnixMs())
	jobCtx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()

	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		p.heartbeat(jobCtx, cancel, msg, log)
	}()

	start := time.Now()
	execErr := p.invoke(jobCtx, msg)

	cancel()
	<-heartbeatDone

	// Report on a fresh context: jobCtx is cancelled by now, and the parent may
	// be too if the worker is shutting down. Telling the broker what happened
	// is worth a few seconds even then -- otherwise the job waits out its whole
	// lease before anyone can retry it.
	reportCtx, done := context.WithTimeout(context.WithoutCancel(parent), reportTimeout)
	defer done()

	if execErr != nil {
		log.Warn("job failed", "err", execErr, "took", time.Since(start).Round(time.Millisecond))
		p.nack(reportCtx, msg, execErr, log)
		return
	}
	log.Info("job done", "took", time.Since(start).Round(time.Millisecond))
	p.ack(reportCtx, msg, log)
}

func (p *Pool) invoke(ctx context.Context, msg *conveyorv1.LeaseResponse) error {
	h, ok := p.Handlers.Get(msg.GetHandler())
	if !ok {
		return fmt.Errorf("no handler registered for %q (have %v)", msg.GetHandler(), p.Handlers.Names())
	}
	return h.Execute(ctx, handler.Job{
		ID:             msg.GetJobId(),
		Queue:          msg.GetQueue(),
		Handler:        msg.GetHandler(),
		IdempotencyKey: msg.GetIdempotencyKey(),
		Payload:        msg.GetPayload(),
		Attempt:        int(msg.GetAttempt()),
	})
}

// heartbeat renews the lease while the job runs, and aborts the job if the
// broker says the lease is no longer ours.
//
// That rejection is the fencing check firing: some other worker now owns this
// job, so continuing would burn time on a result the broker is going to throw
// away -- and, for a handler with side effects, would cause them twice.
func (p *Pool) heartbeat(ctx context.Context, abort context.CancelFunc, msg *conveyorv1.LeaseResponse, log *slog.Logger) {
	// Renew well before expiry so a slow round trip does not cost us the lease.
	interval := p.LeaseDuration / 3
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := p.Client.Heartbeat(ctx, connect.NewRequest(&conveyorv1.HeartbeatRequest{
				JobId:           msg.GetJobId(),
				Epoch:           msg.GetEpoch(),
				WorkerId:        p.WorkerID,
				LeaseDurationMs: p.LeaseDuration.Milliseconds(),
			}))
			if err != nil {
				if ctx.Err() == nil {
					log.Warn("heartbeat failed", "err", err)
				}
				continue
			}
			if !resp.Msg.GetAccepted() {
				log.Warn("lease lost to another worker, aborting", "epoch", msg.GetEpoch())
				abort()
				return
			}
		}
	}
}

func (p *Pool) ack(ctx context.Context, msg *conveyorv1.LeaseResponse, log *slog.Logger) {
	resp, err := p.Client.Ack(ctx, connect.NewRequest(&conveyorv1.AckRequest{
		JobId:    msg.GetJobId(),
		Epoch:    msg.GetEpoch(),
		WorkerId: p.WorkerID,
	}))
	switch {
	case err != nil:
		log.Error("ack failed", "err", err)
	case !resp.Msg.GetAccepted():
		// Fenced: the work was done, but the broker had already reassigned the
		// job, so it does not count. Whatever this job touched has now been
		// touched twice -- the at-least-once contract in action.
		log.Warn("ack rejected: lease was already reclaimed")
	}
}

func (p *Pool) nack(ctx context.Context, msg *conveyorv1.LeaseResponse, cause error, log *slog.Logger) {
	resp, err := p.Client.Nack(ctx, connect.NewRequest(&conveyorv1.NackRequest{
		JobId:    msg.GetJobId(),
		Epoch:    msg.GetEpoch(),
		WorkerId: p.WorkerID,
		Reason:   cause.Error(),
	}))
	switch {
	case err != nil:
		log.Error("nack failed", "err", err)
	case !resp.Msg.GetAccepted():
		log.Warn("nack rejected: lease was already reclaimed")
	case resp.Msg.GetDeadLettered():
		log.Error("job dead-lettered: retries exhausted")
	}
}

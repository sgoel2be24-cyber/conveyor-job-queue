package broker

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"

	conveyorv1 "github.com/sgoel2be24-cyber/conveyor-job-queue/internal/genproto/conveyor/v1"
	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/genproto/conveyor/v1/conveyorv1connect"
	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/job"
)

// Lease duration bounds. A lease too short gets reclaimed out from under a
// working handler; too long, and a dead worker's jobs sit idle that much longer.
const (
	DefaultLeaseDuration = 30 * time.Second
	MinLeaseDuration     = time.Second
	MaxLeaseDuration     = time.Hour
)

// Server adapts Store and Dispatcher to the BrokerService RPC surface.
type Server struct {
	conveyorv1connect.UnimplementedBrokerServiceHandler

	store      *Store
	dispatcher *Dispatcher
}

// NewServer returns a handler backed by store and dispatcher.
func NewServer(store *Store, dispatcher *Dispatcher) *Server {
	return &Server{store: store, dispatcher: dispatcher}
}

// Submit enqueues a job.
func (s *Server) Submit(
	_ context.Context,
	req *connect.Request[conveyorv1.SubmitRequest],
) (*connect.Response[conveyorv1.SubmitResponse], error) {
	msg := req.Msg

	j, dedup, err := s.store.Submit(SubmitParams{
		Queue:          msg.GetQueue(),
		Handler:        msg.GetHandler(),
		Payload:        msg.GetPayload(),
		IdempotencyKey: msg.GetIdempotencyKey(),
		Priority:       priorityFromProto(msg.GetPriority()),
		MaxRetries:     int(msg.GetMaxRetries()),
		Delay:          time.Duration(msg.GetDelayMs()) * time.Millisecond,
	})
	if err != nil {
		if errors.Is(err, ErrQueueRequired) {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&conveyorv1.SubmitResponse{
		JobId:        j.ID,
		Deduplicated: dedup,
	}), nil
}

// Lease streams jobs to a worker for as long as it stays connected.
func (s *Server) Lease(
	ctx context.Context,
	req *connect.Request[conveyorv1.LeaseRequest],
	stream *connect.ServerStream[conveyorv1.LeaseResponse],
) error {
	msg := req.Msg
	if msg.GetQueue() == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("queue is required"))
	}

	workerID := msg.GetWorkerId()
	if workerID == "" {
		workerID = job.NewID()
	}
	maxInFlight := int(msg.GetMaxInFlight())
	if maxInFlight < 1 {
		maxInFlight = 1
	}
	leaseDur := clampLease(time.Duration(msg.GetLeaseDurationMs()) * time.Millisecond)

	st := s.dispatcher.register(ctx, msg.GetQueue(), workerID, maxInFlight, leaseDur)
	defer s.dispatcher.unregister(st)

	for {
		select {
		case <-ctx.Done():
			return nil // the worker hung up; not an error
		case j := <-st.ch:
			if err := stream.Send(&conveyorv1.LeaseResponse{
				JobId:                j.ID,
				Epoch:                j.Epoch,
				Queue:                j.Queue,
				IdempotencyKey:       j.IdempotencyKey,
				Handler:              j.Handler,
				Payload:              j.Payload,
				Attempt:              int32(j.Attempt),
				LeaseExpiresAtUnixMs: j.LeaseExpiresAt.UnixMilli(),
			}); err != nil {
				// The job stays leased and will be reclaimed when it expires.
				return err
			}
		}
	}
}

// Ack marks a leased job complete.
func (s *Server) Ack(
	_ context.Context,
	req *connect.Request[conveyorv1.AckRequest],
) (*connect.Response[conveyorv1.AckResponse], error) {
	accepted, err := s.store.Ack(req.Msg.GetJobId(), req.Msg.GetEpoch())
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if accepted {
		s.dispatcher.release(req.Msg.GetJobId())
	}
	return connect.NewResponse(&conveyorv1.AckResponse{Accepted: accepted}), nil
}

// Nack reports a failed delivery.
func (s *Server) Nack(
	_ context.Context,
	req *connect.Request[conveyorv1.NackRequest],
) (*connect.Response[conveyorv1.NackResponse], error) {
	accepted, deadLettered, err := s.store.Nack(req.Msg.GetJobId(), req.Msg.GetEpoch(), req.Msg.GetReason())
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if accepted {
		s.dispatcher.release(req.Msg.GetJobId())
	}
	return connect.NewResponse(&conveyorv1.NackResponse{
		Accepted:     accepted,
		DeadLettered: deadLettered,
	}), nil
}

// Heartbeat extends a lease.
func (s *Server) Heartbeat(
	_ context.Context,
	req *connect.Request[conveyorv1.HeartbeatRequest],
) (*connect.Response[conveyorv1.HeartbeatResponse], error) {
	accepted, expires, err := s.store.Heartbeat(
		req.Msg.GetJobId(),
		req.Msg.GetEpoch(),
		clampLease(time.Duration(req.Msg.GetLeaseDurationMs())*time.Millisecond),
	)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &conveyorv1.HeartbeatResponse{Accepted: accepted}
	if accepted {
		resp.NewLeaseExpiresAtUnixMs = expires.UnixMilli()
	}
	return connect.NewResponse(resp), nil
}

// Get looks up a job by ID.
func (s *Server) Get(
	_ context.Context,
	req *connect.Request[conveyorv1.GetRequest],
) (*connect.Response[conveyorv1.GetResponse], error) {
	j, ok := s.store.Get(req.Msg.GetJobId())
	if !ok {
		return connect.NewResponse(&conveyorv1.GetResponse{Found: false}), nil
	}
	return connect.NewResponse(&conveyorv1.GetResponse{Found: true, Job: jobToProto(j)}), nil
}

// ListJobs returns jobs filtered by queue and state.
func (s *Server) ListJobs(
	_ context.Context,
	req *connect.Request[conveyorv1.ListJobsRequest],
) (*connect.Response[conveyorv1.ListJobsResponse], error) {
	jobs := s.store.ListJobs(
		req.Msg.GetQueue(),
		stateFromProto(req.Msg.GetState()),
		int(req.Msg.GetLimit()),
	)

	resp := &conveyorv1.ListJobsResponse{Jobs: make([]*conveyorv1.Job, 0, len(jobs))}
	for _, j := range jobs {
		resp.Jobs = append(resp.Jobs, jobToProto(j))
	}
	return connect.NewResponse(resp), nil
}

// ReplayJob returns a dead-lettered job to its queue.
func (s *Server) ReplayJob(
	_ context.Context,
	req *connect.Request[conveyorv1.ReplayJobRequest],
) (*connect.Response[conveyorv1.ReplayJobResponse], error) {
	switch err := s.store.ReplayJob(req.Msg.GetJobId()); {
	case err == nil:
	case errors.Is(err, ErrJobNotFound):
		return nil, connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, ErrNotDeadLettered):
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	default:
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&conveyorv1.ReplayJobResponse{Replayed: true}), nil
}

// Stats reports per-queue counts.
func (s *Server) Stats(
	_ context.Context,
	req *connect.Request[conveyorv1.StatsRequest],
) (*connect.Response[conveyorv1.StatsResponse], error) {
	st := s.store.Stats(req.Msg.GetQueue())

	resp := &conveyorv1.StatsResponse{
		TotalJobs: st.TotalJobs,
		LastLsn:   st.LastLSN,
		Queues:    make([]*conveyorv1.QueueStats, 0, len(st.Queues)),
	}
	for _, q := range st.Queues {
		resp.Queues = append(resp.Queues, &conveyorv1.QueueStats{
			Queue:      q.Queue,
			Pending:    q.Pending,
			Leased:     q.Leased,
			RetryWait:  q.RetryWait,
			Done:       q.Done,
			DeadLetter: q.DeadLetter,
		})
	}
	return connect.NewResponse(resp), nil
}

func clampLease(d time.Duration) time.Duration {
	switch {
	case d <= 0:
		return DefaultLeaseDuration
	case d < MinLeaseDuration:
		return MinLeaseDuration
	case d > MaxLeaseDuration:
		return MaxLeaseDuration
	default:
		return d
	}
}

func jobToProto(j *job.Job) *conveyorv1.Job {
	if j == nil {
		return nil
	}
	p := &conveyorv1.Job{
		Id:               j.ID,
		Queue:            j.Queue,
		Handler:          j.Handler,
		Payload:          j.Payload,
		IdempotencyKey:   j.IdempotencyKey,
		Priority:         priorityToProto(j.Priority),
		MaxRetries:       int32(j.MaxRetries),
		Attempt:          int32(j.Attempt),
		State:            stateToProto(j.State),
		Epoch:            j.Epoch,
		EnqueuedAtUnixMs: j.EnqueuedAt.UnixMilli(),
		EligibleAtUnixMs: j.EligibleAt.UnixMilli(),
		LeasedBy:         j.LeasedBy,
		LastError:        j.LastError,
	}
	if !j.LeaseExpiresAt.IsZero() {
		p.LeaseExpiresAtUnixMs = j.LeaseExpiresAt.UnixMilli()
	}
	return p
}

func priorityFromProto(p conveyorv1.Priority) job.Priority {
	switch p {
	case conveyorv1.Priority_PRIORITY_LOW:
		return job.PriorityLow
	case conveyorv1.Priority_PRIORITY_HIGH:
		return job.PriorityHigh
	default:
		return job.PriorityNormal
	}
}

func priorityToProto(p job.Priority) conveyorv1.Priority {
	switch p {
	case job.PriorityLow:
		return conveyorv1.Priority_PRIORITY_LOW
	case job.PriorityHigh:
		return conveyorv1.Priority_PRIORITY_HIGH
	default:
		return conveyorv1.Priority_PRIORITY_NORMAL
	}
}

func stateToProto(s job.State) conveyorv1.JobState {
	switch s {
	case job.StatePending:
		return conveyorv1.JobState_JOB_STATE_PENDING
	case job.StateLeased:
		return conveyorv1.JobState_JOB_STATE_LEASED
	case job.StateRetryWait:
		return conveyorv1.JobState_JOB_STATE_RETRY_WAIT
	case job.StateDone:
		return conveyorv1.JobState_JOB_STATE_DONE
	case job.StateDeadLetter:
		return conveyorv1.JobState_JOB_STATE_DEAD_LETTER
	default:
		return conveyorv1.JobState_JOB_STATE_UNSPECIFIED
	}
}

func stateFromProto(s conveyorv1.JobState) job.State {
	switch s {
	case conveyorv1.JobState_JOB_STATE_PENDING:
		return job.StatePending
	case conveyorv1.JobState_JOB_STATE_LEASED:
		return job.StateLeased
	case conveyorv1.JobState_JOB_STATE_RETRY_WAIT:
		return job.StateRetryWait
	case conveyorv1.JobState_JOB_STATE_DONE:
		return job.StateDone
	case conveyorv1.JobState_JOB_STATE_DEAD_LETTER:
		return job.StateDeadLetter
	default:
		return job.StateUnspecified
	}
}

package broker

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"

	conveyorv1 "conveyor/internal/genproto/conveyor/v1"
	"conveyor/internal/genproto/conveyor/v1/conveyorv1connect"
	"conveyor/internal/job"
)

// Server adapts Store to the BrokerService RPC surface.
type Server struct {
	conveyorv1connect.UnimplementedBrokerServiceHandler

	store *Store
}

// NewServer returns a handler backed by store.
func NewServer(store *Store) *Server { return &Server{store: store} }

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

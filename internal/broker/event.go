package broker

import (
	"encoding/json"
	"fmt"
	"time"

	"conveyor/internal/job"
)

// eventType identifies a state transition recorded in the WAL.
type eventType string

const (
	// eventSubmit records a newly enqueued job, carrying the whole job record
	// so replay can reconstruct it without consulting anything else.
	eventSubmit eventType = "submit"

	// eventLease records a job being handed to a worker under a fencing token.
	eventLease eventType = "lease"

	// eventAck records successful completion.
	eventAck eventType = "ack"

	// eventFail records a failed delivery and where it left the job -- waiting
	// for a retry, or dead-lettered. It covers both an explicit Nack from a
	// worker and a lease that timed out, because those are the same event as
	// far as the retry budget is concerned. See Store.failLocked.
	eventFail eventType = "fail"

	// eventReplayJob records a dead-lettered job being returned to its queue.
	eventReplayJob eventType = "replay"

	// eventEpochReserve records a block of fencing tokens being claimed. See
	// Store.reserveEpochsLocked for why this exists.
	eventEpochReserve eventType = "epoch_reserve"
)

// event is the unit written to the WAL. Every change to the in-memory index is
// preceded by durably logging the event that caused it, so recovery can rebuild
// the index by replaying events in order.
//
// Events record the *outcome* of a decision, never the inputs that produced it.
// A fail event stores the computed EligibleAt rather than "retry with backoff",
// because the backoff is jittered: recomputing it during replay would produce a
// different retry schedule than the one that actually ran. Anything drawn from
// a clock or an RNG has to be captured here, or replay is not deterministic.
//
// Events are encoded as JSON. A hand-rolled binary encoding would be smaller
// and faster, but the cost that actually governs write throughput here is the
// flush that follows each append -- milliseconds, against microseconds to
// marshal a small struct. JSON buys legible segment files (a WAL you can read
// with `xxd` during a 2am debugging session) at no meaningful cost.
type event struct {
	Type eventType `json:"t"`

	// submit
	Job *job.Job `json:"job,omitempty"`

	// everything else
	JobID string `json:"id,omitempty"`
	Epoch uint64 `json:"e,omitempty"`

	// lease
	WorkerID       string    `json:"w,omitempty"`
	LeaseExpiresAt time.Time `json:"lx,omitzero"`

	// lease and fail both carry the resulting attempt count, so the retry
	// budget survives replay exactly as it stood.
	Attempt int `json:"a,omitempty"`

	// fail
	Reason     string    `json:"r,omitempty"`
	NextState  job.State `json:"ns,omitempty"`
	EligibleAt time.Time `json:"el,omitzero"`
	// Timeout distinguishes a reclaimed lease from an explicit Nack. It changes
	// nothing about how the job is treated -- it is recorded for operators
	// trying to tell "the handler failed" from "the worker vanished".
	Timeout bool `json:"to,omitempty"`

	// epoch_reserve
	ReservedUpTo uint64 `json:"ru,omitempty"`
}

func encodeEvent(e *event) ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("broker: encode event: %w", err)
	}
	return b, nil
}

func decodeEvent(b []byte) (*event, error) {
	var e event
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("broker: decode event: %w", err)
	}
	return &e, nil
}

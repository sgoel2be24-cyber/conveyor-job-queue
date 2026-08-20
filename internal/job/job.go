// Package job defines Conveyor's core domain types: the job record itself and
// the states it moves through.
package job

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// State is a job's position in the delivery lifecycle.
//
//	Pending    --lease(epoch E)--> Leased
//	Leased     --Ack(id, E)------> Done
//	Leased     --Nack(id, E)-----> RetryWait --backoff--> Pending
//	Leased     --lease timeout---> RetryWait --backoff--> Pending
//	RetryWait  --retries exhausted--> DeadLetter
//
// The two paths out of Leased that lead to RetryWait — an explicit Nack and a
// lease timeout — must increment the same attempt counter. If a timeout did not
// count as an attempt, a job whose handler always outlives its lease would
// cycle Pending -> Leased -> timeout -> Pending forever and never reach the
// dead-letter queue.
type State uint8

const (
	StateUnspecified State = iota
	StatePending
	StateLeased
	StateRetryWait
	StateDone
	StateDeadLetter
)

func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateLeased:
		return "leased"
	case StateRetryWait:
		return "retry_wait"
	case StateDone:
		return "done"
	case StateDeadLetter:
		return "dead_letter"
	default:
		return "unspecified"
	}
}

// Priority orders otherwise-eligible jobs within a queue.
type Priority uint8

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
)

func (p Priority) String() string {
	switch p {
	case PriorityLow:
		return "low"
	case PriorityHigh:
		return "high"
	default:
		return "normal"
	}
}

// ParsePriority converts a CLI-supplied priority name.
func ParsePriority(s string) (Priority, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "normal":
		return PriorityNormal, nil
	case "low":
		return PriorityLow, nil
	case "high":
		return PriorityHigh, nil
	default:
		return PriorityNormal, fmt.Errorf("unknown priority %q (want low, normal, or high)", s)
	}
}

// Job is a unit of work. Field names in the JSON tags are part of the on-disk
// format for both WAL records and snapshots: renaming one breaks recovery of
// existing data.
type Job struct {
	ID             string    `json:"id"`
	Queue          string    `json:"queue"`
	Handler        string    `json:"handler"`
	Payload        []byte    `json:"payload,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Priority       Priority  `json:"priority"`
	MaxRetries     int       `json:"max_retries"`
	Attempt        int       `json:"attempt"`
	State          State     `json:"state"`
	Epoch          uint64    `json:"epoch"`
	EnqueuedAt     time.Time `json:"enqueued_at"`
	EligibleAt     time.Time `json:"eligible_at"`
	LeaseExpiresAt time.Time `json:"lease_expires_at,omitzero"`
	LeasedBy       string    `json:"leased_by,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
}

// Clone returns a deep copy, so callers cannot mutate state held by the store.
func (j *Job) Clone() *Job {
	if j == nil {
		return nil
	}
	c := *j
	if j.Payload != nil {
		c.Payload = append([]byte(nil), j.Payload...)
	}
	return &c
}

// NewID returns a time-sortable, unique job identifier: 8 bytes of big-endian
// nanosecond timestamp followed by 8 random bytes, hex-encoded. Sorting by ID
// therefore approximates sorting by submission time, which keeps WAL replay and
// snapshot dumps readable.
func NewID() string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(time.Now().UnixNano()))
	if _, err := rand.Read(b[8:]); err != nil {
		panic(fmt.Sprintf("job: cannot read random bytes: %v", err))
	}
	return hex.EncodeToString(b[:])
}

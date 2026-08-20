package broker

import (
	"encoding/json"
	"fmt"

	"conveyor/internal/job"
)

// eventType identifies a state transition recorded in the WAL.
type eventType string

const (
	// eventSubmit records a newly enqueued job. The full job record is stored,
	// so replay can reconstruct it without consulting anything else.
	eventSubmit eventType = "submit"
)

// event is the unit written to the WAL. Every change to the in-memory index is
// preceded by durably logging the event that caused it, so recovery can rebuild
// the index by replaying events in order.
//
// Events are encoded as JSON. A hand-rolled binary encoding would be smaller
// and faster, but the cost that actually governs write throughput here is the
// F_FULLFSYNC that follows each append — measured in milliseconds, against
// microseconds to marshal a small struct. JSON buys legible segment files
// (a WAL you can read with `xxd` during a 2am debugging session) at no
// meaningful throughput cost. Revisit if profiling ever says otherwise.
type event struct {
	Type eventType `json:"t"`
	Job  *job.Job  `json:"job,omitempty"`
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

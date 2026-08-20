package main

import (
	"strings"

	conveyorv1 "github.com/sgoel2be24-cyber/conveyor-job-queue/internal/genproto/conveyor/v1"
)

// displayState renders a job state the way the docs and CLI flags spell it,
// rather than leaking the protobuf enum constant (JOB_STATE_DONE) into output
// people read and script against.
func displayState(s conveyorv1.JobState) string {
	if s == conveyorv1.JobState_JOB_STATE_UNSPECIFIED {
		return "unspecified"
	}
	return strings.ToLower(strings.TrimPrefix(s.String(), "JOB_STATE_"))
}

// displayPriority does the same for priorities, matching the values accepted by
// `submit --priority`.
func displayPriority(p conveyorv1.Priority) string {
	if p == conveyorv1.Priority_PRIORITY_UNSPECIFIED {
		return "normal"
	}
	return strings.ToLower(strings.TrimPrefix(p.String(), "PRIORITY_"))
}

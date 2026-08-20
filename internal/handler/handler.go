// Package handler defines how a worker actually executes a job, and ships the
// two general-purpose implementations Conveyor comes with.
package handler

import (
	"context"
	"fmt"
	"strings"
)

// Job is the view of a job that a handler receives. It is deliberately narrower
// than the broker's record: a handler has no business seeing lease state or
// retry bookkeeping.
type Job struct {
	ID             string
	Queue          string
	Handler        string
	IdempotencyKey string
	Payload        []byte
	Attempt        int
}

// DedupKey returns the value a handler should give a downstream service to
// recognize a repeat delivery. It falls back to the job ID so the key is always
// present, even when the producer did not supply one.
//
// This matters because Conveyor delivers at least once. A job can genuinely run
// twice -- a worker that stalls past its lease has its work reassigned, and
// nothing can un-send an HTTP request it already made. Handing the receiver a
// stable key is what lets it collapse those duplicates on its end.
func (j Job) DedupKey() string {
	if j.IdempotencyKey != "" {
		return j.IdempotencyKey
	}
	return j.ID
}

// Handler executes jobs of one kind.
//
// Execute must respect ctx: it is cancelled when the job's lease expires or
// when the broker tells the worker its lease was reassigned. Returning a
// non-nil error nacks the job, which schedules a retry or dead-letters it.
type Handler interface {
	Name() string
	Execute(ctx context.Context, j Job) error
}

// Registry maps handler names to implementations.
type Registry map[string]Handler

// Get looks up a handler by name.
func (r Registry) Get(name string) (Handler, bool) {
	h, ok := r[name]
	return h, ok
}

// Names returns the registered handler names.
func (r Registry) Names() []string {
	names := make([]string, 0, len(r))
	for name := range r {
		names = append(names, name)
	}
	return names
}

// Default returns the built-in handlers.
func Default() Registry {
	return Registry{
		"shell":   NewShell(),
		"webhook": NewWebhook(),
	}
}

// truncate bounds captured output so one chatty job cannot flood the log or the
// job's stored error message.
func truncate(s string, max int) string {
	s = strings.TrimSpace(s)
	if len(s) <= max {
		return s
	}
	return s[:max] + fmt.Sprintf("... (%d bytes truncated)", len(s)-max)
}

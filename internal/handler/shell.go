package handler

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

// maxCapturedOutput bounds how much of a command's output is kept for the error
// message.
const maxCapturedOutput = 2048

// Shell runs a job's payload as a shell command.
//
// SECURITY: this executes whatever a producer submits. That is the point of a
// job runner -- it is what Sidekiq and Celery do too -- but it means the broker
// must be treated as a trusted-input service. Conveyor listens on localhost by
// default for exactly this reason; exposing it to an untrusted network without
// putting authentication in front of it hands out remote code execution.
type Shell struct {
	// Program is the shell used to interpret payloads. Defaults to "sh".
	Program string
	// GracefulTimeout is how long a cancelled command has to exit after SIGTERM
	// before it is killed outright.
	GracefulTimeout time.Duration
}

// NewShell returns a Shell handler with defaults.
func NewShell() *Shell {
	return &Shell{Program: "sh", GracefulTimeout: 5 * time.Second}
}

// Name implements Handler.
func (h *Shell) Name() string { return "shell" }

// Execute runs the payload, treating a non-zero exit status as a failure.
func (h *Shell) Execute(ctx context.Context, j Job) error {
	program := h.Program
	if program == "" {
		program = "sh"
	}

	cmd := exec.CommandContext(ctx, program, "-c", string(j.Payload))

	// On cancellation -- a lease that expired, or one the broker reassigned --
	// ask the command to stop before killing it, and do not wait forever for a
	// process that ignores the request. This is the best-effort abort described
	// in docs/DESIGN.md: it can stop work still in progress, but it cannot
	// undo an effect the command already had.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	grace := h.GracefulTimeout
	if grace <= 0 {
		grace = 5 * time.Second
	}
	cmd.WaitDelay = grace

	// Job metadata reaches the command as environment variables, so a script
	// can make itself idempotent without parsing its own payload.
	cmd.Env = append(os.Environ(),
		"CONVEYOR_JOB_ID="+j.ID,
		"CONVEYOR_QUEUE="+j.Queue,
		"CONVEYOR_ATTEMPT="+strconv.Itoa(j.Attempt),
		"CONVEYOR_DEDUP_KEY="+j.DedupKey(),
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		if captured := truncate(string(out), maxCapturedOutput); captured != "" {
			return fmt.Errorf("%w: %s", err, captured)
		}
		return err
	}
	return nil
}

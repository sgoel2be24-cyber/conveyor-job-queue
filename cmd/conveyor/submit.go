package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	conveyorv1 "conveyor/internal/genproto/conveyor/v1"
	"conveyor/internal/job"
)

var (
	submitBrokerAddr     string
	submitQueue          string
	submitPayload        string
	submitHandler        string
	submitMaxRetries     int
	submitPriority       string
	submitDelay          time.Duration
	submitIdempotencyKey string
	submitCount          int
)

var submitCmd = &cobra.Command{
	Use:   "submit",
	Short: "Submit a job to a queue.",
	Long: `Submit one or more jobs to a queue.

Each accepted job's ID is printed on its own line, one per line, only after the
broker has durably logged it. Redirecting that output gives you an exact record
of which jobs the broker has promised to keep -- which is what the crash-recovery
harness in scripts/crash_test.sh checks against after a kill -9.`,
	RunE: runSubmit,
}

func runSubmit(cmd *cobra.Command, _ []string) error {
	priority, err := job.ParsePriority(submitPriority)
	if err != nil {
		return err
	}
	if submitCount < 1 {
		return fmt.Errorf("--count must be at least 1")
	}

	client := newClient(submitBrokerAddr)
	ctx := cmd.Context()

	for i := 0; i < submitCount; i++ {
		key := submitIdempotencyKey
		// A fixed key across a batch would collapse it into one job, which is
		// never what --count means.
		if key != "" && submitCount > 1 {
			key = fmt.Sprintf("%s-%d", key, i)
		}

		resp, err := client.Submit(ctx, connectRequest(&conveyorv1.SubmitRequest{
			Queue:          submitQueue,
			Payload:        []byte(submitPayload),
			Handler:        submitHandler,
			IdempotencyKey: key,
			Priority:       priorityToProto(priority),
			MaxRetries:     int32(submitMaxRetries),
			DelayMs:        submitDelay.Milliseconds(),
		}))
		if err != nil {
			return fmt.Errorf("submit job %d of %d: %w", i+1, submitCount, err)
		}

		suffix := ""
		if resp.Msg.GetDeduplicated() {
			suffix = "\t(deduplicated)"
		}
		fmt.Fprintf(cmd.OutOrStdout(), "%s%s\n", resp.Msg.GetJobId(), suffix)
	}
	return nil
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

func init() {
	submitCmd.Flags().StringVar(&submitBrokerAddr, "broker", "localhost:7777", "broker address")
	submitCmd.Flags().StringVar(&submitQueue, "queue", "", "queue name (required)")
	submitCmd.Flags().StringVar(&submitPayload, "payload", "", "job payload")
	submitCmd.Flags().StringVar(&submitHandler, "handler", "shell", "handler to run this job (shell, webhook)")
	submitCmd.Flags().IntVar(&submitMaxRetries, "max-retries", 5, "maximum delivery attempts before dead-lettering")
	submitCmd.Flags().StringVar(&submitPriority, "priority", "normal", "priority: low, normal, high")
	submitCmd.Flags().DurationVar(&submitDelay, "delay", 0, "delay before the job becomes eligible for lease")
	submitCmd.Flags().StringVar(&submitIdempotencyKey, "idempotency-key", "", "dedup key for this submission")
	submitCmd.Flags().IntVar(&submitCount, "count", 1, "number of jobs to submit")
	_ = submitCmd.MarkFlagRequired("queue")
	rootCmd.AddCommand(submitCmd)
}

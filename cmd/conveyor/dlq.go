package main

import (
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	conveyorv1 "github.com/sgoel2be24-cyber/conveyor-job-queue/internal/genproto/conveyor/v1"
)

var (
	dlqBrokerAddr string
	dlqQueue      string
	dlqLimit      int
)

var dlqCmd = &cobra.Command{
	Use:   "dlq",
	Short: "Inspect and replay dead-lettered jobs.",
	Long: `Inspect and replay dead-lettered jobs.

A job lands here after exhausting its retry budget. Nothing retries it again on
its own -- that is the point, since something about it needs a human to look --
so fix the cause, then replay it with a fresh budget.`,
}

var dlqListCmd = &cobra.Command{
	Use:   "list",
	Short: "List dead-lettered jobs.",
	RunE:  runDLQList,
}

var dlqReplayCmd = &cobra.Command{
	Use:   "replay [job-id]",
	Short: "Requeue a dead-lettered job with a fresh retry budget.",
	Args:  cobra.ExactArgs(1),
	RunE:  runDLQReplay,
}

func runDLQList(cmd *cobra.Command, _ []string) error {
	client := newClient(dlqBrokerAddr)

	resp, err := client.ListJobs(cmd.Context(), connectRequest(&conveyorv1.ListJobsRequest{
		Queue: dlqQueue,
		State: conveyorv1.JobState_JOB_STATE_DEAD_LETTER,
		Limit: int32(dlqLimit),
	}))
	if err != nil {
		return fmt.Errorf("list dead-lettered jobs: %w", err)
	}

	out := cmd.OutOrStdout()
	jobs := resp.Msg.GetJobs()
	if len(jobs) == 0 {
		fmt.Fprintln(out, "dead-letter queue is empty")
		return nil
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "JOB ID\tQUEUE\tATTEMPTS\tFAILED AT\tLAST ERROR")
	for _, j := range jobs {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\n",
			j.GetId(), j.GetQueue(), j.GetAttempt(),
			time.UnixMilli(j.GetEligibleAtUnixMs()).Format(time.RFC3339),
			truncateError(j.GetLastError()))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(out, "\n%d dead-lettered job(s). Replay one with: conveyor dlq replay <job-id>\n", len(jobs))
	return nil
}

func runDLQReplay(cmd *cobra.Command, args []string) error {
	client := newClient(dlqBrokerAddr)

	if _, err := client.ReplayJob(cmd.Context(), connectRequest(&conveyorv1.ReplayJobRequest{
		JobId: args[0],
	})); err != nil {
		return fmt.Errorf("replay job: %w", err)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "%s requeued with a fresh retry budget\n", args[0])
	return nil
}

func truncateError(s string) string {
	const max = 60
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

func init() {
	dlqCmd.PersistentFlags().StringVar(&dlqBrokerAddr, "broker", "localhost:7777", "broker address")
	dlqListCmd.Flags().StringVar(&dlqQueue, "queue", "", "restrict to one queue")
	dlqListCmd.Flags().IntVar(&dlqLimit, "limit", 50, "maximum jobs to list (0 for no limit)")
	dlqCmd.AddCommand(dlqListCmd, dlqReplayCmd)
	rootCmd.AddCommand(dlqCmd)
}

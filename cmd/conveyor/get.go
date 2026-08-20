package main

import (
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	conveyorv1 "conveyor/internal/genproto/conveyor/v1"
)

var getBrokerAddr string

var getCmd = &cobra.Command{
	Use:   "get [job-id]",
	Short: "Look up a single job by ID.",
	Args:  cobra.ExactArgs(1),
	RunE:  runGet,
}

func runGet(cmd *cobra.Command, args []string) error {
	client := newClient(getBrokerAddr)

	resp, err := client.Get(cmd.Context(), connectRequest(&conveyorv1.GetRequest{JobId: args[0]}))
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	if !resp.Msg.GetFound() {
		return fmt.Errorf("job %s not found", args[0])
	}

	j := resp.Msg.GetJob()
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "id\t%s\n", j.GetId())
	fmt.Fprintf(tw, "queue\t%s\n", j.GetQueue())
	fmt.Fprintf(tw, "state\t%s\n", j.GetState())
	fmt.Fprintf(tw, "handler\t%s\n", j.GetHandler())
	fmt.Fprintf(tw, "priority\t%s\n", j.GetPriority())
	fmt.Fprintf(tw, "attempt\t%d of %d\n", j.GetAttempt(), j.GetMaxRetries())
	fmt.Fprintf(tw, "epoch\t%d\n", j.GetEpoch())
	fmt.Fprintf(tw, "enqueued\t%s\n", time.UnixMilli(j.GetEnqueuedAtUnixMs()).Format(time.RFC3339))
	fmt.Fprintf(tw, "eligible\t%s\n", time.UnixMilli(j.GetEligibleAtUnixMs()).Format(time.RFC3339))
	if key := j.GetIdempotencyKey(); key != "" {
		fmt.Fprintf(tw, "idempotency-key\t%s\n", key)
	}
	if errMsg := j.GetLastError(); errMsg != "" {
		fmt.Fprintf(tw, "last-error\t%s\n", errMsg)
	}
	if payload := j.GetPayload(); len(payload) > 0 {
		fmt.Fprintf(tw, "payload\t%s\n", payload)
	}
	return tw.Flush()
}

func init() {
	getCmd.Flags().StringVar(&getBrokerAddr, "broker", "localhost:7777", "broker address")
	rootCmd.AddCommand(getCmd)
}

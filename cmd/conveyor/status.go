package main

import (
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"

	conveyorv1 "conveyor/internal/genproto/conveyor/v1"
)

var (
	statusBrokerAddr string
	statusQueue      string
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show queue depths, throughput, and failure counts.",
	RunE:  runStatus,
}

func runStatus(cmd *cobra.Command, _ []string) error {
	client := newClient(statusBrokerAddr)

	resp, err := client.Stats(cmd.Context(), connectRequest(&conveyorv1.StatsRequest{
		Queue: statusQueue,
	}))
	if err != nil {
		return fmt.Errorf("stats: %w", err)
	}

	out := cmd.OutOrStdout()
	if len(resp.Msg.GetQueues()) == 0 {
		fmt.Fprintln(out, "no jobs")
		return nil
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "QUEUE\tPENDING\tLEASED\tRETRY\tDONE\tDEAD")
	for _, q := range resp.Msg.GetQueues() {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\n",
			q.GetQueue(), q.GetPending(), q.GetLeased(), q.GetRetryWait(), q.GetDone(), q.GetDeadLetter())
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	fmt.Fprintf(out, "\ntotal jobs: %d\nlast durable LSN: %d\n",
		resp.Msg.GetTotalJobs(), resp.Msg.GetLastLsn())
	return nil
}

func init() {
	statusCmd.Flags().StringVar(&statusBrokerAddr, "broker", "localhost:7777", "broker address")
	statusCmd.Flags().StringVar(&statusQueue, "queue", "", "restrict the report to one queue")
	rootCmd.AddCommand(statusCmd)
}

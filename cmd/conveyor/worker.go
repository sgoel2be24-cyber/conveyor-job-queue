package main

import "github.com/spf13/cobra"

var (
	workerQueue       string
	workerConcurrency int
	workerBrokerAddr  string
)

var workerStartCmd = &cobra.Command{
	Use:   "worker",
	Short: "Start a worker pool that leases and executes jobs.",
	RunE: func(cmd *cobra.Command, args []string) error {
		return errNotImplemented("worker")
	},
}

func init() {
	workerStartCmd.Flags().StringVar(&workerQueue, "queue", "", "queue to consume from (required)")
	workerStartCmd.Flags().IntVar(&workerConcurrency, "concurrency", 4, "number of jobs to execute concurrently")
	workerStartCmd.Flags().StringVar(&workerBrokerAddr, "broker", "localhost:7777", "broker address")
	_ = workerStartCmd.MarkFlagRequired("queue")
	rootCmd.AddCommand(workerStartCmd)
}

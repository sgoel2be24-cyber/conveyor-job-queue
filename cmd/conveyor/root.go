package main

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{
	Use:   "conveyor",
	Short: "Conveyor is a durable, distributed job-processing pipeline.",
	Long: `Conveyor durably queues jobs, dispatches them to a pool of workers with
at-least-once delivery, retries failures with backoff, and dead-letters jobs
that exceed their retry budget. Everything is controlled from this CLI.`,
}

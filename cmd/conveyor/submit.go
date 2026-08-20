package main

import "github.com/spf13/cobra"

var (
	submitQueue          string
	submitPayload        string
	submitHandler        string
	submitMaxRetries     int
	submitPriority       string
	submitDelay          string
	submitIdempotencyKey string
)

var submitCmd = &cobra.Command{
	Use:   "submit",
	Short: "Submit a job to a queue.",
	RunE: func(cmd *cobra.Command, args []string) error {
		return errNotImplemented("submit")
	},
}

func init() {
	submitCmd.Flags().StringVar(&submitQueue, "queue", "", "queue name (required)")
	submitCmd.Flags().StringVar(&submitPayload, "payload", "", "job payload, JSON (required)")
	submitCmd.Flags().StringVar(&submitHandler, "handler", "shell", "handler to run this job (shell, webhook)")
	submitCmd.Flags().IntVar(&submitMaxRetries, "max-retries", 5, "maximum delivery attempts before dead-lettering")
	submitCmd.Flags().StringVar(&submitPriority, "priority", "normal", "priority: low, normal, high")
	submitCmd.Flags().StringVar(&submitDelay, "delay", "0s", "delay before the job becomes eligible for lease")
	submitCmd.Flags().StringVar(&submitIdempotencyKey, "idempotency-key", "", "dedup key for this submission")
	_ = submitCmd.MarkFlagRequired("queue")
	_ = submitCmd.MarkFlagRequired("payload")
	rootCmd.AddCommand(submitCmd)
}

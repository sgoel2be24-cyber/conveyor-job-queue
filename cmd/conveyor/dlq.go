package main

import "github.com/spf13/cobra"

var dlqBrokerAddr string

var dlqCmd = &cobra.Command{
	Use:   "dlq",
	Short: "Inspect and replay dead-lettered jobs.",
}

var dlqListCmd = &cobra.Command{
	Use:   "list",
	Short: "List dead-lettered jobs.",
	RunE: func(cmd *cobra.Command, args []string) error {
		return errNotImplemented("dlq list")
	},
}

var dlqReplayCmd = &cobra.Command{
	Use:   "replay [job-id]",
	Short: "Requeue a dead-lettered job for another attempt.",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return errNotImplemented("dlq replay")
	},
}

func init() {
	dlqCmd.PersistentFlags().StringVar(&dlqBrokerAddr, "broker", "localhost:7777", "broker address")
	dlqCmd.AddCommand(dlqListCmd, dlqReplayCmd)
	rootCmd.AddCommand(dlqCmd)
}

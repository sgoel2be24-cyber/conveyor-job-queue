package main

import "github.com/spf13/cobra"

var statusBrokerAddr string

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show queue depths, throughput, and failure counts.",
	RunE: func(cmd *cobra.Command, args []string) error {
		return errNotImplemented("status")
	},
}

func init() {
	statusCmd.Flags().StringVar(&statusBrokerAddr, "broker", "localhost:7777", "broker address")
	rootCmd.AddCommand(statusCmd)
}

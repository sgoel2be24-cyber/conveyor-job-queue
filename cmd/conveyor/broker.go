package main

import "github.com/spf13/cobra"

var (
	brokerAddr    string
	brokerDataDir string
)

var brokerCmd = &cobra.Command{
	Use:   "broker",
	Short: "Manage the broker.",
}

var brokerStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the broker (WAL-backed job store + lease dispatcher).",
	RunE: func(cmd *cobra.Command, args []string) error {
		return errNotImplemented("broker start")
	},
}

func init() {
	brokerStartCmd.Flags().StringVar(&brokerAddr, "listen", "localhost:7777", "address to listen on")
	brokerStartCmd.Flags().StringVar(&brokerDataDir, "data-dir", "./data", "directory for the WAL and snapshots")
	brokerCmd.AddCommand(brokerStartCmd)
	rootCmd.AddCommand(brokerCmd)
}

package main

import "github.com/spf13/cobra"

var (
	benchRate     int
	benchDuration string
	benchQueue    string
)

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Run a fixed load-test scenario against a broker and report latency/throughput.",
	RunE: func(cmd *cobra.Command, args []string) error {
		return errNotImplemented("bench")
	},
}

func init() {
	benchCmd.Flags().IntVar(&benchRate, "rate", 1000, "target submissions per second")
	benchCmd.Flags().StringVar(&benchDuration, "duration", "30s", "how long to run the benchmark")
	benchCmd.Flags().StringVar(&benchQueue, "queue", "bench", "queue to use for the benchmark")
	rootCmd.AddCommand(benchCmd)
}

package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"conveyor/internal/handler"
	"conveyor/internal/job"
	"conveyor/internal/worker"
)

var (
	workerBrokerAddr   string
	workerQueue        string
	workerConcurrency  int
	workerLeaseTimeout time.Duration
	workerID           string
)

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Start a worker pool that leases and executes jobs.",
	Long: `Start a worker pool.

The worker holds each job under a lease and renews it while the job runs. If the
worker dies, its leases expire and the broker hands those jobs to someone else --
so killing a worker mid-job is safe, and is worth trying:

    conveyor worker --queue emails --concurrency 4
    kill -9 <that worker>   # its jobs come back and finish elsewhere

Handlers:
  shell    runs the payload as a shell command
  webhook  sends the payload as an HTTP request, forwarding Idempotency-Key`,
	RunE: runWorker,
}

func runWorker(cmd *cobra.Command, _ []string) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	id := workerID
	if id == "" {
		id = job.NewID()[:12]
	}

	pool := &worker.Pool{
		Client:        newClient(workerBrokerAddr),
		Queue:         workerQueue,
		WorkerID:      id,
		Concurrency:   workerConcurrency,
		LeaseDuration: workerLeaseTimeout,
		Handlers:      handler.Default(),
		Logger:        logger,
	}

	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := pool.Run(ctx); err != nil {
		return err
	}
	logger.Info("worker stopped", "worker", id)
	return nil
}

func init() {
	workerCmd.Flags().StringVar(&workerBrokerAddr, "broker", "localhost:7777", "broker address")
	workerCmd.Flags().StringVar(&workerQueue, "queue", "", "queue to consume from (required)")
	workerCmd.Flags().IntVar(&workerConcurrency, "concurrency", 4, "number of jobs to execute concurrently")
	workerCmd.Flags().DurationVar(&workerLeaseTimeout, "lease-duration", 30*time.Second, "how long to hold a job before the broker may reclaim it")
	workerCmd.Flags().StringVar(&workerID, "worker-id", "", "identifier for this worker (generated if empty)")
	_ = workerCmd.MarkFlagRequired("queue")
	rootCmd.AddCommand(workerCmd)
}

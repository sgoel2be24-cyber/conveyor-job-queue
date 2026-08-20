package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/broker"
	"github.com/sgoel2be24-cyber/conveyor-job-queue/internal/genproto/conveyor/v1/conveyorv1connect"
)

var (
	brokerAddr         string
	brokerDataDir      string
	brokerSnapshotFreq time.Duration
)

var brokerCmd = &cobra.Command{
	Use:   "broker",
	Short: "Manage the broker.",
}

var brokerStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the broker (WAL-backed job store + lease dispatcher).",
	RunE:  runBroker,
}

func runBroker(cmd *cobra.Command, _ []string) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	start := time.Now()
	store, err := broker.OpenStore(brokerDataDir)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	// This is the last write the process makes. A failure here can mean the log
	// did not reach disk, which is exactly the kind of thing an operator needs
	// told rather than swallowed.
	defer func() {
		if cerr := store.Close(); cerr != nil {
			logger.Error("closing the store failed; the log may not be fully flushed", "err", cerr)
		}
	}()

	stats := store.Stats("")
	logger.Info("recovered",
		"data_dir", brokerDataDir,
		"jobs", stats.TotalJobs,
		"last_lsn", stats.LastLSN,
		"took", time.Since(start),
	)

	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The dispatcher owns lease placement and reclaim; the server just exposes
	// it. Start it before serving so a worker connecting immediately finds a
	// running loop.
	dispatcher := broker.NewDispatcher(store, logger)
	dispatcherDone := make(chan struct{})
	go func() {
		defer close(dispatcherDone)
		dispatcher.Run(ctx)
	}()

	mux := http.NewServeMux()
	path, handler := conveyorv1connect.NewBrokerServiceHandler(broker.NewServer(store, dispatcher))
	mux.Handle(path, handler)

	// Serve HTTP/2 without TLS, which the streaming Lease RPC wants and which
	// keeps gRPC clients compatible over a plaintext local socket. HTTP/1.1
	// stays enabled so plain curl still works against the Connect endpoints.
	srv := &http.Server{
		Addr:              brokerAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		Protocols:         broker.Protocols(),
	}

	// Periodic snapshots keep recovery time bounded; without them the WAL grows
	// forever even though most jobs end up terminal.
	snapDone := make(chan struct{})
	go func() {
		defer close(snapDone)
		ticker := time.NewTicker(brokerSnapshotFreq)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := store.Snapshot(); err != nil {
					logger.Error("snapshot failed", "err", err)
				}
			}
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", brokerAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
		logger.Info("shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
	}
	<-snapDone
	<-dispatcherDone

	// A final snapshot is an optimization, not a correctness requirement: every
	// acknowledged job is already durable in the WAL.
	if err := store.Snapshot(); err != nil {
		logger.Error("final snapshot failed", "err", err)
	}
	return nil
}

func init() {
	brokerStartCmd.Flags().StringVar(&brokerAddr, "listen", "localhost:7777", "address to listen on")
	brokerStartCmd.Flags().StringVar(&brokerDataDir, "data-dir", "./data", "directory for the WAL and snapshots")
	brokerStartCmd.Flags().DurationVar(&brokerSnapshotFreq, "snapshot-interval", 30*time.Second, "how often to snapshot the index and trim the WAL")
	brokerCmd.AddCommand(brokerStartCmd)
	rootCmd.AddCommand(brokerCmd)
}

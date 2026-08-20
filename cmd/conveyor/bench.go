package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	conveyorv1 "github.com/sgoel2be24-cyber/conveyor-job-queue/internal/genproto/conveyor/v1"
)

var (
	benchBrokerAddr  string
	benchQueue       string
	benchDuration    time.Duration
	benchConcurrency int
	benchRate        int
	benchPayloadSize int
)

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Measure submission throughput and latency against a running broker.",
	Long: `Measure submission throughput and latency against a running broker.

Concurrency is the interesting knob. Every submission has to reach stable
storage before it is acknowledged, and a flush costs milliseconds -- so a single
submitter is limited to roughly one job per flush no matter how fast the machine
is. Concurrent submitters share a flush between them, and throughput rises with
how many arrive together.

Compare, on the same broker:

    conveyor bench --concurrency 1
    conveyor bench --concurrency 64

The reported commit batch size explains the difference: it is the average number
of submissions that shared a single flush.

Jobs submitted by this command are real. Point it at a scratch --data-dir, or
run workers to drain the queue afterwards.`,
	RunE: runBench,
}

// result is one submission's outcome.
type result struct {
	latency time.Duration
	err     error
}

func runBench(cmd *cobra.Command, _ []string) error {
	if benchConcurrency < 1 {
		return fmt.Errorf("--concurrency must be at least 1")
	}
	if benchDuration <= 0 {
		return fmt.Errorf("--duration must be positive")
	}

	client := newClient(benchBrokerAddr)
	out := cmd.OutOrStdout()
	payload := strings.Repeat("x", benchPayloadSize)

	// Snapshot the broker's own counters so the report can show what the
	// flushing layer was doing, not just what the client observed.
	before, metricsErr := scrapeCommitStats(cmd.Context(), benchBrokerAddr)

	ctx, cancel := context.WithTimeout(cmd.Context(), benchDuration)
	defer cancel()

	var ticker *time.Ticker
	if benchRate > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(benchRate))
		defer ticker.Stop()
	}

	fmt.Fprintf(out, "submitting for %s with %d concurrent client(s)", benchDuration, benchConcurrency)
	if benchRate > 0 {
		fmt.Fprintf(out, " at %d/sec", benchRate)
	}
	fmt.Fprintln(out, "...")

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results []result
	)
	start := time.Now()

	for i := 0; i < benchConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			local := make([]result, 0, 1024)
			// Deferred so an early return still contributes its measurements.
			defer func() {
				mu.Lock()
				results = append(results, local...)
				mu.Unlock()
			}()

			for ctx.Err() == nil {
				if ticker != nil {
					select {
					case <-ticker.C:
					case <-ctx.Done():
						return
					}
				}

				sent := time.Now()
				_, err := client.Submit(ctx, connectRequest(&conveyorv1.SubmitRequest{
					Queue:      benchQueue,
					Payload:    []byte(payload),
					Handler:    "shell",
					MaxRetries: 3,
				}))
				if err != nil && ctx.Err() != nil {
					return // the deadline cut this one short; not a real failure
				}
				local = append(local, result{latency: time.Since(sent), err: err})
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	after, afterErr := scrapeCommitStats(cmd.Context(), benchBrokerAddr)
	if afterErr != nil {
		metricsErr = afterErr
	}

	report(out, results, elapsed, before, after, metricsErr)
	return nil
}

func report(out io.Writer, results []result, elapsed time.Duration, before, after commitStats, metricsErr error) {
	latencies := make([]time.Duration, 0, len(results))
	failures := 0
	for _, r := range results {
		if r.err != nil {
			failures++
			continue
		}
		latencies = append(latencies, r.latency)
	}

	fmt.Fprintf(out, "\n%-14s %d in %s\n", "submitted", len(latencies), elapsed.Round(time.Millisecond))
	if failures > 0 {
		fmt.Fprintf(out, "%-14s %d\n", "failed", failures)
	}
	if len(latencies) == 0 {
		fmt.Fprintln(out, "no successful submissions to report on")
		return
	}

	fmt.Fprintf(out, "%-14s %.1f jobs/sec\n", "throughput", float64(len(latencies))/elapsed.Seconds())

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	fmt.Fprintf(out, "%-14s p50 %s   p95 %s   p99 %s   max %s\n", "latency",
		round(percentile(latencies, 0.50)),
		round(percentile(latencies, 0.95)),
		round(percentile(latencies, 0.99)),
		round(latencies[len(latencies)-1]))

	if metricsErr != nil {
		fmt.Fprintf(out, "\n(broker metrics unavailable: %v)\n", metricsErr)
		return
	}

	flushes := after.commitCount - before.commitCount
	records := after.batchSum - before.batchSum
	if flushes <= 0 {
		return
	}
	fmt.Fprintf(out, "\n%-14s %.0f\n", "flushes", flushes)
	fmt.Fprintf(out, "%-14s %.1f submissions per flush\n", "commit batch", records/flushes)
	if secs := after.commitSeconds - before.commitSeconds; secs > 0 {
		fmt.Fprintf(out, "%-14s %s per flush\n", "flush cost",
			round(time.Duration(secs/flushes*float64(time.Second))))
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)))
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

// round trims sub-microsecond noise that would only clutter the report.
func round(d time.Duration) time.Duration {
	if d >= time.Millisecond {
		return d.Round(10 * time.Microsecond)
	}
	return d.Round(time.Microsecond)
}

// commitStats holds the broker-side flush counters this command reports on.
type commitStats struct {
	commitCount   float64
	commitSeconds float64
	batchSum      float64
}

// scrapeCommitStats reads the handful of counters the report needs straight out
// of the Prometheus exposition text. Parsing four known lines is cheaper than
// pulling in a parser for a format this simple.
func scrapeCommitStats(ctx context.Context, addr string) (commitStats, error) {
	url := addr
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}
	url = strings.TrimSuffix(url, "/") + "/metrics"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return commitStats{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return commitStats{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return commitStats{}, fmt.Errorf("GET %s: %s", url, resp.Status)
	}

	var stats commitStats
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		name, value, ok := parseMetricLine(scanner.Text())
		if !ok {
			continue
		}
		switch name {
		case "conveyor_wal_commits_total":
			stats.commitCount = value
		case "conveyor_wal_commit_seconds_sum":
			stats.commitSeconds = value
		case "conveyor_wal_commit_batch_records_sum":
			stats.batchSum = value
		}
	}
	return stats, scanner.Err()
}

// parseMetricLine splits an unlabelled Prometheus sample into its name and
// value. Labelled samples are ignored: none of the metrics read here carry any.
func parseMetricLine(line string) (string, float64, bool) {
	if line == "" || strings.HasPrefix(line, "#") {
		return "", 0, false
	}
	name, rest, ok := strings.Cut(line, " ")
	if !ok || strings.Contains(name, "{") {
		return "", 0, false
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
	if err != nil {
		return "", 0, false
	}
	return name, value, true
}

func init() {
	benchCmd.Flags().StringVar(&benchBrokerAddr, "broker", "localhost:7777", "broker address")
	benchCmd.Flags().StringVar(&benchQueue, "queue", "bench", "queue to submit to")
	benchCmd.Flags().DurationVar(&benchDuration, "duration", 10*time.Second, "how long to run")
	benchCmd.Flags().IntVar(&benchConcurrency, "concurrency", 16, "concurrent submitting clients")
	benchCmd.Flags().IntVar(&benchRate, "rate", 0, "target submissions per second (0 for unthrottled)")
	benchCmd.Flags().IntVar(&benchPayloadSize, "payload-size", 256, "payload size in bytes")
	rootCmd.AddCommand(benchCmd)
}

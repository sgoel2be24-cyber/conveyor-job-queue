package wal

import (
	"bytes"
	"fmt"
	"testing"
)

func benchWAL(b *testing.B) *WAL {
	b.Helper()
	w, err := Open(Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { w.Close() })
	return w
}

// BenchmarkAppendSync measures the cost of the strictest durability setting:
// one F_FULLFSYNC per record. This is the throughput ceiling a naive
// commit-per-write design runs into, and the baseline group commit is measured
// against.
func BenchmarkAppendSync(b *testing.B) {
	w := benchWAL(b)
	payload := bytes.Repeat([]byte("x"), 256)

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		if _, err := w.AppendSync(payload); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
}

// BenchmarkAppendGroupCommit amortizes one flush across a batch of records,
// trading a bounded window of latency for throughput. Reported ns/op is
// per-record, so it is directly comparable to BenchmarkAppendSync.
func BenchmarkAppendGroupCommit(b *testing.B) {
	for _, batch := range []int{1, 8, 32, 128, 512} {
		b.Run(fmt.Sprintf("batch_%d", batch), func(b *testing.B) {
			w := benchWAL(b)
			payload := bytes.Repeat([]byte("x"), 256)

			b.ResetTimer()
			b.SetBytes(int64(len(payload)))
			for i := 0; i < b.N; i++ {
				if _, err := w.Append(payload); err != nil {
					b.Fatalf("append: %v", err)
				}
				if (i+1)%batch == 0 {
					if err := w.Sync(); err != nil {
						b.Fatalf("sync: %v", err)
					}
				}
			}
			if err := w.Sync(); err != nil {
				b.Fatalf("final sync: %v", err)
			}
		})
	}
}

// BenchmarkAppendNoSync isolates the encoding and write path from durability
// entirely, showing how little of the per-record cost is anything other than
// waiting for the drive.
func BenchmarkAppendNoSync(b *testing.B) {
	w := benchWAL(b)
	payload := bytes.Repeat([]byte("x"), 256)

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		if _, err := w.Append(payload); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
}

// BenchmarkReplay measures crash-recovery speed: how long it takes to rebuild
// state by reading the log back.
func BenchmarkReplay(b *testing.B) {
	const records = 100_000
	w := benchWAL(b)
	payload := bytes.Repeat([]byte("x"), 256)
	for i := 0; i < records; i++ {
		if _, err := w.Append(payload); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		b.Fatalf("sync: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		if err := w.Replay(1, func(uint64, []byte) error { n++; return nil }); err != nil {
			b.Fatalf("replay: %v", err)
		}
		if n != records {
			b.Fatalf("replayed %d records, want %d", n, records)
		}
	}
	b.ReportMetric(float64(records), "records/replay")
}

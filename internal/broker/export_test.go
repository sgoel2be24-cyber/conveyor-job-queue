package broker

import "github.com/sgoel2be24-cyber/conveyor-job-queue/internal/wal"

// openSmallSegmentWAL opens a log with a deliberately tiny segment size so tests
// can exercise rotation and truncation without writing megabytes.
func openSmallSegmentWAL(dir string) (*wal.WAL, error) {
	return wal.Open(wal.Options{Dir: dir, MaxSegmentSize: 4 << 10})
}

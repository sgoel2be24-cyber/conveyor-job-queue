//go:build !darwin

package wal

import "os"

// fullSync flushes f to stable storage.
//
// This is a plain fsync. On Linux with a volatile drive write cache that is a
// weaker guarantee than the F_FULLFSYNC used on macOS; matching it would mean
// issuing a device flush (and, on many setups, disabling the write cache
// outright). Conveyor is developed and benchmarked on macOS — see
// docs/DESIGN.md — so this path exists for portability of the build, not as a
// tested durability guarantee.
func fullSync(f *os.File) error { return f.Sync() }

//go:build darwin

package wal

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// fullSync flushes f's contents all the way to stable storage.
//
// On macOS, fsync(2) only guarantees that data has reached the drive — it may
// still sit in the drive's volatile write cache, where a power loss destroys
// it. F_FULLFSYNC asks the drive to flush that cache too, which is what
// durability actually requires. It is substantially slower than a plain fsync,
// and that cost is precisely what motivates batching commits rather than
// syncing once per record (see docs/DESIGN.md).
//
// Some filesystems — network mounts in particular — do not implement
// F_FULLFSYNC and fail with ENOTSUP or EINVAL. Nothing stronger is available
// there, so fall back to fsync rather than failing the write.
func fullSync(f *os.File) error {
	if _, err := unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0); err != nil {
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EINVAL) {
			return f.Sync()
		}
		return err
	}
	return nil
}

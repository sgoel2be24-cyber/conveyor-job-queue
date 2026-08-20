// Package wal implements a crash-safe, append-only write-ahead log.
//
// The log is a sequence of size-rotated segment files holding length-prefixed,
// checksummed records. Every record is assigned a monotonically increasing
// sequence number (LSN) starting at 1. A single writer appends; readers replay
// from any LSN forward.
//
// Record layout:
//
//	[length uint32 LE][crc32c uint32 LE][payload length bytes]
//
// The checksum covers the length field as well as the payload, so a corrupted
// length that still falls within bounds is caught when the payload it implies
// fails to checksum.
//
// # Crash safety
//
// A process killed mid-append leaves a partial record at the tail of the active
// segment. Replay treats the first record it cannot fully read or verify as the
// point where the crash happened and stops there cleanly, rather than erroring
// out or — worse — interpreting the garbage. Open goes one step further and
// truncates the file back to the last valid record boundary, so that subsequent
// appends do not land after an unreadable gap that would hide them from the
// next replay.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	// headerSize is the per-record overhead: 4-byte length + 4-byte checksum.
	headerSize = 8

	// DefaultMaxSegmentSize is the size at which the active segment is rotated.
	DefaultMaxSegmentSize = 16 << 20

	// MaxRecordSize bounds a single payload. It also serves as a sanity check
	// during replay: a length field larger than this means we are reading
	// garbage rather than a record, so replay stops instead of trying to
	// allocate whatever the corrupt header claims.
	MaxRecordSize = 64 << 20

	segmentSuffix = ".wal"
)

// crcTable uses the Castagnoli polynomial, which has hardware support on both
// arm64 and amd64 — the checksum is therefore not a meaningful cost next to the
// fsync that follows it.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

var (
	// ErrClosed is returned by operations on a closed WAL.
	ErrClosed = errors.New("wal: closed")
	// ErrEmptyPayload is returned when appending a zero-length payload. Empty
	// records are disallowed so that a zero length field during replay
	// unambiguously means "this is not a record".
	ErrEmptyPayload = errors.New("wal: empty payload")
	// ErrRecordTooLarge is returned when a payload exceeds MaxRecordSize.
	ErrRecordTooLarge = errors.New("wal: record too large")
)

// Options configures a WAL.
type Options struct {
	// Dir is the directory holding segment files. Created if absent.
	Dir string
	// MaxSegmentSize is the size at which the active segment is rotated.
	// Defaults to DefaultMaxSegmentSize.
	MaxSegmentSize int64
}

// WAL is an append-only, crash-safe log, safe for concurrent use.
//
// # Appending is decoupled from writing
//
// Append does not touch the file. It frames the record into an in-memory
// buffer and returns; the bytes reach the disk when someone calls Sync, which
// writes the whole accumulated buffer in one go and then flushes it.
//
// This split is what makes group commit possible. The naive alternative --
// write(2) on the caller's path -- looks cheap, and usually is: a write takes
// microseconds. But a write that lands while the drive is busy with an
// F_FULLFSYNC blocks for the rest of that flush, and callers hold their own
// locks while they append. One unlucky writer then stalls every other writer
// for milliseconds, so records trickle in one at a time and each ends up paying
// for a flush of its own. Buffering removes the syscall from the caller's path
// entirely, so writers keep arriving at full speed while the disk works and a
// single flush can cover hundreds of them.
//
// The durability contract is unchanged: a record is on stable storage once a
// Sync that began after the record was appended has returned successfully.
// Records still sitting in the buffer when a process dies are lost, which is
// correct -- nobody was told they were durable.
//
// # Locking
//
// mu guards the buffer and append bookkeeping, and is held only for the
// microseconds it takes to frame a record. Sync takes mu just long enough to
// write the buffer out, then flushes with mu released.
//
// fileMu guards which file is active, so a flush is never left holding a handle
// that rotation has closed underneath it. It is held for reading while flushing
// and for writing while rotating; the lock order is always mu -> fileMu.
type WAL struct {
	dir            string
	maxSegmentSize int64

	fileMu sync.RWMutex
	active *os.File
	closed bool

	mu         sync.Mutex
	buf        []byte   // records appended but not yet written to the file
	bases      []uint64 // sorted base LSNs, one per segment file
	activeBase uint64
	// activeSize counts bytes already written plus bytes still buffered, since
	// both are destined for the active segment.
	activeSize int64
	nextLSN    uint64
}

// Open opens (or creates) the log in opts.Dir, repairing a torn tail left by a
// previous crash.
func Open(opts Options) (*WAL, error) {
	if opts.Dir == "" {
		return nil, errors.New("wal: Dir is required")
	}
	if opts.MaxSegmentSize <= 0 {
		opts.MaxSegmentSize = DefaultMaxSegmentSize
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}

	bases, err := listSegments(opts.Dir)
	if err != nil {
		return nil, err
	}

	w := &WAL{dir: opts.Dir, maxSegmentSize: opts.MaxSegmentSize, bases: bases}

	if len(bases) == 0 {
		if err := w.createSegment(1); err != nil {
			return nil, err
		}
		w.nextLSN = 1
		return w, nil
	}

	// Only the last segment can hold a torn record: earlier ones were closed
	// and synced before rotation.
	base := bases[len(bases)-1]
	path := segmentPath(opts.Dir, base)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open segment %s: %w", path, err)
	}

	validEnd, count, err := scanRecords(f, nil)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("wal: scan segment %s: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if info.Size() > validEnd {
		// Discard the partial record a crash left behind, so the next append
		// does not land beyond a gap that replay would stop at.
		if err := f.Truncate(validEnd); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("wal: truncate torn tail: %w", err)
		}
		if err := fullSync(f); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("wal: sync after truncate: %w", err)
		}
	}
	if _, err := f.Seek(validEnd, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}

	w.active = f
	w.activeBase = base
	w.activeSize = validEnd
	w.nextLSN = base + count
	return w, nil
}

// Append writes payload to the log without flushing it to stable storage. The
// record is durable only after a subsequent Sync (or an AppendSync).
func (w *WAL) Append(payload []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendLocked(payload)
}

// AppendSync writes payload and flushes it all the way to stable storage,
// returning only once the record would survive a power loss.
//
// The append lock is released before flushing, so this does not stall other
// writers for the duration of the disk write. A concurrent append may therefore
// be made durable by the same flush, which is harmless -- the guarantee here is
// that this record is durable on return, not that it is the only one.
func (w *WAL) AppendSync(payload []byte) (uint64, error) {
	w.mu.Lock()
	lsn, err := w.appendLocked(payload)
	w.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if err := w.Sync(); err != nil {
		return 0, fmt.Errorf("wal: sync: %w", err)
	}
	return lsn, nil
}

func (w *WAL) appendLocked(payload []byte) (uint64, error) {
	if w.closed {
		return 0, ErrClosed
	}
	if len(payload) == 0 {
		return 0, ErrEmptyPayload
	}
	if len(payload) > MaxRecordSize {
		return 0, ErrRecordTooLarge
	}

	size := int64(headerSize + len(payload))
	// activeSize > 0 keeps a single oversized record from rotating forever.
	if w.activeSize > 0 && w.activeSize+size > w.maxSegmentSize {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
	}

	// Frame straight into the pending buffer; no syscall on this path.
	off := len(w.buf)
	w.buf = append(w.buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(w.buf[off:off+4], uint32(len(payload)))
	w.buf = append(w.buf, payload...)
	h := crc32.New(crcTable)
	h.Write(w.buf[off : off+4])
	h.Write(payload)
	binary.LittleEndian.PutUint32(w.buf[off+4:off+8], h.Sum32())

	w.activeSize += size
	lsn := w.nextLSN
	w.nextLSN++
	return lsn, nil
}

// flushBufferLocked writes pending records to the active segment without
// flushing them to stable storage. Callers must hold mu.
func (w *WAL) flushBufferLocked() error {
	if len(w.buf) == 0 {
		return nil
	}
	if _, err := w.active.Write(w.buf); err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	w.buf = w.buf[:0]
	return nil
}

// Sync writes every record appended so far and flushes them to stable storage.
//
// mu is held only for the single write that drains the buffer, never across the
// flush itself, so callers keep appending while the disk works -- which is what
// lets the next flush cover a whole batch. See the type comment.
func (w *WAL) Sync() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	if err := w.flushBufferLocked(); err != nil {
		w.mu.Unlock()
		return err
	}
	w.mu.Unlock()

	// A rotation slipping in here is harmless: it flushes and syncs the segment
	// we just wrote to before closing it, so those records are durable either
	// way.
	w.fileMu.RLock()
	defer w.fileMu.RUnlock()
	if w.closed {
		return ErrClosed
	}
	return fullSync(w.active)
}

// rotateLocked closes the active segment and starts a new one.
//
// fileMu is held exclusively for the whole swap, which is what stops a
// concurrent flush from being handed a file descriptor that is closed out from
// under it. Rotation happens once per segment (16MiB by default), so blocking
// flushes for its duration costs nothing measurable.
//
// Callers must hold mu.
func (w *WAL) rotateLocked() error {
	// Anything still buffered belongs to the segment being closed, so it has to
	// go out before the handle is swapped.
	if err := w.flushBufferLocked(); err != nil {
		return err
	}

	w.fileMu.Lock()
	defer w.fileMu.Unlock()

	if err := fullSync(w.active); err != nil {
		return fmt.Errorf("wal: sync before rotate: %w", err)
	}
	if err := w.active.Close(); err != nil {
		return fmt.Errorf("wal: close before rotate: %w", err)
	}
	return w.createSegmentLocked(w.nextLSN)
}

// createSegment starts a new segment, taking fileMu itself. Used where no
// rotation is in progress.
//
// Callers must hold mu.
func (w *WAL) createSegment(base uint64) error {
	w.fileMu.Lock()
	defer w.fileMu.Unlock()
	return w.createSegmentLocked(base)
}

// createSegmentLocked starts a new segment. Callers must hold mu and fileMu.
func (w *WAL) createSegmentLocked(base uint64) error {
	path := segmentPath(w.dir, base)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("wal: create segment %s: %w", path, err)
	}
	// Make the new directory entry itself durable, not just the file contents:
	// a crash could otherwise leave records written to a file that does not
	// appear in the directory after reboot.
	if err := syncDir(w.dir); err != nil {
		_ = f.Close()
		return fmt.Errorf("wal: sync dir: %w", err)
	}
	w.active = f
	w.activeBase = base
	w.activeSize = 0
	w.bases = append(w.bases, base)
	return nil
}

// LastLSN returns the sequence number of the most recently appended record, or
// 0 if the log is empty.
func (w *WAL) LastLSN() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextLSN - 1
}

// Replay invokes fn for every record with an LSN >= from, in order. Replay
// stops cleanly at a torn or corrupt record rather than reporting an error: see
// the package comment on crash safety.
func (w *WAL) Replay(from uint64, fn func(lsn uint64, payload []byte) error) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	// Replay reads files, so anything still buffered has to be written first or
	// it would be invisible to the caller that just appended it.
	if err := w.flushBufferLocked(); err != nil {
		w.mu.Unlock()
		return err
	}
	bases := append([]uint64(nil), w.bases...)
	w.mu.Unlock()

	for i, base := range bases {
		// Every record in this segment precedes the next segment's base, so if
		// that base is already below the cutoff the whole file can be skipped
		// without reading it.
		nextBase := ^uint64(0)
		if i+1 < len(bases) {
			nextBase = bases[i+1]
		}
		if nextBase <= from {
			continue
		}

		path := segmentPath(w.dir, base)
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("wal: open segment %s: %w", path, err)
		}
		ordinal := uint64(0)
		_, _, err = scanRecords(f, func(payload []byte) error {
			lsn := base + ordinal
			ordinal++
			if lsn < from {
				return nil
			}
			return fn(lsn, payload)
		})
		_ = f.Close()
		if err != nil {
			return fmt.Errorf("wal: replay segment %s: %w", path, err)
		}
	}
	return nil
}

// TruncateBefore deletes whole segments whose records all precede lsn. It is
// used after a snapshot has been durably written: the records the snapshot
// already accounts for no longer need to be replayed, and without this the log
// would grow without bound even though most jobs are terminal.
//
// The active segment is never deleted.
func (w *WAL) TruncateBefore(lsn uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}

	removed := 0
	for i := 0; i+1 < len(w.bases); i++ {
		if w.bases[i+1] > lsn {
			break
		}
		path := segmentPath(w.dir, w.bases[i])
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("wal: remove segment %s: %w", path, err)
		}
		removed++
	}
	if removed > 0 {
		w.bases = append([]uint64(nil), w.bases[removed:]...)
		if err := syncDir(w.dir); err != nil {
			return fmt.Errorf("wal: sync dir after truncate: %w", err)
		}
	}
	return nil
}

// Close flushes and closes the log. It waits for any flush in progress, so a
// caller cannot pull the file out from under one.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.fileMu.Lock()
	defer w.fileMu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.flushBufferLocked(); err != nil {
		_ = w.active.Close()
		return err
	}
	if err := fullSync(w.active); err != nil {
		_ = w.active.Close()
		return err
	}
	return w.active.Close()
}

// scanRecords reads records from r in order, invoking fn for each valid one. It
// returns the byte offset just past the last valid record along with the number
// of records read.
//
// A truncated or failing-checksum record is treated as the point where a crash
// interrupted a write: scanning stops there and returns what came before it.
// Only genuine I/O errors (and errors from fn) are reported.
func scanRecords(r io.Reader, fn func(payload []byte) error) (validEnd int64, count uint64, err error) {
	br := bufio.NewReader(r)
	hdr := make([]byte, headerSize)

	for {
		if _, err := io.ReadFull(br, hdr); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return validEnd, count, nil // clean end, or a torn header
			}
			return validEnd, count, err
		}

		length := binary.LittleEndian.Uint32(hdr[0:4])
		want := binary.LittleEndian.Uint32(hdr[4:8])
		// Zero length means we are reading unwritten (zeroed) space rather than
		// a record; anything past the cap means the header is garbage.
		if length == 0 || length > MaxRecordSize {
			return validEnd, count, nil
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(br, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return validEnd, count, nil // torn payload
			}
			return validEnd, count, err
		}

		h := crc32.New(crcTable)
		h.Write(hdr[0:4])
		h.Write(payload)
		if h.Sum32() != want {
			return validEnd, count, nil // torn or corrupt record
		}

		if fn != nil {
			if err := fn(payload); err != nil {
				return validEnd, count, err
			}
		}
		validEnd += headerSize + int64(length)
		count++
	}
}

func segmentName(base uint64) string {
	return fmt.Sprintf("%020d%s", base, segmentSuffix)
}

func segmentPath(dir string, base uint64) string {
	return filepath.Join(dir, segmentName(base))
}

func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: read dir: %w", err)
	}
	var bases []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segmentSuffix) {
			continue
		}
		base, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), segmentSuffix), 10, 64)
		if err != nil {
			continue // not one of ours
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

// syncDir flushes a directory entry. Directories take a plain fsync:
// F_FULLFSYNC is about a drive's write cache for file data and is not
// applicable here.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}

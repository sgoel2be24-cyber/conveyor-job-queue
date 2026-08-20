package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// collect replays the whole log into a slice.
func collect(t *testing.T, w *WAL, from uint64) []string {
	t.Helper()
	var got []string
	err := w.Replay(from, func(lsn uint64, payload []byte) error {
		got = append(got, string(payload))
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return got
}

func mustOpen(t *testing.T, dir string, maxSeg int64) *WAL {
	t.Helper()
	w, err := Open(Options{Dir: dir, MaxSegmentSize: maxSeg})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return w
}

// onlySegment returns the path of the sole segment file, failing if there is
// more than one.
func onlySegment(t *testing.T, dir string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("want exactly 1 segment, got %d: %v", len(matches), matches)
	}
	return matches[0]
}

func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, 0)

	want := []string{"alpha", "beta", "gamma"}
	for i, s := range want {
		lsn, err := w.AppendSync([]byte(s))
		if err != nil {
			t.Fatalf("append %q: %v", s, err)
		}
		if got := uint64(i + 1); lsn != got {
			t.Errorf("append %q: LSN = %d, want %d", s, lsn, got)
		}
	}
	if got := w.LastLSN(); got != 3 {
		t.Errorf("LastLSN = %d, want 3", got)
	}

	got := collect(t, w, 1)
	if len(got) != len(want) {
		t.Fatalf("replayed %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("record %d = %q, want %q", i, got[i], want[i])
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestReplayFromSkipsEarlierRecords(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, 0)
	defer w.Close()

	for _, s := range []string{"a", "b", "c", "d"} {
		if _, err := w.AppendSync([]byte(s)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	got := collect(t, w, 3)
	want := []string{"c", "d"}
	if len(got) != len(want) {
		t.Fatalf("replayed %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("record %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestEmptyPayloadRejected(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, 0)
	defer w.Close()

	if _, err := w.Append(nil); err != ErrEmptyPayload {
		t.Errorf("Append(nil) error = %v, want %v", err, ErrEmptyPayload)
	}
}

// TestTornTailAtEveryOffset is the core crash-safety test: it simulates a
// process killed at every possible byte offset within the final record and
// asserts that reopening the log recovers exactly the records that were
// completely written, never a partial one.
func TestTornTailAtEveryOffset(t *testing.T) {
	const payload = "durable-record"
	recordSize := int64(headerSize + len(payload))

	for cut := int64(1); cut <= recordSize; cut++ {
		t.Run(fmt.Sprintf("cut_%d_bytes", cut), func(t *testing.T) {
			dir := t.TempDir()

			w := mustOpen(t, dir, 0)
			for i := 0; i < 3; i++ {
				if _, err := w.AppendSync([]byte(payload)); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			// Chop bytes off the tail, as an interrupted write would.
			path := onlySegment(t, dir)
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat: %v", err)
			}
			if err := os.Truncate(path, info.Size()-cut); err != nil {
				t.Fatalf("truncate: %v", err)
			}

			w2 := mustOpen(t, dir, 0)
			defer w2.Close()

			got := collect(t, w2, 1)
			if len(got) != 2 {
				t.Fatalf("recovered %d records, want 2 (the torn third must be dropped)", len(got))
			}
			for i, rec := range got {
				if rec != payload {
					t.Errorf("record %d = %q, want %q", i, rec, payload)
				}
			}

			// The torn bytes must be gone, so the next write lands where replay
			// will find it rather than behind an unreadable gap.
			if got := w2.LastLSN(); got != 2 {
				t.Errorf("LastLSN after recovery = %d, want 2", got)
			}
			lsn, err := w2.AppendSync([]byte("after-recovery"))
			if err != nil {
				t.Fatalf("append after recovery: %v", err)
			}
			if lsn != 3 {
				t.Errorf("LSN after recovery = %d, want 3", lsn)
			}
			if got := collect(t, w2, 1); len(got) != 3 || got[2] != "after-recovery" {
				t.Errorf("replay after recovery = %v, want 3 records ending in %q", got, "after-recovery")
			}
		})
	}
}

// TestCorruptChecksumStopsReplay covers bit rot rather than a torn write: a
// record whose contents no longer match its checksum ends the log, and the
// records after it are discarded rather than replayed across a hole.
func TestCorruptChecksumStopsReplay(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, 0)
	for _, s := range []string{"first", "second", "third"} {
		if _, err := w.AppendSync([]byte(s)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	path := onlySegment(t, dir)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Flip a payload bit inside the second record.
	secondPayloadStart := headerSize + len("first") + headerSize
	data[secondPayloadStart] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	w2 := mustOpen(t, dir, 0)
	defer w2.Close()

	got := collect(t, w2, 1)
	if len(got) != 1 || got[0] != "first" {
		t.Fatalf("replayed %v, want only [first]", got)
	}
	if got := w2.LastLSN(); got != 1 {
		t.Errorf("LastLSN = %d, want 1", got)
	}
}

func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	// Small enough that each record forces a new segment quickly.
	w := mustOpen(t, dir, 128)

	const n = 40
	for i := 0; i < n; i++ {
		if _, err := w.AppendSync([]byte(fmt.Sprintf("record-%03d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	segments, err := listSegments(dir)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(segments) < 2 {
		t.Fatalf("got %d segments, want rotation to have produced several", len(segments))
	}

	got := collect(t, w, 1)
	if len(got) != n {
		t.Fatalf("replayed %d records across %d segments, want %d", len(got), len(segments), n)
	}
	for i, rec := range got {
		if want := fmt.Sprintf("record-%03d", i); rec != want {
			t.Fatalf("record %d = %q, want %q", i, rec, want)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Rotation must survive a reopen with LSNs intact.
	w2 := mustOpen(t, dir, 128)
	defer w2.Close()
	if got := w2.LastLSN(); got != n {
		t.Errorf("LastLSN after reopen = %d, want %d", got, n)
	}
}

func TestTruncateBeforeDropsCoveredSegments(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, 128)
	defer w.Close()

	const n = 40
	for i := 0; i < n; i++ {
		if _, err := w.AppendSync([]byte(fmt.Sprintf("record-%03d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	before, err := listSegments(dir)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	// Everything below the last segment's base LSN is now redundant.
	cutoff := before[len(before)-1]
	if err := w.TruncateBefore(cutoff); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	after, err := listSegments(dir)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(after) >= len(before) {
		t.Fatalf("segments after truncate = %d, want fewer than %d", len(after), len(before))
	}
	if after[0] != cutoff {
		t.Errorf("oldest remaining segment base = %d, want %d", after[0], cutoff)
	}

	// The active segment is never removed, so replay still yields its records.
	got := collect(t, w, 1)
	if len(got) == 0 {
		t.Fatal("replay after truncate returned nothing, want the surviving segment's records")
	}
	if want := fmt.Sprintf("record-%03d", cutoff-1); got[0] != want {
		t.Errorf("first surviving record = %q, want %q", got[0], want)
	}
	if got := w.LastLSN(); got != n {
		t.Errorf("LastLSN = %d, want %d", got, n)
	}
}

func TestOperationsAfterCloseFail(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, 0)
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := w.Append([]byte("x")); err != ErrClosed {
		t.Errorf("Append after Close = %v, want %v", err, ErrClosed)
	}
	if err := w.Sync(); err != ErrClosed {
		t.Errorf("Sync after Close = %v, want %v", err, ErrClosed)
	}
}

package job

import (
	"testing"
	"time"
)

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	const base = time.Second
	const capDelay = 30 * time.Second

	var prevMin time.Duration
	for attempt := 1; attempt <= 12; attempt++ {
		// Sample repeatedly, since each call is jittered.
		var min, max time.Duration
		for i := 0; i < 200; i++ {
			d := Backoff(attempt, base, capDelay)
			if i == 0 || d < min {
				min = d
			}
			if d > max {
				max = d
			}
		}

		if max > capDelay {
			t.Errorf("attempt %d produced %s, above the %s cap", attempt, max, capDelay)
		}
		if min <= 0 {
			t.Errorf("attempt %d produced a non-positive delay %s", attempt, min)
		}
		// Half the delay is fixed, so the floor must rise until the cap is hit.
		if attempt > 1 && min < prevMin && prevMin < capDelay/2 {
			t.Errorf("attempt %d floor %s dropped below attempt %d's %s", attempt, min, attempt-1, prevMin)
		}
		prevMin = min
	}
}

// TestBackoffIsJittered guards the property that actually matters in an
// incident: jobs that fail together must not retry in lockstep.
func TestBackoffIsJittered(t *testing.T) {
	seen := make(map[time.Duration]int)
	for i := 0; i < 500; i++ {
		seen[Backoff(5, time.Second, time.Minute)]++
	}
	if len(seen) < 100 {
		t.Errorf("500 calls produced only %d distinct delays; retries would cluster", len(seen))
	}
}

func TestBackoffNeverOverflows(t *testing.T) {
	// A huge attempt count must not shift a duration into negative territory.
	for _, attempt := range []int{62, 63, 64, 100, 1 << 20} {
		d := Backoff(attempt, time.Second, time.Minute)
		if d <= 0 {
			t.Errorf("attempt %d produced %s, want a positive delay", attempt, d)
		}
		if d > time.Minute {
			t.Errorf("attempt %d produced %s, above the cap", attempt, d)
		}
	}
}

func TestBackoffHandlesZeroConfig(t *testing.T) {
	if d := Backoff(0, 0, 0); d <= 0 || d > DefaultBackoffCap {
		t.Errorf("Backoff(0,0,0) = %s, want a sane positive delay", d)
	}
}

func TestStatePredicates(t *testing.T) {
	leasable := map[State]bool{
		StatePending:    true,
		StateRetryWait:  true,
		StateLeased:     false,
		StateDone:       false,
		StateDeadLetter: false,
	}
	for state, want := range leasable {
		if got := state.Leasable(); got != want {
			t.Errorf("%s.Leasable() = %v, want %v", state, got, want)
		}
	}

	terminal := map[State]bool{
		StateDone:       true,
		StateDeadLetter: true,
		StatePending:    false,
		StateRetryWait:  false,
		StateLeased:     false,
	}
	for state, want := range terminal {
		if got := state.Terminal(); got != want {
			t.Errorf("%s.Terminal() = %v, want %v", state, got, want)
		}
	}
}

func TestParsePriority(t *testing.T) {
	for input, want := range map[string]Priority{
		"low": PriorityLow, "normal": PriorityNormal, "high": PriorityHigh,
		"": PriorityNormal, "HIGH": PriorityHigh, "  low  ": PriorityLow,
	} {
		got, err := ParsePriority(input)
		if err != nil {
			t.Errorf("ParsePriority(%q) errored: %v", input, err)
			continue
		}
		if got != want {
			t.Errorf("ParsePriority(%q) = %s, want %s", input, got, want)
		}
	}
	if _, err := ParsePriority("urgent"); err == nil {
		t.Error("ParsePriority(\"urgent\") should have failed")
	}
}

func TestNewIDIsUniqueAndSortable(t *testing.T) {
	const n = 1000
	seen := make(map[string]bool, n)
	prev := ""
	for i := 0; i < n; i++ {
		id := NewID()
		if seen[id] {
			t.Fatalf("duplicate ID %s at iteration %d", id, i)
		}
		seen[id] = true
		if len(id) != 32 {
			t.Fatalf("ID %q has length %d, want 32", id, len(id))
		}
		if id < prev {
			t.Errorf("ID %s sorts before its predecessor %s", id, prev)
		}
		prev = id
	}
}

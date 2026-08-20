package job

import (
	"math/rand/v2"
	"time"
)

// Backoff defaults. Exported so the broker can override them per deployment and
// tests can shrink them.
const (
	DefaultBackoffBase = time.Second
	DefaultBackoffCap  = 5 * time.Minute
)

// maxBackoffShift bounds the exponent so that base<<attempt cannot overflow a
// time.Duration on a job that has failed many times.
const maxBackoffShift = 32

// Backoff returns how long to wait before retrying a job whose attempt-th
// delivery just failed.
//
// The delay grows exponentially up to a cap, then half of it is replaced by
// randomness ("equal jitter"). The jitter matters more than the growth: without
// it, a batch of jobs failing together -- the usual case, since they usually
// fail for the same reason -- retries in lockstep forever, hammering whatever
// is already unhealthy at exactly the same instants. Half-fixed, half-random
// spreads the retries out while still guaranteeing a minimum wait.
//
// Full randomization (delay = rand(0, d)) spreads slightly better but allows
// near-instant retries, which is the wrong behavior when the failure is a
// dependency that needs a moment to recover.
func Backoff(attempt int, base, capDelay time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if base <= 0 {
		base = DefaultBackoffBase
	}
	if capDelay <= 0 {
		capDelay = DefaultBackoffCap
	}

	shift := attempt - 1
	if shift > maxBackoffShift {
		shift = maxBackoffShift
	}

	d := base << shift
	if d <= 0 || d > capDelay { // d <= 0 catches overflow
		d = capDelay
	}

	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(rand.Int64N(int64(half)))
}

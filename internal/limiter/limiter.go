package limiter

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"
const (
	AlgorithmTokenBucket          Algorithm = "token_bucket"
	AlgorithmSlidingWindowLog     Algorithm = "sliding_window_log"
	AlgorithmSlidingWindowCounter Algorithm = "sliding_window_counter"
	AlgorithmFixedWindow          Algorithm = "fixed_window"
)

// Result describes the evaluation outcome of a rate-limiting evaluation.
// Directly implements the IETF RateLimit Headers specification:
// https://datatracker.ietf.org/doc/draft-ietf-httpapi-ratelimit-headers/
type Result struct {
	Allowed    bool          // True if request is within quota
	Limit      int           // Quota ceiling for the current window/bucket
	Remaining  int           // Remaining allowed requests before throttling
	ResetAt    time.Time     // Absolute timestamp when the limit fully resets/refills
	RetryAfter time.Duration // Duration to wait before retrying (0 if Allowed is true)
}

// RetryAfterSeconds returns integer delta seconds client must wait.
// Complies with RFC 9110 § 10.2.3 (HTTP Semantics: Retry-After).
// Uses zero-allocation integer arithmetic.
func (r Result) RetryAfterSeconds() int {
	if r.RetryAfter <= 0 {
		if !r.Allowed {
			return 1 // Guard against 0s infinite tight retry loop
		}
		return 0
	}
	// Integer ceiling: (d + 1s - 1ns) / 1s
	return int((r.RetryAfter + time.Second - 1) / time.Second)
}

// ResetSeconds returns integer delta seconds until ResetAt relative to 'now'.
// Complies with IETF RateLimit-Reset header specification.
func (r Result) ResetSeconds(now time.Time) int {
	if !now.Before(r.ResetAt) {
		if !r.Allowed {
			return 1
		}
		return 0
	}
	diff := r.ResetAt.Sub(now)
	return int((diff + time.Second - 1) / time.Second)
}

// WriteHeaders applies standard IETF and RFC 9110 rate limit headers
// directly onto an http.Header or ResponseWriter.
func (r Result) WriteHeaders(h http.Header, now time.Time) {
	h.Set("RateLimit-Limit", strconv.Itoa(r.Limit))
	h.Set("RateLimit-Remaining", strconv.Itoa(r.Remaining))
	h.Set("RateLimit-Reset", strconv.Itoa(r.ResetSeconds(now)))

	if !r.Allowed {
		h.Set("Retry-After", strconv.Itoa(r.RetryAfterSeconds()))
	}
}

// Limiter defines the universal contract for rate-limiting engines.
type Limiter interface {
	// Allow evaluates whether a single unit is permitted for key.
	Allow(ctx context.Context, key string) (Result, error)

	// AllowN evaluates whether n units are permitted for key.
	// Implementations MUST return ErrInvalidBatchSize if n <= 0.
	AllowN(ctx context.Context, key string, n int) (Result, error)

	// Inspect queries quota status without consuming tokens/slots.
	Inspect(ctx context.Context, key string) (Result, error)

	// Reset resets or clears the quota for the specified key immediately.
	Reset(ctx context.Context, key string) error
}

// Reservation represents an intent to consume a token in the future.
type Reservation struct {
	Allowed bool
	Delay   time.Duration // Duration caller must sleep before proceeding
}

// WaitLimiter adds queueing/blocking rate limit semantics for background workers.
type WaitLimiter interface {
	Limiter

	// Reserve returns a Reservation indicating how long to wait before token is available.
	Reserve(ctx context.Context, key string) (Reservation, error)

	// Wait blocks until a token becomes available or ctx is cancelled.
	Wait(ctx context.Context, key string) error
}

package limiter

import (
	"context"
	"io"
	"math"
	"time"
)

// Result describes the evaluation outcome of a rate-limiting request.
// Conforms directly with IETF RateLimit Headers specification
// (draft-ietf-httpapi-ratelimit-headers).
type Result struct {
	Allowed    bool          // True if request is within quota
	Limit      int           // Quota ceiling for the current window/bucket
	Remaining  int           // Remaining allowed requests before throttling
	ResetAt    time.Time     // Absolute timestamp when the limit fully resets or refills
	RetryAfter time.Duration // Duration client must wait before retrying (0 if Allowed is true)
}

// RetryAfterSeconds returns RetryAfter duration rounded UP to full seconds,
// complying with RFC 9110 (HTTP Semantics) specifications for integer Retry-After headers.
func (r Result) RetryAfterSeconds() int {
	if r.RetryAfter <= 0 {
		return 0
	}
	return int(math.Ceil(r.RetryAfter.Seconds()))
}

// ResetSeconds returns delta seconds until ResetAt from the reference time 'now'.
func (r Result) ResetSeconds(now time.Time) int {
	diff := r.ResetAt.Sub(now).Seconds()
	if diff <= 0 {
		return 0
	}
	return int(math.Ceil(diff))
}

// Limiter defines the core behavior of a rate-limiting engine.
type Limiter interface {
	// Allow evaluates whether a single unit is permitted for the given key.
	// It is equivalent to AllowN(ctx, key, 1).
	Allow(ctx context.Context, key string) (Result, error)

	// AllowN evaluates whether n units are permitted for the given key.
	// Implementations MUST return an error if n <= 0.
	AllowN(ctx context.Context, key string, n int) (Result, error)

	// Inspect queries the current quota without consuming tokens.
	Inspect(ctx context.Context, key string) (Result, error)
}

// CloserLimiter combines Limiter with io.Closer for engines requiring graceful
// cleanup of background goroutines, connection pools, or cache sync flush.
type CloserLimiter interface {
	Limiter
	io.Closer
}

// Algorithm represents the rate-limiting mathematical model.
type Algorithm string

const (
	AlgorithmTokenBucket         Algorithm = "token_bucket"
	AlgorithmSlidingWindowLog    Algorithm = "sliding_window_log"
	AlgorithmSlidingWindowCounter Algorithm = "sliding_window_counter"
	AlgorithmFixedWindow         Algorithm = "fixed_window"
)

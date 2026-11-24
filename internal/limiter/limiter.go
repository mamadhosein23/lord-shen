package limiter

import (
	"context"
	"errors"
	"io"
	"math"
	"time"
)

// Sentinel errors for standard contract assertions.
var (
	ErrInvalidBatchSize = errors.New("limiter: batch count n must be greater than 0")
	ErrKeyNotFound      = errors.New("limiter: rate limit key not found")
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

// RetryAfterSeconds returns RetryAfter duration rounded UP to the nearest full second.
// Returns 0 if request was allowed or RetryAfter <= 0.
// Conforms with RFC 9110 (HTTP Semantics) § 10.2.3.
func (r Result) RetryAfterSeconds() int {
	if r.RetryAfter <= 0 {
		return 0
	}
	sec := int(math.Ceil(r.RetryAfter.Seconds()))
	if sec == 0 && !r.Allowed {
		return 1 // Prevent tight 0-second retry storms on throttled clients
	}
	return sec
}

// ResetSeconds returns the delta seconds until ResetAt relative to reference time 'now'.
// Rounded up to full seconds for draft-ietf-httpapi-ratelimit-headers compliance.
func (r Result) ResetSeconds(now time.Time) int {
	if !now.Before(r.ResetAt) {
		return 0
	}
	diff := r.ResetAt.Sub(now)
	sec := int(math.Ceil(diff.Seconds()))
	if sec <= 0 && !r.Allowed {
		return 1
	}
	return sec
}

// Limiter defines the core behavior of a rate-limiting engine.
type Limiter interface {
	// Allow evaluates whether a single unit is permitted for the given key.
	// Equivalent to AllowN(ctx, key, 1).
	Allow(ctx context.Context, key string) (Result, error)

	// AllowN evaluates whether n units are permitted for the given key.
	// Returns ErrInvalidBatchSize if n <= 0.
	AllowN(ctx context.Context, key string, n int) (Result, error)

	// Inspect queries current quota without consuming tokens.
	Inspect(ctx context.Context, key string) (Result, error)
}

// CloserLimiter joins Limiter and io.Closer for engines managing
// explicit background loops, redis pool teardowns, or memory sweepers.
type CloserLimiter interface {
	Limiter
	io.Closer
}

// Algorithm represents the rate-limiting mathematical model.
type Algorithm string

const (
	AlgorithmTokenBucket          Algorithm = "token_bucket"
	AlgorithmSlidingWindowLog     Algorithm = "sliding_window_log"
	AlgorithmSlidingWindowCounter Algorithm = "sliding_window_counter"
	AlgorithmFixedWindow          Algorithm = "fixed_window"
)

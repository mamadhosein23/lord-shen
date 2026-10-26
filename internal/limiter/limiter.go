package limiter

import (
	"context"
	"io"
	"time"
)

// Result describes the evaluation outcome of a rate-limiting request.
// Conforms directly with standard HTTP RateLimit headers (RFC draft-ietf-httpapi-ratelimit-headers).
type Result struct {
	Allowed    bool          // True if request is within quota
	Limit      int           // Quota ceiling for the current window/bucket
	Remaining  int           // Remaining allowed requests before throttling
	ResetAt    time.Time     // Absolute timestamp when the limit resets or refills
	RetryAfter time.Duration // Duration the client must wait before retrying (0 if Allowed)
}

// Limiter defines the core behavior of a rate-limiting engine.
type Limiter interface {
	// Allow evaluates whether a single action is permitted for the given key.
	Allow(ctx context.Context, key string) (Result, error)

	// AllowN evaluates whether n units are permitted for the given key.
	AllowN(ctx context.Context, key string, n int) (Result, error)
}

// CloserLimiter combines Limiter with io.Closer for engines requiring resource cleanup (e.g. background workers).
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

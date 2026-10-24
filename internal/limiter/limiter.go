package limiter

import (
	"context"
	"time"
)

// Result describes the evaluation outcome of a rate-limiting request.
type Result struct {
	Allowed    bool
	Limit      int
	Remaining  int
	ResetAt    time.Time
	RetryAfter time.Duration
}

// Limiter defines the core interface for all rate-limiting engines.
type Limiter interface {
	Allow(ctx context.Context, key string) (Result, error)
	Close() error
}

// Strategy represents the algorithm type.
type Strategy string

const (
	StrategyTokenBucket   Strategy = "token_bucket"
	StrategySlidingWindow Strategy = "sliding_window"
	StrategyRedis         Strategy = "redis"
)

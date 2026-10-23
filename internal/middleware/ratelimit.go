package limiter

import (
	"context"
	"math"
	"sync"
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

// Limiter defines the core interface for rate-limiting engines.
type Limiter interface {
	Allow(ctx context.Context, key string) (Result, error)
	Close() error
}

type bucket struct {
	mu       sync.Mutex
	tokens   float64
	lastSeen time.Time
}

// Config holds operational parameters for the TokenBucket engine.
type Config struct {
	Rate         float64
	Burst        int
	CleanupEvery time.Duration
	KeyTTL       time.Duration
}

// TokenBucket provides a concurrent-safe, isolated in-memory rate limiter using the token bucket algorithm.
type TokenBucket struct {
	cfg     Config
	mu      sync.RWMutex
	buckets map[string]*bucket
	now     func() time.Time
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewTokenBucket creates a new TokenBucket engine and starts the background janitor goroutine.
func NewTokenBucket(cfg Config) *TokenBucket {
	if cfg.CleanupEvery <= 0 {
		cfg.CleanupEvery = time.Minute
	}
	if cfg.KeyTTL <= 0 {
		cfg.KeyTTL = 10 * time.Minute
	}

	tb := &TokenBucket{
		cfg:     cfg,
		buckets: make(map[string]*bucket),
		now:     time.Now,
		stopCh:  make(chan struct{}),
	}

	tb.wg.Add(1)
	go tb.janitor()

	return tb
}

// Allow evaluates whether the specified key is permitted to consume a token.
func (tb *TokenBucket) Allow(_ context.Context, key string) (Result, error) {
	b := tb.getOrCreateBucket(key)

	b.mu.Lock()
	defer b.mu.Unlock()

	now := tb.now()
	elapsed := now.Sub(b.lastSeen).Seconds()
	b.lastSeen = now

	// Replenish tokens based on elapsed time and generation rate
	b.tokens = math.Min(float64(tb.cfg.Burst), b.tokens+elapsed*tb.cfg.Rate)

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		remaining := int(b.tokens)
		fillDuration := time.Duration((float64(tb.cfg.Burst)-b.tokens)/tb.cfg.Rate * float64(time.Second))

		return Result{
			Allowed:    true,
			Limit:      tb.cfg.Burst,
			Remaining:  remaining,
			ResetAt:    now.Add(fillDuration),
			RetryAfter: 0,
		}, nil
	}

	// Reject request and calculate required wait duration
	missing := 1.0 - b.tokens
	retry := time.Duration(missing / tb.cfg.Rate * float64(time.Second))

	return Result{
		Allowed:    false,
		Limit:      tb.cfg.Burst,
		Remaining:  0,
		ResetAt:    now.Add(retry),
		RetryAfter: retry,
	}, nil
}

// getOrCreateBucket retrieves an existing bucket or safely creates a new one using double-checked locking.
func (tb *TokenBucket) getOrCreateBucket(key string) *bucket {
	tb.mu.RLock()
	b, ok := tb.buckets[key]
	tb.mu.RUnlock()
	if ok {
		return b
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	if b, ok = tb.buckets[key]; ok {
		return b
	}

	b = &bucket{
		tokens:   float64(tb.cfg.Burst),
		lastSeen: tb.now(),
	}
	tb.buckets[key] = b
	return b
}

// Close gracefully shuts down the background janitor cleanup process.
func (tb *TokenBucket) Close() error {
	select {
	case <-tb.stopCh:
		// Already closed
		return nil
	default:
		close(tb.stopCh)
	}
	tb.wg.Wait()
	return nil
}

// janitor periodically purges expired buckets from memory to prevent memory leaks.
func (tb *TokenBucket) janitor() {
	defer tb.wg.Done()
	ticker := time.NewTicker(tb.cfg.CleanupEvery)
	defer ticker.Stop()

	for {
		select {
		case <-tb.stopCh:
			return
		case <-ticker.C:
			cutoff := tb.now().Add(-tb.cfg.KeyTTL)

			tb.mu.Lock()
			for k, b := range tb.buckets {
				b.mu.Lock()
				if b.lastSeen.Before(cutoff) {
					delete(tb.buckets, k)
				}
				b.mu.Unlock()
			}
			tb.mu.Unlock()
		}
	}
}

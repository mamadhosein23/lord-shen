package limiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

var errClosed = errors.New("token bucket is closed")

// TokenBucket is a concurrent-safe, in-memory token bucket rate limiter.
type TokenBucket struct {
	rate         float64
	burst        int
	cleanupEvery time.Duration
	keyTTL       time.Duration

	// Always acquire mu before acquiring a bucket's mutex.
	mu      sync.RWMutex
	buckets map[string]*bucket
	closed  bool

	now       func() time.Time
	stopCh    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

type bucket struct {
	mu       sync.Mutex
	tokens   float64
	lastSeen time.Time
}

var _ CloserLimiter = (*TokenBucket)(nil)

// NewTokenBucket creates a limiter and starts its idle-bucket cleanup worker.
func NewTokenBucket(
	rate float64,
	burst int,
	cleanupEvery time.Duration,
	keyTTL time.Duration,
) (*TokenBucket, error) {
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return nil, fmt.Errorf("rate must be a positive finite number, got %v", rate)
	}
	if burst <= 0 {
		return nil, fmt.Errorf("burst must be > 0, got %d", burst)
	}

	// Float64 cannot represent every integer above 2^53 exactly.
	if uint64(burst) > 1<<53 {
		return nil, fmt.Errorf("burst must be <= 2^53, got %d", burst)
	}

	if cleanupEvery <= 0 {
		cleanupEvery = time.Minute
	}
	if keyTTL <= 0 {
		keyTTL = 10 * time.Minute
	}

	// Evicting a bucket before it can fully refill would restore its
	// entire quota early.
	if float64(burst)/rate > keyTTL.Seconds() {
		return nil, fmt.Errorf(
			"keyTTL (%v) must be >= bucket refill time (%g seconds)",
			keyTTL,
			float64(burst)/rate,
		)
	}

	tb := &TokenBucket{
		rate:         rate,
		burst:        burst,
		cleanupEvery: cleanupEvery,
		keyTTL:       keyTTL,
		buckets:      make(map[string]*bucket),
		now:          time.Now,
		stopCh:       make(chan struct{}),
	}

	tb.wg.Add(1)
	go tb.janitor()

	return tb, nil
}

// Allow evaluates whether one token can be consumed for key.
func (tb *TokenBucket) Allow(ctx context.Context, key string) (Result, error) {
	return tb.AllowN(ctx, key, 1)
}

// AllowN atomically consumes n tokens when enough tokens are available.
func (tb *TokenBucket) AllowN(
	ctx context.Context,
	key string,
	n int,
) (Result, error) {
	if n <= 0 {
		return Result{}, errors.New("n must be > 0")
	}
	if n > tb.burst {
		return Result{}, fmt.Errorf(
			"n (%d) must be <= burst (%d)",
			n,
			tb.burst,
		)
	}

	b, err := tb.lockBucket(ctx, key)
	if err != nil {
		return Result{}, err
	}
	defer b.mu.Unlock()

	now := tb.refill(b)
	allowed := b.tokens >= float64(n)

	if allowed {
		b.tokens -= float64(n)
	}

	return tb.result(b, now, n, allowed), nil
}

// Inspect reports the current quota without consuming a token.
// Like AllowN, it refreshes the bucket's idle timestamp.
func (tb *TokenBucket) Inspect(
	ctx context.Context,
	key string,
) (Result, error) {
	b, err := tb.lockBucket(ctx, key)
	if err != nil {
		return Result{}, err
	}
	defer b.mu.Unlock()

	now := tb.refill(b)
	return tb.result(b, now, 1, b.tokens >= 1), nil
}

// lockBucket returns with the bucket mutex held.
//
// Holding the map lock until the bucket lock is acquired prevents the
// cleanup worker from deleting a bucket while a request starts using it.
func (tb *TokenBucket) lockBucket(
	ctx context.Context,
	key string,
) (*bucket, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tb.mu.RLock()
	if tb.closed {
		tb.mu.RUnlock()
		return nil, errClosed
	}

	if b, ok := tb.buckets[key]; ok {
		b.mu.Lock()
		tb.mu.RUnlock()

		if err := ctx.Err(); err != nil {
			b.mu.Unlock()
			return nil, err
		}
		return b, nil
	}
	tb.mu.RUnlock()

	tb.mu.Lock()
	if tb.closed {
		tb.mu.Unlock()
		return nil, errClosed
	}
	if err := ctx.Err(); err != nil {
		tb.mu.Unlock()
		return nil, err
	}

	b, ok := tb.buckets[key]
	if !ok {
		b = &bucket{
			tokens:   float64(tb.burst),
			lastSeen: tb.now(),
		}
		tb.buckets[key] = b
	}

	b.mu.Lock()
	tb.mu.Unlock()

	if err := ctx.Err(); err != nil {
		b.mu.Unlock()
		return nil, err
	}
	return b, nil
}

// refill adds earned tokens and records the latest activity time.
// The caller must hold the bucket mutex.
func (tb *TokenBucket) refill(b *bucket) time.Time {
	now := tb.now()

	// Do not move the bucket clock backward if the supplied clock does.
	if now.Before(b.lastSeen) {
		now = b.lastSeen
	}

	elapsed := now.Sub(b.lastSeen).Seconds()
	b.tokens = math.Min(
		float64(tb.burst),
		b.tokens+elapsed*tb.rate,
	)
	b.lastSeen = now

	return now
}

// result builds a consistent response after an AllowN or Inspect operation.
// The caller must hold the bucket mutex.
func (tb *TokenBucket) result(
	b *bucket,
	now time.Time,
	n int,
	allowed bool,
) Result {
	fullRefill := tb.waitFor(float64(tb.burst) - b.tokens)

	res := Result{
		Allowed:   allowed,
		Limit:     tb.burst,
		Remaining: int(math.Floor(b.tokens)),
		ResetAt:   now.Add(fullRefill),
	}

	if !allowed {
		res.RetryAfter = tb.waitFor(float64(n) - b.tokens)
	}

	return res
}

// waitFor rounds up so a retry is never scheduled before enough tokens
// have accumulated. It saturates at the largest time.Duration.
func (tb *TokenBucket) waitFor(missing float64) time.Duration {
	if missing <= 0 {
		return 0
	}

	nanoseconds := math.Ceil(
		(missing / tb.rate) * float64(time.Second),
	)

	if nanoseconds >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	if nanoseconds < 1 {
		return time.Nanosecond
	}
	return time.Duration(nanoseconds)
}

// Close stops cleanup and waits for the worker to exit.
// Subsequent calls to Allow, AllowN, and Inspect return an error.
func (tb *TokenBucket) Close() error {
	tb.closeOnce.Do(func() {
		tb.mu.Lock()
		tb.closed = true
		close(tb.stopCh)
		tb.mu.Unlock()
	})

	tb.wg.Wait()
	return nil
}

// janitor removes buckets that have been idle for at least keyTTL.
func (tb *TokenBucket) janitor() {
	defer tb.wg.Done()

	ticker := time.NewTicker(tb.cleanupEvery)
	defer ticker.Stop()

	for {
		select {
		case <-tb.stopCh:
			return
		case <-ticker.C:
			tb.cleanup()
		}
	}
}

// cleanup snapshots keys, then checks each deletion under the map and
// bucket locks. This avoids holding the map lock for the entire scan.
func (tb *TokenBucket) cleanup() {
	cutoff := tb.now().Add(-tb.keyTTL)

	tb.mu.RLock()
	keys := make([]string, 0, len(tb.buckets))
	for key := range tb.buckets {
		keys = append(keys, key)
	}
	tb.mu.RUnlock()

	for _, key := range keys {
		select {
		case <-tb.stopCh:
			return
		default:
		}

		tb.mu.RLock()
		b := tb.buckets[key]
		if b == nil {
			tb.mu.RUnlock()
			continue
		}

		b.mu.Lock()
		expired := !b.lastSeen.After(cutoff)
		b.mu.Unlock()
		tb.mu.RUnlock()

		if !expired {
			continue
		}

		tb.mu.Lock()

		// A request may have refreshed the bucket after the first check,
		// or a different bucket may now occupy the same key.
		if tb.buckets[key] == b {
			b.mu.Lock()
			if !b.lastSeen.After(cutoff) {
				delete(tb.buckets, key)
			}
			b.mu.Unlock()
		}

		tb.mu.Unlock()
	}
}

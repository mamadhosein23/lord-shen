package limiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// TokenBucket is an in-memory token bucket rate limiter.
type TokenBucket struct {
	rate         float64
	burst        int
	cleanupEvery time.Duration
	keyTTL       time.Duration

	mu      sync.RWMutex
	buckets map[string]*bucket

	now    func() time.Time
	stopCh chan struct{}
	wg     sync.WaitGroup
}

type bucket struct {
	mu       sync.Mutex
	tokens   float64
	lastSeen time.Time
}

func NewTokenBucket(rate float64, burst int, cleanupEvery, keyTTL time.Duration) (*TokenBucket, error) {
	if rate <= 0 {
		return nil, fmt.Errorf("rate must be > 0, got %f", rate)
	}
	if burst <= 0 {
		return nil, fmt.Errorf("burst must be > 0, got %d", burst)
	}
	if cleanupEvery <= 0 {
		cleanupEvery = time.Minute
	}
	if keyTTL <= 0 {
		keyTTL = 10 * time.Minute
	}

	// Optional but recommended consistency check
	minRefill := time.Duration((float64(burst)/rate)*float64(time.Second))
	if keyTTL < minRefill {
		return nil, fmt.Errorf("keyTTL (%v) must be >= bucket refill time (%v)", keyTTL, minRefill)
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

func (tb *TokenBucket) Allow(ctx context.Context, key string) (Result, error) {
	return tb.AllowN(ctx, key, 1)
}

func (tb *TokenBucket) AllowN(_ context.Context, key string, n int) (Result, error) {
	if n <= 0 {
		return Result{}, errors.New("n must be > 0")
	}

	b := tb.getOrCreate(key)

	b.mu.Lock()
	defer b.mu.Unlock()

	now := tb.now()

	// Refill
	elapsed := now.Sub(b.lastSeen).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	b.tokens = math.Min(float64(tb.burst), b.tokens+elapsed*tb.rate)
	b.lastSeen = now

	// Allow?
	if b.tokens >= float64(n) {
		b.tokens -= float64(n)

		remaining := int(math.Floor(b.tokens))
		nextTokenIn := time.Duration((1.0/tb.rate)*float64(time.Second))

		return Result{
			Allowed:    true,
			Limit:      tb.burst,
			Remaining:  remaining,
			ResetAt:    now.Add(nextTokenIn), // time until next token (more meaningful than "full refill")
			RetryAfter: 0,
		}, nil
	}

	// Deny
	missing := float64(n) - b.tokens
	retry := time.Duration((missing/tb.rate)*float64(time.Second))
	if retry < 0 {
		retry = 0
	}

	return Result{
		Allowed:    false,
		Limit:      tb.burst,
		Remaining:  int(math.Floor(b.tokens)),
		ResetAt:    now.Add(retry),
		RetryAfter: retry,
	}, nil
}

func (tb *TokenBucket) getOrCreate(key string) *bucket {
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
		tokens:   float64(tb.burst),
		lastSeen: tb.now(),
	}
	tb.buckets[key] = b
	return b
}

func (tb *TokenBucket) Close() error {
	select {
	case <-tb.stopCh:
		return nil
	default:
		close(tb.stopCh)
	}
	tb.wg.Wait()
	return nil
}

// janitor removes idle buckets without holding map lock for long durations.
func (tb *TokenBucket) janitor() {
	defer tb.wg.Done()

	ticker := time.NewTicker(tb.cleanupEvery)
	defer ticker.Stop()

	for {
		select {
		case <-tb.stopCh:
			return
		case <-ticker.C:
			cutoff := tb.now().Add(-tb.keyTTL)

			// 1) Snapshot keys + bucket pointers under RLock
			tb.mu.RLock()
			snap := make(map[string]*bucket, len(tb.buckets))
			for k, b := range tb.buckets {
				snap[k] = b
			}
			tb.mu.RUnlock()

			// 2) Decide deletions without holding map lock
			var toDelete []string
			for k, b := range snap {
				b.mu.Lock()
				expired := b.lastSeen.Before(cutoff)
				b.mu.Unlock()
				if expired {
					toDelete = append(toDelete, k)
				}
			}

			// 3) Delete under Lock (no bucket lock needed; safe enough)
			if len(toDelete) > 0 {
				tb.mu.Lock()
				for _, k := range toDelete {
					// defensive: ensure still same pointer and still expired
					if b, ok := tb.buckets[k]; ok {
						b.mu.Lock()
						expired := b.lastSeen.Before(cutoff)
						b.mu.Unlock()
						if expired {
							delete(tb.buckets, k)
						}
					}
				}
				tb.mu.Unlock()
			}
		}
	}
}

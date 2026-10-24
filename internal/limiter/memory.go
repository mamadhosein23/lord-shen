package limiter

import (
	"context"
	"math"
	"sync"
	"time"
)

// TokenBucket memory-based rate limiter (our existing implementation refined).
type TokenBucket struct {
	rate         float64
	burst        int
	cleanupEvery time.Duration
	keyTTL       time.Duration
	mu           sync.RWMutex
	buckets      map[string]*bucket
	now          func() time.Time
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

type bucket struct {
	mu       sync.Mutex
	tokens   float64
	lastSeen time.Time
}

func NewTokenBucket(rate float64, burst int, cleanupEvery, keyTTL time.Duration) *TokenBucket {
	if cleanupEvery <= 0 {
		cleanupEvery = time.Minute
	}
	if keyTTL <= 0 {
		keyTTL = 10 * time.Minute
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
	return tb
}

func (tb *TokenBucket) Allow(_ context.Context, key string) (Result, error) {
	b := tb.getOrCreate(key)
	b.mu.Lock()
	defer b.mu.Unlock()

	now := tb.now()
	elapsed := now.Sub(b.lastSeen).Seconds()
	b.lastSeen = now

	b.tokens = math.Min(float64(tb.burst), b.tokens+elapsed*tb.rate)

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		fillDuration := time.Duration((float64(tb.burst)-b.tokens)/tb.rate * float64(time.Second))
		return Result{
			Allowed:    true,
			Limit:      tb.burst,
			Remaining:  int(b.tokens),
			ResetAt:    now.Add(fillDuration),
			RetryAfter: 0,
		}, nil
	}

	missing := 1.0 - b.tokens
	retry := time.Duration(missing / tb.rate * float64(time.Second))
	return Result{
		Allowed:    false,
		Limit:      tb.burst,
		Remaining:  0,
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

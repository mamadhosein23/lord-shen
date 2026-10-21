package limiter

import (
	"context"
	"math"
	"sync"
	"time"
)

// Result نتیجه‌ی ارزیابی یک درخواست را توصیف می‌کند.
type Result struct {
	Allowed    bool
	Limit      int
	Remaining  int
	ResetAt    time.Time
	RetryAfter time.Duration
}

// Limiter قرارداد اصلی موتور نرخ‌دهی است.
type Limiter interface {
	Allow(ctx context.Context, key string) (Result, error)
	Close() error
}

type bucket struct {
	mu       sync.Mutex
	tokens   float64
	lastSeen time.Time
}

// Config تنظیمات عملیاتی موتور TokenBucket است.
type Config struct {
	Rate         float64
	Burst        int
	CleanupEvery time.Duration
	KeyTTL       time.Duration
}

// TokenBucket پیاده‌سازی هم‌روند و امن در حافظه.
type TokenBucket struct {
	cfg     Config
	mu      sync.RWMutex
	buckets map[string]*bucket
	now     func() time.Time
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewTokenBucket یک موتور جدید می‌سازد و پاکساز را به همراه Graceful Shutdown مقداردهی می‌کند.
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

// Allow بررسی می‌کند آیا درخواست مربوط به کلید مجاز است یا خیر.
func (tb *TokenBucket) Allow(_ context.Context, key string) (Result, error) {
	b := tb.getOrCreateBucket(key)

	b.mu.Lock()
	defer b.mu.Unlock()

	now := tb.now()
	elapsed := now.Sub(b.lastSeen).Seconds()
	b.lastSeen = now

	// شارژ توکن‌ها با رعایت سقف Burst
	b.tokens = math.Min(float64(tb.cfg.Burst), b.tokens+elapsed*tb.cfg.Rate)

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		remaining := int(b.tokens)
		// زمان لازم تا پر شدن کامل باکت (Burst)
		fillDuration := time.Duration((float64(tb.cfg.Burst)-b.tokens)/tb.cfg.Rate * float64(time.Second))

		return Result{
			Allowed:    true,
			Limit:      tb.cfg.Burst,
			Remaining:  remaining,
			ResetAt:    now.Add(fillDuration),
			RetryAfter: 0,
		}, nil
	}

	// در صورت رد شدن: محاسبه دقیق کسری تا دسترسی به ۱ توکن
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

func (tb *TokenBucket) getOrCreateBucket(key string) *bucket {
	tb.mu.RLock()
	b, ok := tb.buckets[key]
	tb.mu.RUnlock()
	if ok {
		return b
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	// Double-check locking جهت جلوگیری از ایجاد مجدد در ریس‌کاندیشن
	if b, ok = tb.buckets[key]; ok {
		return b
	}

	now := tb.now()
	b = &bucket{
		tokens:   float64(tb.cfg.Burst),
		lastSeen: now,
	}
	tb.buckets[key] = b
	return b
}

// Close برای جلوگیری از نشت منابع و اتمام کار گوروتین پاکساز.
func (tb *TokenBucket) Close() error {
	close(tb.stopCh)
	tb.wg.Wait()
	return nil
}

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
				isExpired := b.lastSeen.Before(cutoff)
				b.mu.Unlock()

				if isExpired {
					delete(tb.buckets, k)
				}
			}
			tb.mu.Unlock()
		}
	}
}

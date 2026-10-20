package limiter

import (
	"context"
	"sync"
	"time"
)

// Result نتیجه‌ی یک درخواست را توصیف می‌کند.
type Result struct {
	Allowed   bool
	Limit     int
	Remaining int
	ResetAt   time.Time
	RetryAfter time.Duration // در صورت رد شدن
}

// Limiter قرارداد اصلی موتور نرخ‌دهی است.
// هر پیاده‌سازی (In-Memory یا Redis) باید این را رعایت کند.
type Limiter interface {
	Allow(ctx context.Context, key string) (Result, error)
}

// ---------- Token Bucket ----------

// bucket ظرفیت و وضعیت فعلی یک توکن باکت است.
type bucket struct {
	tokens   float64
	lastSeen time.Time
}

// Config تنظیمات موتور است.
type Config struct {
	Rate      float64       // توکن‌های تولیدشده در هر ثانیه
	Burst     int           // حداکثر ظرفیت باکت (اجازه‌ی Burst)
	CleanupEvery time.Duration // فاصله‌ی پاکسازی کلیدهای منقضی
	KeyTTL    time.Duration // عمر یک کلید بدون فعالیت
}

// TokenBucket پیاده‌سازی In-Memory و Concurrent-Safe است.
type TokenBucket struct {
	cfg Config
	mu   sync.Mutex
	buckets map[string]*bucket
	now  func() time.Time // برای تست‌پذیری (قابلیت تزریق زمان)
}

// NewTokenBucket یک موتور جدید می‌سازد و پاکساز پس‌زمینه را شروع می‌کند.
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
	}
	go tb.janitor() // Goroutine پاکسازی حافظه
	return tb
}

// Allow بررسی می‌کند آیا کلید (مثلاً IP کاربر) مجاز به درخواست است یا نه.
func (tb *TokenBucket) Allow(_ context.Context, key string) (Result, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := tb.now()
	b, ok := tb.buckets[key]
	if !ok {
		b = &bucket{tokens: float64(tb.cfg.Burst), lastSeen: now}
		tb.buckets[key] = b
	}

	// شارژ توکن بر اساس زمان سپری‌شده
	elapsed := now.Sub(b.lastSeen).Seconds()
	b.tokens = min(float64(tb.cfg.Burst), b.tokens+elapsed*tb.cfg.Rate)
	b.lastSeen = now

	if b.tokens >= 1 {
		b.tokens--
		reset := now.Add(time.Until(timeOfNextToken(b.tokens, tb.cfg.Rate)))
		return Result{
			Allowed:   true,
			Limit:     tb.cfg.Burst,
			Remaining: int(b.tokens),
			ResetAt:   reset,
		}, nil
	}

	// رد شد؛ محاسبه‌ی زمان انتظار برای توکن بعدی
	retry := time.Duration((1 - b.tokens) / tb.cfg.Rate * float64(time.Second))
	return Result{
		Allowed:    false,
		Limit:      tb.cfg.Burst,
		Remaining:  0,
		ResetAt:    now.Add(retry),
		RetryAfter: retry,
	}, nil
}

// timeOfNextToken زمان رسیدن توکن بعدی را برمی‌گرداند (برای هدر Reset).
func timeOfNextToken(tokens, rate float64) time.Duration {
	return time.Duration((1 - tokens) / rate * float64(time.Second))
}

// janitor کلیدهای منقضی را به‌صورت دوره‌ای حذف می‌کند (جلوگیری از Memory Leak).
func (tb *TokenBucket) janitor() {
	ticker := time.NewTicker(tb.cfg.CleanupEvery)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := tb.now().Add(-tb.cfg.KeyTTL)
		tb.mu.Lock()
		for k, b := range tb.buckets {
			if b.lastSeen.Before(cutoff) {
				delete(tb.buckets, k)
			}
		}
		tb.mu.Unlock()
	}
}

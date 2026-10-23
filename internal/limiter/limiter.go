package limiter

import (
	"sync"
	"time"
)

// Client رکورد مربوط به تعداد درخواست‌های هر کلاینت را نگهداری می‌کند.
type Client struct {
	mu        sync.Mutex
	count     int
	resetTime time.Time
}

// RateLimiter ساختار اصلی مدیریت نرخ درخواست‌هاست.
type RateLimiter struct {
	mu      sync.RWMutex
	clients map[string]*Client
	rate    int           // حداکثر تعداد درخواست مجاز در بازه زمانی
	window  time.Duration // طول بازه زمانی
}

// NewRateLimiter یک نمونه جدید از RateLimiter می‌سازد.
func NewRateLimiter(rate int, window time.Duration) *RateLimiter {
	limiter := &RateLimiter{
		clients: make(map[string]*Client),
		rate:    rate,
		window:  window,
	}

	// راه‌اندازی گوروتین پس‌زمینه برای پاکسازی داده‌های منقضی‌شده (جلوگیری از Memory Leak)
	go limiter.cleanupLoop(1 * time.Minute)

	return limiter
}

// Allow بررسی می‌کند آیا IP مورد نظر مجاز به ارسال درخواست است یا خیر.
// خروجی دوم تعداد درخواست‌های باقی‌مانده و خروجی سوم زمان ریست شدن پنجره را برمی‌گرداند.
func (rl *RateLimiter) Allow(ip string) (bool, int, time.Time) {
	rl.mu.Lock()
	client, exists := rl.clients[ip]
	if !exists {
		client = &Client{
			resetTime: time.Now().Add(rl.window),
		}
		rl.clients[ip] = client
	}
	rl.mu.Unlock()

	client.mu.Lock()
	defer client.mu.Unlock()

	now := time.Now()

	// اگر بازه زمانی فعلی منقضی شده باشد، پنجره را ریست می‌کنیم
	if now.After(client.resetTime) {
		client.count = 0
		client.resetTime = now.Add(rl.window)
	}

	// اگر به حد مجاز رسیده باشد
	if client.count >= rl.rate {
		remaining := 0
		return false, remaining, client.resetTime
	}

	client.count++
	remaining := rl.rate - client.count
	return true, remaining, client.resetTime
}

// cleanupLoop به صورت دوره‌ای کلاینت‌های قدیمی را از حافظه پاک می‌کند.
func (rl *RateLimiter) cleanupLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		for ip, client :=5 client.resetTime.Before(now) {
			client.mu.Lock()
			if now.After(client.resetTime) {
				delete(rl.clients, ip)
			}
			client.mu.Unlock()
		}
		rl.mu.Unlock()
	}
}

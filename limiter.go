package main

import (
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"
)

type Client struct {
	count       int
	windowStart time.Time
}

type RateLimiter struct {
	mu      sync.Mutex
	clients map[string]*Client
	limit   int
	window  time.Duration
}

func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		clients: make(map[string]*Client),
		limit:   limit,
		window:  window,
	}
}

func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}

		rl.mu.Lock()
		defer rl.mu.Unlock()

		now := time.Now()
		client, exists := rl.clients[ip]

		// تنظیم هدرهای استاندارد ریت لیمیت
		w.Header().Set("X-RateLimit-Limit", string(rune(rl.limit)))

		if !exists || now.Sub(client.windowStart) > rl.window {
			rl.clients[ip] = &Client{count: 1, windowStart: now}
			w.Header().Set("X-RateLimit-Remaining", string(rune(rl.limit-1)))
			next.ServeHTTP(w, r)
			return
		}

		if client.count < rl.limit {
			client.count++
			remaining := rl.limit - client.count
			w.Header().Set("X-RateLimit-Remaining", string(rune(remaining)))
			next.ServeHTTP(w, r)
			return
		}

		// اگر از حد مجاز گذشت
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":   "Too Many Requests",
			"message": "لرد شن اجازه عبور نمی‌دهد! ترافیک بیش از حد مجاز است.",
			"retry_after_seconds": int(rl.window.Seconds()),
		})
	})
}

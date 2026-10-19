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

		if !exists {
			rl.clients[ip] = &Client{count: 1, windowStart: now}
			next.ServeHTTP(w, r)
			return
		}

		if now.Sub(client.windowStart) > rl.window {
			client.count = 1
			client.windowStart = now
			next.ServeHTTP(w, r)
			return
		}

		if client.count < rl.limit {
			client.count++
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(map[string]string{
			"error":   "Too Many Requests",
			"message": "لرد شن اجازه ورود نمی‌دهد! درخواست‌های شما بیش از حد مجاز است.",
		})
	})
}

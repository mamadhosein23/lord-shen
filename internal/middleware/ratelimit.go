package middleware

import (
	"context"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"
)

type LimiterBackend interface {
	Allow(ctx context.Context, key string, capacity int64, ratePerSecond float64) (bool, int64, time.Duration, error)
}

type RateLimitConfig struct {
	Capacity      int64
	RatePerSecond float64
	KeyExtractor  func(r *http.Request) string
}

func RateLimit(limiter LimiterBackend, cfg RateLimitConfig) func(http.Handler) http.Handler {
	if cfg.KeyExtractor == nil {
		cfg.KeyExtractor = defaultClientIPExtractor
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := cfg.KeyExtractor(r)
			allowed, remaining, retryAfter, err := limiter.Allow(r.Context(), key, cfg.Capacity, cfg.RatePerSecond)

			// Resilience Pattern: Fail-open on storage error to avoid dropping legitimate users
			if err != nil {
				log.Printf("[RateLimiter Error] Key: %s, Err: %v. Degrading gracefully (Fail-Open).", key, err)
				next.ServeHTTP(w, r)
				return
			}

			// Add IETF compliant headers
			w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(cfg.Capacity, 10))
			w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(remaining, 10))

			if !allowed {
				retryAfterSec := int(retryAfter.Seconds())
				if retryAfterSec < 1 {
					retryAfterSec = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(retryAfterSec))
				http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func defaultClientIPExtractor(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

package middleware

import (
	"context"
	"log"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type LimiterBackend interface {
	Allow(ctx context.Context, key string, capacity int64, ratePerSecond float64) (bool, int64, time.Duration, error)
}

type RateLimitConfig struct {
	Capacity      int64
	RatePerSecond float64
	KeyExtractor  func(r *http.Request) string

	// Optional:
	Timeout time.Duration // timeout for limiter backend call (e.g. Redis)
	KeyPrefix string      // e.g. "rl:http:" (optional)
}

func RateLimit(limiter LimiterBackend, cfg RateLimitConfig) func(http.Handler) http.Handler {
	if cfg.KeyExtractor == nil {
		cfg.KeyExtractor = defaultClientIPExtractor
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "rl:http:"
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rawKey := strings.TrimSpace(cfg.KeyExtractor(r))
			if rawKey == "" {
				rawKey = "unknown"
			}
			key := cfg.KeyPrefix + rawKey

			ctx, cancel := context.WithTimeout(r.Context(), cfg.Timeout)
			defer cancel()

			allowed, remaining, retryAfter, err := limiter.Allow(ctx, key, cfg.Capacity, cfg.RatePerSecond)

			// Fail-open on storage error to avoid dropping legitimate users
			if err != nil {
				log.Printf("[RateLimiter Error] Key=%q Err=%v (fail-open)", key, err)
				next.ServeHTTP(w, r)
				return
			}

			// Prefer RFC 9331 headers (IETF standard).
			// Keep X-RateLimit-* too if you want backward compatibility.
			limitStr := strconv.FormatInt(cfg.Capacity, 10)
			remStr := strconv.FormatInt(max0i64(remaining), 10)

			w.Header().Set("RateLimit-Limit", limitStr)
			w.Header().Set("RateLimit-Remaining", remStr)

			// Backward compatibility (optional)
			w.Header().Set("X-RateLimit-Limit", limitStr)
			w.Header().Set("X-RateLimit-Remaining", remStr)

			if !allowed {
				retryAfterSec := retryAfterSeconds(retryAfter)

				// RFC 9110: Retry-After is seconds or HTTP-date. Use seconds here.
				w.Header().Set("Retry-After", strconv.Itoa(retryAfterSec))

				// RFC 9331 reset is seconds until reset (approx for token bucket: same as retry when blocked)
				w.Header().Set("RateLimit-Reset", strconv.Itoa(retryAfterSec))

				http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
				return
			}

			// When allowed, reset can be 0 (approx).
			w.Header().Set("RateLimit-Reset", "0")

			next.ServeHTTP(w, r)
		})
	}
}

func defaultClientIPExtractor(r *http.Request) string {
	// IMPORTANT: Only trust these headers if you are behind a trusted proxy/CDN
	// that overwrites them. Otherwise they can be spoofed by clients.
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		parts := strings.Split(xff, ",")
		ip := strings.TrimSpace(parts[0])
		if ip != "" {
			return ip
		}
	}
	if xrip := strings.TrimSpace(r.Header.Get("X-Real-IP")); xrip != "" {
		return xrip
	}

	ip, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		return strings.TrimSpace(r.RemoteAddr)
	}
	return ip
}

func retryAfterSeconds(d time.Duration) int {
	if d <= 0 {
		return 1
	}
	sec := int(math.Ceil(d.Seconds()))
	if sec < 1 {
		return 1
	}
	return sec
}

func max0i64(x int64) int64 {
	if x < 0 {
		return 0
	}
	return x
}

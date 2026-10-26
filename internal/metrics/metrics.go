package middleware

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"lord-shen/internal/limiter"
	"lord-shen/internal/metrics"
)

type ErrorDetail struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
}

// KeyExtractor defines how to derive the rate limit key from an HTTP request.
type KeyExtractor func(r *http.Request) string

// IPKeyExtractor extracts client IP address.
// Note: Trust proxy headers only if your deployment guarantees they are set by a trusted proxy/CDN.
// Otherwise, prefer RemoteAddr or validate known proxy ranges.
func IPKeyExtractor(r *http.Request) string {
	if cfIP := strings.TrimSpace(r.Header.Get("CF-Connecting-IP")); cfIP != "" {
		return cfIP
	}
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
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

// APIKeyExtractor extracts key from Authorization header, otherwise falls back to IP.
func APIKeyExtractor(r *http.Request) string {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(auth, "Bearer ") {
		token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
		if token != "" {
			return token
		}
	}
	return IPKeyExtractor(r)
}

func RateLimitMiddleware(engine limiter.Limiter, extractor KeyExtractor, logger *slog.Logger) func(http.Handler) http.Handler {
	if extractor == nil {
		extractor = IPKeyExtractor
	}
	if logger == nil {
		logger = slog.Default()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rawKey := strings.TrimSpace(extractor(r))
			if rawKey == "" {
				rawKey = "unknown"
			}

			// Namespace the key to avoid collisions between different extractor strategies.
			// If you always use only one extractor, this is still harmless.
			key := "http:" + rawKey

			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()

			result, err := engine.Allow(ctx, key)
			if err != nil {
				// Fail-open policy: don't take the service down if Redis is down.
				logger.Error("rate limiter internal error (fail-open)", slog.String("error", err.Error()))
				metrics.IncError()
				next.ServeHTTP(w, r)
				return
			}

			// IETF RateLimit fields (RFC 9331): values must be decimal integers as strings.
			// RateLimit-Limit: requests allowed in the window/bucket (approx for token bucket).
			w.Header().Set("RateLimit-Limit", strconv.Itoa(result.Limit))
			w.Header().Set("RateLimit-Remaining", strconv.Itoa(max0(result.Remaining)))

			// RateLimit-Reset: number of seconds until quota resets (for token bucket we approximate with RetryAfter when blocked; otherwise 0)
			// If you want a more accurate reset semantics, you can compute "time to full refill".
			if result.Allowed {
				w.Header().Set("RateLimit-Reset", "0")
			} else {
				w.Header().Set("RateLimit-Reset", strconv.Itoa(retryAfterSeconds(result.RetryAfter)))
			}

			if !result.Allowed {
				metrics.IncBlocked()

				// Retry-After should be integer seconds (or HTTP-date). Use ceil; min 1s when blocked.
				w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds(result.RetryAfter)))

				w.Header().Set("Content-Type", "application/problem+json")
				w.WriteHeader(http.StatusTooManyRequests)

				problem := ErrorDetail{
					Type:     "https://datatracker.ietf.org/doc/html/rfc7807",
					Title:    "Too Many Requests",
					Status:   http.StatusTooManyRequests,
					Detail:   "Rate limit exceeded. Please try again later.",
					Instance: r.URL.Path,
				}
				_ = json.NewEncoder(w).Encode(problem)
				return
			}

			metrics.IncAllowed()
			next.ServeHTTP(w, r)
		})
	}
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

func max0(x int) int {
	if x < 0 {
		return 0
	}
	return x
}

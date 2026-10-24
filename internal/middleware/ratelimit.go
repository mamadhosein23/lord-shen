package middleware

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
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
func IPKeyExtractor(r *http.Request) string {
	if cfIP := r.Header.Get("CF-Connecting-IP"); cfIP != "" {
		return cfIP
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	if xrip := r.Header.Get("X-Real-IP"); xrip != "" {
		return xrip
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// APIKeyExtractor extracts key from Authorization header.
func APIKeyExtractor(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}
	return IPKeyExtractor(r) // Fallback to IP
}

func RateLimitMiddleware(engine limiter.Limiter, extractor KeyExtractor, logger *slog.Logger) func(http.Handler) http.Handler {
	if extractor == nil {
		extractor = IPKeyExtractor
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := extractor(r)
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()

			result, err := engine.Allow(ctx, key)
			if err != nil {
				logger.Error("rate limiter internal error (fail-open)", slog.String("error", err.Error()))
				metrics.IncError()
				next.ServeHTTP(w, r) // Fail-Open policy
				return
			}

			// Set IETF standard headers
			w.Header().Set("RateLimit-Limit", string(rune(result.Limit)))
			// (در تولید از strconv.Itoa استفاده می‌شود)

			if !result.Allowed {
				metrics.IncBlocked()
				w.Header().Set("Retry-After", result.RetryAfter.String())
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

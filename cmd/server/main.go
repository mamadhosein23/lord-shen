package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"lord-shen/internal/limiter"
)

// LimiterEngine defines the interface for underlying rate-limiting engines.
type LimiterEngine interface {
	Allow(ctx context.Context, key string) (limiter.Result, error)
}

// RateLimitMiddleware creates an HTTP middleware that enforces rate limiting 
// based on standard IETF/RFC rate limit headers and Fail-Open reliability.
func RateLimitMiddleware(engine LimiterEngine, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			clientIP := extractRealIP(r)
			
			// Enforce a strict timeout for the rate-limiting evaluation to protect latency
			ctx, cancel := context.WithTimeout(r.Context(), 50*time.Millisecond)
			defer cancel()

			res, err := engine.Allow(ctx, clientIP)
			if err != nil {
				// Log the internal error but apply Fail-Open: do not block traffic due to limiter failure
				logger.Error("rate limiter internal error", "ip", clientIP, "error", err)
				next.ServeHTTP(w, r)
				return
			}

			// Set standard IETF RateLimit headers for client integration
			w.Header().Set("RateLimit-Limit", fmt.Sprintf("%d", res.Limit))
			w.Header().Set("RateLimit-Remaining", fmt.Sprintf("%d", res.Remaining))
			w.Header().Set("RateLimit-Reset", fmt.Sprintf("%d", res.ResetAt.Unix()))

			if !res.Allowed {
				retryAfterSeconds := int(res.RetryAfter.Seconds())
				if retryAfterSeconds <= 0 {
					retryAfterSeconds = 1
				}
				
				w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfterSeconds))
				w.Header().Set("Content-Type", "application/problem+json")
				w.WriteHeader(http.StatusTooManyRequests)

				// Structured error response compliant with RFC 7807 (Problem Details for HTTP APIs)
				payload := map[string]any{
					"type":        "https://httpstatuses.com/429",
					"title":       "Too Many Requests",
					"status":      http.StatusTooManyRequests,
					"detail":      "Rate limit exceeded. Please try again later.",
					"retry_after": retryAfterSeconds,
				}
				_ = json.NewEncoder(w).Encode(payload)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// extractRealIP accurately extracts the client's real IP address behind proxies, load balancers, and CDNs (e.g., Cloudflare).
func extractRealIP(r *http.Request) string {
	// Check Cloudflare specific connecting IP header
	if cfIP := r.Header.Get("CF-Connecting-IP"); cfIP != "" {
		return strings.TrimSpace(cfIP)
	}
	// Check standard X-Real-IP header
	if xRealIP := r.Header.Get("X-Real-IP"); xRealIP != "" {
		return strings.TrimSpace(xRealIP)
	}
	// Check X-Forwarded-For chain and extract the first client IP
	if xForwardedFor := r.Header.Get("X-Forwarded-For"); xForwardedFor != "" {
		parts := strings.Split(xForwardedFor, ",")
		if len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
			return strings.TrimSpace(parts[0])
		}
	}
	
	// Fallback to RemoteAddr if no proxy headers are present
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

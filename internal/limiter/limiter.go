package middleware

import (
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"

	"lord-shen/internal/limiter"
)

// ErrorResponse defines the structure of the JSON payload returned when rate limits are exceeded.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// RateLimitMiddleware creates an HTTP middleware that intercepts requests and enforces rate limiting per client IP.
func RateLimitMiddleware(limiter *limiter.RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Accurately extract the client's IP address (handling potential proxy ports using net.SplitHostPort)
			ip, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				ip = r.RemoteAddr
			}

			allowed, remaining, resetTime := limiter.Allow(ip)

			// Set standard HTTP Rate Limit headers for client integration
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(100)) // Example: static limit fallback
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetTime.Unix(), 10))

			if !allowed {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(ErrorResponse{
					Error:   "Too Many Requests",
					Message: "Rate limit exceeded. Please try again later.",
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

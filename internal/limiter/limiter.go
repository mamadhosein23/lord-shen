package middleware

import (
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"

	"lord-shen/internal/limiter"
)

type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func RateLimitMiddleware(limiter *limiter.RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// استخراج دقیق IP کلاینت (با در نظر گرفتن پراکسی‌های احتمالی مثل Nginx)
			ip, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				ip = r.RemoteAddr
			}

			allowed, remaining, resetTime := limiter.Allow(ip)

			// تنظیم هدرهای استاندارد Rate Limit
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(100)) // مثال: حد ثابت
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetTime.Unix(), 10))

			if !allowed {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				json.NewEncoder(w).Encode(ErrorResponse{
					Error:   "Too Many Requests",
					Message: "Rate limit exceeded. Please try again later.",
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

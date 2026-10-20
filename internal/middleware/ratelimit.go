package middleware

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"time"

	"github.com/mamadhosein23/lord-shen/internal/limiter"
)

// RateLimit یک میدل‌ور استاندارد http.Handler برمی‌گرداند.
func RateLimit(l limiter.Limiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}

		res, err := l.Allow(context.Background(), ip)
		if err != nil {
			http.Error(w, "internal limiter error", http.StatusInternalServerError)
			return
		}

		// هدرهای استاندارد Rate Limit
		w.Header().Set("X-RateLimit-Limit", itoa(res.Limit))
		w.Header().Set("X-RateLimit-Remaining", itoa(res.Remaining))
		w.Header().Set("X-RateLimit-Reset", res.ResetAt.Format(time.RFC3339))

		if !res.Allowed {
			w.Header().Set("Retry-After", secs(res.RetryAfter))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]string{
				"error":       "rate limit exceeded",
				"retry_after": secs(res.RetryAfter),
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}

func itoa(n int) string {
	return fmtInt(n)
}

func secs(d time.Duration) string {
	s := int(d.Seconds())
	if s < 1 {
		s = 1
	}
	return fmtInt(s)
}

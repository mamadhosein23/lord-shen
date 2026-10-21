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

type LimiterEngine interface {
	Allow(ctx context.Context, key string) (limiter.Result, error)
}

// RateLimitMiddleware میدلور حرفه‌ای نرخ‌سنج با استانداردهای RFC/IETF
func RateLimitMiddleware(engine LimiterEngine, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			clientIP := extractRealIP(r)
			ctx, cancel := context.WithTimeout(r.Context(), 50*time.Millisecond)
			defer cancel()

			res, err := engine.Allow(ctx, clientIP)
			if err != nil {
				logger.Error("rate limiter error", "ip", clientIP, "error", err)
				// اصل Fail-Open: در صورت خطای داخلی لیمیتر، سرویس کلاینت را دان نمی‌کند
				next.ServeHTTP(w, r)
				return
			}

			// تنظیم هدرهای استاندارد IETF RateLimit Headers
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

				payload := map[string]any{
					"type":        "https://httpstatuses.com/429",
					"title":       "Too Many Requests",
					"status":      http.StatusTooManyRequests,
					"detail":      "تعداد درخواست‌های شما از حد مجاز عبور کرده است.",
					"retry_after": retryAfterSeconds,
				}
				_ = json.NewEncoder(w).Encode(payload)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// extractRealIP تشخیص دقیق آی‌پی کلاینت پشت پروکسی و CDN
func extractRealIP(r *http.Request) string {
	// هدرهای متداول کلودفلر و لودبالانسرها
	if cfIP := r.Header.Get("CF-Connecting-IP"); cfIP != "" {
		return strings.TrimSpace(cfIP)
	}
	if xRealIP := r.Header.Get("X-Real-IP"); xRealIP != "" {
		return strings.TrimSpace(xRealIP)
	}
	if xForwardedFor := r.Header.Get("X-Forwarded-For"); xForwardedFor != "" {
		parts := strings.Split(xForwardedFor, ",")
		if len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
			return strings.TrimSpace(parts[0])
		}
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

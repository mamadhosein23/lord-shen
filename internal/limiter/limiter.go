package limiter

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
	"unsafe"
)

// Sentinel errors as immutable value types for errors.Is() matching.
var (
	ErrInvalidBatchSize = errors.New("limiter: batch count n must be > 0")
	ErrKeyNotFound      = errors.New("limiter: rate limit key not found")
	ErrRateExceeded     = errors.New("limiter: quota exceeded")
	ErrClosed           = errors.New("limiter: engine is closed")
)

// Policy exposes the static rule governing a bucket/window for RFC RateLimit-Policy.
type Policy struct {
	Limit  int           // Quota limit
	Window time.Duration // Window timeframe (e.g., 1m, 1h)
	Burst  int           // Max capacity for leaky/token bucket
}

// Result describes the evaluation outcome of a rate-limiting evaluation.
// Conforms directly with IETF RateLimit Headers specification:
// (draft-ietf-httpapi-ratelimit-headers-08).
// Memory layout is ordered by field alignment (64-bit down to 8-bit) to minimize struct padding.
type Result struct {
	ResetAt    time.Time     // 24 bytes: Monotonic + wall clock
	RetryAfter time.Duration // 8 bytes: Wait time if throttled
	Limit      int           // 8 bytes (on 64-bit arch)
	Remaining  int           // 8 bytes
	Window     time.Duration // 8 bytes: Window of policy
	Allowed    bool          // 1 byte
	// 7 bytes padding inserted by compiler here
}

// RetryAfterSeconds returns integer seconds to wait before retry.
// Uses fast integer math: ceil(duration / 1s).
// Guaranteed to return at least 1s if throttled (RFC 9110 § 10.2.3).
func (r Result) RetryAfterSeconds() int {
	if r.Allowed || r.RetryAfter <= 0 {
		return 0
	}
	// Integer ceiling: (d + 1s - 1ns) / 1s
	sec := int((r.RetryAfter + time.Second - 1) / time.Second)
	if sec <= 0 {
		return 1
	}
	return sec
}

// ResetSeconds returns integer delta seconds until ResetAt relative to 'now'.
func (r Result) ResetSeconds(now time.Time) int {
	if !now.Before(r.ResetAt) {
		if !r.Allowed {
			return 1
		}
		return 0
	}
	diff := r.ResetAt.Sub(now)
	sec := int((diff + time.Second - 1) / time.Second)
	if sec <= 0 && !r.Allowed {
		return 1
	}
	return sec
}

// Fast header key constants (avoids allocation during canonicalization).
const (
	HeaderRateLimitLimit     = "RateLimit-Limit"
	HeaderRateLimitRemaining = "RateLimit-Remaining"
	HeaderRateLimitReset     = "RateLimit-Reset"
	HeaderRateLimitPolicy    = "RateLimit-Policy"
	HeaderRetryAfter         = "Retry-After"
)

// WriteHeaders applies IETF standard headers directly into http.Header.
// Employs a stack-allocated buffer to eliminate heap allocations for integer conversions.
func (r Result) WriteHeaders(h http.Header, now time.Time) {
	var buf [32]byte

	// 1. RateLimit-Limit
	h[HeaderRateLimitLimit] = []string{string(strconv.AppendInt(buf[:0], int64(r.Limit), 10))}

	// 2. RateLimit-Remaining
	rem := r.Remaining
	if rem < 0 {
		rem = 0
	}
	h[HeaderRateLimitRemaining] = []string{string(strconv.AppendInt(buf[:0], int64(rem), 10))}

	// 3. RateLimit-Reset
	h[HeaderRateLimitReset] = []string{string(strconv.AppendInt(buf[:0], int64(r.ResetSeconds(now)), 10))}

	// 4. RateLimit-Policy (e.g., "100;w=60") if window is present
	if r.Window > 0 {
		wSec := int64(r.Window / time.Second)
		pBuf := strconv.AppendInt(buf[:0], int64(r.Limit), 10)
		pBuf = append(pBuf, ';', 'w', '=')
		pBuf = strconv.AppendInt(pBuf, wSec, 10)
		h[HeaderRateLimitPolicy] = []string{string(pBuf)}
	}

	// 5. RFC 9110 Retry-After header on 429
	if !r.Allowed {
		h[HeaderRetryAfter] = []string{string(strconv.AppendInt(buf[:0], int64(r.RetryAfterSeconds()), 10))}
	}
}

// Limiter is the clean, unbloated core interface.
type Limiter interface {
	// Allow evaluates a single unit against key.
	Allow(ctx context.Context, key string) (Result, error)

	// AllowN evaluates n units against key. Returns ErrInvalidBatchSize if n <= 0.
	AllowN(ctx context.Context, key string, n int) (Result, error)

	// Inspect views quota without state mutation.
	Inspect(ctx context.Context, key string) (Result, error)

	// Reset invalidates counter/bucket for key immediately.
	Reset(ctx context.Context, key string) error
}

// ZeroAllocKeyLimiter provides a zero-allocation interface for hot network paths
// where keys are received as raw bytes ([]byte) from the transport layer.
type ZeroAllocKeyLimiter interface {
	AllowBytes(ctx context.Context, key []byte) (Result, error)
	AllowNBytes(ctx context.Context, key []byte, n int) (Result, error)
}

// StringToBytesUnsafe converts string to byte slice without heap allocation.
// (Useful inside driver implementations).
func StringToBytesUnsafe(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// BytesToStringUnsafe converts byte slice to string without heap allocation.
func BytesToStringUnsafe(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// Closer is an optional interface checked by drivers (IoC / composition over pollution).
type Closer interface {
	io.Closer
}

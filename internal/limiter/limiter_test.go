package limiter_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mamadhosein23/lord-shen/internal/limiter"
)

// MockMemoryLimiter اکنون به عنوان یک Struct برای تست کاملاً مناسب است.
type MockMemoryLimiter struct {
	mu        sync.Mutex
	capacity  int
	remaining int
	resetAt   time.Time
}

func NewMockMemoryLimiter(capacity int) *MockMemoryLimiter {
	return &MockMemoryLimiter{
		capacity:  capacity,
		remaining: capacity,
		resetAt:   time.Now().Add(time.Second), // تعیین زمان ثابت برای سادگی
	}
}

func (m *MockMemoryLimiter) Allow(ctx context.Context, key string) (limiter.Result, error) {
	return m.AllowN(ctx, key, 1)
}

func (m *MockMemoryLimiter) AllowN(ctx context.Context, key string, n int) (limiter.Result, error) {
	if n <= 0 {
		return limiter.Result{}, errors.New("n must be strictly positive")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.remaining >= n {
		m.remaining -= n
		return limiter.Result{
			Allowed:   true,
			Limit:     m.capacity,
			Remaining: m.remaining,
			ResetAt:   m.resetAt,
		}, nil
	}

	return limiter.Result{
		Allowed:    false,
		Limit:      m.capacity,
		Remaining:  m.remaining,
		ResetAt:    m.resetAt,
		RetryAfter: time.Until(m.resetAt), // استفاده از متد استاندارد برای محاسبه فاصله زمانی
	}, nil
}

func (m *MockMemoryLimiter) Inspect(ctx context.Context, key string) (limiter.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return limiter.Result{
		Allowed:   m.remaining > 0,
		Limit:     m.capacity,
		Remaining: m.remaining,
		ResetAt:   m.resetAt,
	}, nil
}

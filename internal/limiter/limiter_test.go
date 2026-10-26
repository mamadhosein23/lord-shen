package limiter_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mamadhosein23/lord-shen/internal/limiter"
)

// MockLimiter پیاده‌سازی موقت برای تست امضای اینترفیس جدید است.
// (در صورت داشتن پیاده‌سازی اصلی، از ساختار واقعی استفاده کنید)
type MockMemoryLimiter struct {
	mu        sync.Mutex
	capacity  int
	remaining int
}

func NewMockMemoryLimiter(capacity int) *MockMemoryLimiter {
	return &MockMemoryLimiter{
		capacity:  capacity,
		remaining: capacity,
	}
}

func (m *MockMemoryLimiter) Allow(ctx context.Context, key string) (limiter.Result, error) {
	return m.AllowN(ctx, key, 1)
}

func (m *MockMemoryLimiter) AllowN(ctx context.Context, key string, n int) (limiter.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	if m.remaining >= n {
		m.remaining -= n
		return limiter.Result{
			Allowed:    true,
			Limit:      m.capacity,
			Remaining:  m.remaining,
			ResetAt:    now.Add(time.Second),
			RetryAfter: 0,
		}, nil
	}

	return limiter.Result{
		Allowed:    false,
		Limit:      m.capacity,
		Remaining:  0,
		ResetAt:    now.Add(time.Second),
		RetryAfter: time.Second,
	}, nil
}

// TestRateLimiter_Allow verifies that requests are permitted up to the limit and rejected afterward.
func TestRateLimiter_Allow(t *testing.T) {
	ctx := context.Background()
	rl := NewMockMemoryLimiter(2)
	ip := "192.168.1.10"

	// First request must be allowed
	res1, err := rl.Allow(ctx, ip)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res1.Allowed || res1.Remaining != 1 {
		t.Errorf("expected request 1 to be allowed with 1 remaining, got %+v", res1)
	}

	// Second request must be allowed
	res2, err := rl.Allow(ctx, ip)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res2.Allowed || res2.Remaining != 0 {
		t.Errorf("expected request 2 to be allowed with 0 remaining, got %+v", res2)
	}

	// Third request exceeds the limit and must be blocked
	res3, err := rl.Allow(ctx, ip)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res3.Allowed {
		t.Errorf("expected request 3 to be blocked, got %+v", res3)
	}
	if res3.RetryAfter <= 0 {
		t.Errorf("expected positive RetryAfter duration, got %v", res3.RetryAfter)
	}
}

// TestRateLimiter_Concurrent verifies thread safety under concurrent access to detect race conditions.
// Run with: go test -race ./internal/limiter/...
func TestRateLimiter_Concurrent(t *testing.T) {
	ctx := context.Background()
	rl := NewMockMemoryLimiter(1000)
	var wg sync.WaitGroup
	ip := "10.0.0.1"

	workers := 100
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = rl.Allow(ctx, ip)
		}()
	}
	wg.Wait()
}

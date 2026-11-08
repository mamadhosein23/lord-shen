package limiter_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mamadhosein23/lord-shen/internal/limiter"
)

// Ensure MockMemoryLimiter satisfies the limiter.Limiter interface at compile time.
var _ limiter.Limiter = (*MockMemoryLimiter)(nil)

// MockMemoryLimiter provides a concurrent-safe, in-memory implementation of the Limiter interface for testing.
type MockMemoryLimiter struct {
	mu        sync.Mutex
	capacity  int
	remaining int
}

// NewMockMemoryLimiter initializes an in-memory limiter with the specified capacity.
func NewMockMemoryLimiter(capacity int) *MockMemoryLimiter {
	return &MockMemoryLimiter{
		capacity:  capacity,
		remaining: capacity,
	}
}

// Allow evaluates a single permit request for the given key.
func (m *MockMemoryLimiter) Allow(ctx context.Context, key string) (limiter.Result, error) {
	return m.AllowN(ctx, key, 1)
}

// AllowN evaluates whether n units are permitted, decrementing available quota on success.
func (m *MockMemoryLimiter) AllowN(ctx context.Context, key string, n int) (limiter.Result, error) {
	if n <= 0 {
		return limiter.Result{}, errors.New("n must be strictly positive")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	resetAt := now.Add(time.Second)

	if m.remaining >= n {
		m.remaining -= n
		return limiter.Result{
			Allowed:    true,
			Limit:      m.capacity,
			Remaining:  m.remaining,
			ResetAt:    resetAt,
			RetryAfter: 0,
		}, nil
	}

	return limiter.Result{
		Allowed:    false,
		Limit:      m.capacity,
		Remaining:  m.remaining,
		ResetAt:    resetAt,
		RetryAfter: time.Second,
	}, nil
}

// Inspect returns current quota status without consuming permits.
func (m *MockMemoryLimiter) Inspect(ctx context.Context, key string) (limiter.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	return limiter.Result{
		Allowed:    m.remaining > 0,
		Limit:      m.capacity,
		Remaining:  m.remaining,
		ResetAt:    now.Add(time.Second),
		RetryAfter: 0,
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
		t.Fatalf("unexpected error on first request: %v", err)
	}
	if !res1.Allowed || res1.Remaining != 1 {
		t.Errorf("expected request 1 allowed with 1 remaining, got allowed=%v, remaining=%d", res1.Allowed, res1.Remaining)
	}

	// Second request must be allowed
	res2, err := rl.Allow(ctx, ip)
	if err != nil {
		t.Fatalf("unexpected error on second request: %v", err)
	}
	if !res2.Allowed || res2.Remaining != 0 {
		t.Errorf("expected request 2 allowed with 0 remaining, got allowed=%v, remaining=%d", res2.Allowed, res2.Remaining)
	}

	// Third request exceeds the limit and must be blocked
	res3, err := rl.Allow(ctx, ip)
	if err != nil {
		t.Fatalf("unexpected error on third request: %v", err)
	}
	if res3.Allowed {
		t.Errorf("expected request 3 to be blocked, got allowed=%v", res3.Allowed)
	}
	if res3.RetryAfter <= 0 {
		t.Errorf("expected positive RetryAfter duration, got %v", res3.RetryAfter)
	}
	if sec := res3.RetryAfterSeconds(); sec != 1 {
		t.Errorf("expected RetryAfterSeconds to be 1, got %d", sec)
	}
}

// TestRateLimiter_AllowN_Validation ensures non-positive permits yield an error.
func TestRateLimiter_AllowN_Validation(t *testing.T) {
	ctx := context.Background()
	rl := NewMockMemoryLimiter(10)

	for _, n := range []int{0, -1, -5} {
		if _, err := rl.AllowN(ctx, "key", n); err == nil {
			t.Errorf("expected error for non-positive n=%d, got nil", n)
		}
	}
}

// TestRateLimiter_Inspect verifies that querying quota does not consume tokens.
func TestRateLimiter_Inspect(t *testing.T) {
	ctx := context.Background()
	rl := NewMockMemoryLimiter(5)
	key := "test-key"

	res, err := rl.Inspect(ctx, key)
	if err != nil {
		t.Fatalf("unexpected inspect error: %v", err)
	}
	if !res.Allowed || res.Remaining != 5 {
		t.Fatalf("expected 5 remaining permits, got allowed=%v, remaining=%d", res.Allowed, res.Remaining)
	}

	// Verify that inspect did not consume capacity
	resAfter, err := rl.Inspect(ctx, key)
	if err != nil {
		t.Fatalf("unexpected inspect error on repeat: %v", err)
	}
	if resAfter.Remaining != 5 {
		t.Errorf("expected inspect to leave quota intact at 5, got %d", resAfter.Remaining)
	}
}

// TestRateLimiter_Concurrent verifies thread safety and atomic quota consumption under race conditions.
// Run with: go test -race -v ./internal/limiter/...
func TestRateLimiter_Concurrent(t *testing.T) {
	ctx := context.Background()
	capacity := 1000
	workers := 100
	rl := NewMockMemoryLimiter(capacity)
	ip := "10.0.0.1"

	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := rl.Allow(ctx, ip)
			if err != nil {
				errCh <- err
				return
			}
			if !res.Allowed {
				errCh <- errors.New("unexpected throttling under concurrent capacity")
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent worker error: %v", err)
	}

	inspectRes, err := rl.Inspect(ctx, ip)
	if err != nil {
		t.Fatalf("failed to inspect limiter state: %v", err)
	}

	expectedRemaining := capacity - workers
	if inspectRes.Remaining != expectedRemaining {
		t.Errorf("quota drift detected: expected remaining=%d, got=%d", expectedRemaining, inspectRes.Remaining)
	}
}

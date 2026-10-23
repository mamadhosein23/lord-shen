package limiter

import (
	"sync"
	"testing"
	"time"
)

// TestRateLimiter_Allow verifies that requests are permitted up to the limit and rejected afterward.
func TestRateLimiter_Allow(t *testing.T) {
	rl := NewRateLimiter(2, 1*time.Second)
	ip := "192.168.1.10"

	// First and second requests must be allowed
	if allowed, _, _ := rl.Allow(ip); !allowed {
		t.Errorf("expected request 1 to be allowed")
	}
	if allowed, _, _ := rl.Allow(ip); !allowed {
		t.Errorf("expected request 2 to be allowed")
	}

	// Third request exceeds the limit and must be blocked
	if allowed, _, _ := rl.Allow(ip); allowed {
		t.Errorf("expected request 3 to be blocked")
	}
}

// TestRateLimiter_Concurrent verifies thread safety under concurrent access to detect race conditions.
func TestRateLimiter_Concurrent(t *testing.T) {
	rl := NewRateLimiter(1000, 1*time.Minute)
	var wg sync.WaitGroup
	ip := "10.0.0.1"

	// Dispatch 100 concurrent goroutines accessing the same IP entry
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rl.Allow(ip)
		}()
	}
	wg.Wait()
}

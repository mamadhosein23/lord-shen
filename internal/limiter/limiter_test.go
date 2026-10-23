package limiter

import (
	"sync"
	"testing"
	"time"
)

func TestRateLimiter_Allow(t *testing.T) {
	rl := NewRateLimiter(2, 1*time.Second)
	ip := "192.168.1.10"

	// درخواست اول و دوم باید مجاز باشند
	if allowed, _, _ := rl.Allow(ip); !allowed {
		t.Errorf("Expected request 1 to be allowed")
	}
	if allowed, _, _ := rl.Allow(ip); !allowed {
		t.Errorf("Expected request 2 to be allowed")
	}

	// درخواست سوم باید رد شود
	if allowed, _, _ := rl.Allow(ip); allowed {
		t.Errorf("Expected request 3 to be blocked")
	}
}

// TestRateLimiter_Concurrent بررسی Race Condition با ابزار داخلی Go
func TestRateLimiter_Concurrent(t *testing.T) {
	rl := NewRateLimiter(1000, 1*time.Minute)
	var wg sync.WaitGroup
	ip := "10.0.0.1"

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rl.Allow(ip)
		}()
	}
	wg.Wait()
}

package realtime

import (
	"sync"
	"testing"
	"time"
)

func TestRateLimiter_Basic(t *testing.T) {
	// Rate limit: 3 messages per second
	limit := 3
	interval := time.Second
	rl := NewRateLimiter(limit, interval)

	baseTime := time.Now()

	// Initial tokens should be equal to limit (3).
	// We can consume 3 messages immediately.
	for i := 0; i < 3; i++ {
		if !rl.AllowAt(baseTime) {
			t.Fatalf("expected message %d to be allowed", i+1)
		}
	}

	// 4th message should be denied immediately at the same timestamp.
	if rl.AllowAt(baseTime) {
		t.Fatal("expected 4th message to be denied")
	}

	// Move time forward by 1/3 of a second. We should get 1 token.
	refillTime := baseTime.Add(334 * time.Millisecond) // slightly more than 1/3s
	if !rl.AllowAt(refillTime) {
		t.Fatal("expected message to be allowed after 1/3s refill")
	}

	// Subsequent message at same timestamp is denied.
	if rl.AllowAt(refillTime) {
		t.Fatal("expected message to be denied again after consuming refill")
	}

	// Move time forward by 1 second. We should refill to capacity (capped at 3).
	capacityTime := refillTime.Add(time.Second)
	for i := 0; i < 3; i++ {
		if !rl.AllowAt(capacityTime) {
			t.Fatalf("expected message %d to be allowed after full refill", i+1)
		}
	}

	// 4th message should be denied.
	if rl.AllowAt(capacityTime) {
		t.Fatal("expected 4th message to be denied after consuming capacity")
	}
}

func TestRateLimiter_CappedCapacity(t *testing.T) {
	limit := 5
	interval := time.Minute
	rl := NewRateLimiter(limit, interval)

	baseTime := time.Now()

	// Consume all 5
	for i := 0; i < 5; i++ {
		if !rl.AllowAt(baseTime) {
			t.Fatal("expected allowed")
		}
	}
	if rl.AllowAt(baseTime) {
		t.Fatal("expected denied")
	}

	// Advance time by 10 minutes. Capped capacity should still only allow 5 messages, not 50.
	futureTime := baseTime.Add(10 * time.Minute)
	for i := 0; i < 5; i++ {
		if !rl.AllowAt(futureTime) {
			t.Fatalf("expected message %d to be allowed after long wait", i+1)
		}
	}
	if rl.AllowAt(futureTime) {
		t.Fatal("expected 6th message to be denied even after long wait due to limit cap")
	}
}

func TestRateLimiter_ZeroOrNegativeLimits(t *testing.T) {
	// A limiter with 0 or negative limits should allow all traffic.
	rl := NewRateLimiter(0, time.Second)
	for i := 0; i < 10; i++ {
		if !rl.Allow() {
			t.Fatal("expected always allowed with 0 limit")
		}
	}

	rlNegative := NewRateLimiter(-5, time.Second)
	for i := 0; i < 10; i++ {
		if !rlNegative.Allow() {
			t.Fatal("expected always allowed with negative limit")
		}
	}
}

func TestRateLimiter_Concurrency(t *testing.T) {
	limit := 1000
	interval := time.Second
	rl := NewRateLimiter(limit, interval)

	var wg sync.WaitGroup
	workers := 10
	iterations := 100

	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = rl.Allow()
			}
		}()
	}
	wg.Wait()
	// Just verifies no data races occur during concurrent operations.
}

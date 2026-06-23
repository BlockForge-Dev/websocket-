package realtime

import (
	"sync"
	"time"
)

// RateLimiter limits message arrival frequency.
type RateLimiter struct {
	mu            sync.Mutex
	limit         int
	interval      time.Duration
	tokens        float64
	lastRefreshed time.Time
}

// NewRateLimiter creates a thread-safe rate limiter.
func NewRateLimiter(limit int, interval time.Duration) *RateLimiter {
	return &RateLimiter{
		limit:         limit,
		interval:      interval,
		tokens:        float64(limit),
		lastRefreshed: time.Now(),
	}
}

// Allow returns true if a message is admitted.
func (rl *RateLimiter) Allow() bool {
	return rl.AllowAt(time.Now())
}

// AllowAt permits injecting a time source for tests.
func (rl *RateLimiter) AllowAt(now time.Time) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.limit <= 0 || rl.interval <= 0 {
		return true
	}

	elapsed := now.Sub(rl.lastRefreshed)
	rl.lastRefreshed = now

	// Refill rate: limit tokens per interval
	refill := float64(rl.limit) * (float64(elapsed) / float64(rl.interval))
	rl.tokens += refill
	if rl.tokens > float64(rl.limit) {
		rl.tokens = float64(rl.limit)
	}

	if rl.tokens >= 1.0 {
		rl.tokens -= 1.0
		return true
	}
	return false
}

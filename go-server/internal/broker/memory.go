package broker

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
)

// MemoryBroker is a process-local pub/sub bus for single-node development.
// It provides the same Broker semantics without requiring an external
// service. Subscriptions are goroutine-safe and handler invocations occur
// synchronously on the publisher's goroutine for simplicity.
type MemoryBroker struct {
	mu          sync.RWMutex
	subscribers map[int]*memorySubscription
	nextID      int
	closed      atomic.Bool
}

type memorySubscription struct {
	subject string
	handler func(string, []byte)
}

// NewMemoryBroker creates a ready-to-use in-memory event bus.
func NewMemoryBroker() *MemoryBroker {
	return &MemoryBroker{
		subscribers: make(map[int]*memorySubscription),
	}
}

// Publish fans data out to all matching subscribers synchronously.
func (b *MemoryBroker) Publish(_ context.Context, subject string, data []byte) error {
	if b.closed.Load() {
		return ErrBrokerClosed
	}

	b.mu.RLock()
	matches := make([]*memorySubscription, 0, len(b.subscribers))
	for _, sub := range b.subscribers {
		if matchSubject(sub.subject, subject) {
			matches = append(matches, sub)
		}
	}
	b.mu.RUnlock()

	// Copy data to prevent mutation by handlers.
	copied := make([]byte, len(data))
	copy(copied, data)

	for _, sub := range matches {
		sub.handler(subject, copied)
	}
	return nil
}

// Subscribe registers handler for subjects matching pattern.
// The pattern supports trailing wildcards: "foo.>" matches "foo.bar" and
// "foo.bar.baz". An exact match is also supported.
func (b *MemoryBroker) Subscribe(_ context.Context, subject string, handler func(string, []byte)) (func(), error) {
	if b.closed.Load() {
		return nil, ErrBrokerClosed
	}

	b.mu.Lock()
	id := b.nextID
	b.nextID++
	b.subscribers[id] = &memorySubscription{
		subject: subject,
		handler: handler,
	}
	b.mu.Unlock()

	return func() {
		b.mu.Lock()
		delete(b.subscribers, id)
		b.mu.Unlock()
	}, nil
}

// Ready always returns true for the in-memory bus (unless closed).
func (b *MemoryBroker) Ready() bool {
	return !b.closed.Load()
}

// Close shuts down the bus and prevents further operations.
func (b *MemoryBroker) Close() error {
	b.closed.Store(true)
	b.mu.Lock()
	b.subscribers = make(map[int]*memorySubscription)
	b.mu.Unlock()
	return nil
}

// matchSubject checks if a published subject matches a subscription pattern.
// Supports NATS-style ">" wildcard for trailing segments.
// "blockforge.room.>" matches "blockforge.room.lobby", "blockforge.room.game.1", etc.
func matchSubject(pattern, subject string) bool {
	if pattern == subject {
		return true
	}
	if strings.HasSuffix(pattern, ".>") {
		prefix := strings.TrimSuffix(pattern, ".>")
		return strings.HasPrefix(subject, prefix+".")
	}
	if pattern == ">" {
		return true
	}
	return false
}

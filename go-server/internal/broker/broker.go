// Package broker defines the publish/subscribe contract for cross-node
// event routing. Implementations include an in-memory bus for single-node
// development and a NATS JetStream backend for production multi-node
// deployments.
package broker

import "context"

// Broker is the minimal publish/subscribe contract that the realtime hub
// uses to fan events across WebSocket server nodes.
//
// Implementations must be safe for concurrent use.
type Broker interface {
	// Publish sends data to the named subject. The call is fire-and-forget
	// from the caller's perspective; implementations may buffer or retry
	// internally.
	Publish(ctx context.Context, subject string, data []byte) error

	// Subscribe registers handler for messages arriving on subject.
	// The returned cancel function removes the subscription. handler is
	// called from an implementation-owned goroutine; it must not block.
	Subscribe(ctx context.Context, subject string, handler func(subject string, data []byte)) (cancel func(), err error)

	// Ready reports whether the broker connection is healthy enough to
	// accept publishes and deliver subscriptions.
	Ready() bool

	// Close tears down the broker connection and all active subscriptions.
	Close() error
}

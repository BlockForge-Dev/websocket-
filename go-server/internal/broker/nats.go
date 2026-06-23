package broker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// streamName is the JetStream stream that holds all blockforge events.
	streamName = "BLOCKFORGE"
	// streamSubjects defines the subject space for the stream.
	streamSubjects = "blockforge.>"
	// consumerPrefix is the base name for push consumers. The node ID is
	// appended to create a node-unique consumer name.
	consumerPrefix = "blockforge-node-"
)

// NATSBroker implements Broker using NATS JetStream for durable,
// replayable cross-node event delivery.
type NATSBroker struct {
	conn   *nats.Conn
	js     jetstream.JetStream
	logger *slog.Logger
	nodeID string

	mu     sync.Mutex
	subs   []jetstream.ConsumeContext
	closed atomic.Bool
	subSeq int
}

// NATSOptions configures a NATS broker connection.
type NATSOptions struct {
	URL    string
	NodeID string
	Logger *slog.Logger
}

// NewNATSBroker connects to NATS and ensures the JetStream stream exists.
func NewNATSBroker(ctx context.Context, opts NATSOptions) (*NATSBroker, error) {
	nc, err := nats.Connect(opts.URL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if opts.Logger != nil {
				opts.Logger.Warn("broker_nats_disconnected", "error", err)
			}
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			if opts.Logger != nil {
				opts.Logger.Info("broker_nats_reconnected")
			}
		}),
	)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}

	// Ensure the stream exists. CreateOrUpdate is idempotent.
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{streamSubjects},
		Retention: jetstream.InterestPolicy,
		MaxAge:    5 * time.Minute,
	})
	if err != nil {
		nc.Close()
		return nil, err
	}

	return &NATSBroker{
		conn:   nc,
		js:     js,
		logger: opts.Logger,
		nodeID: opts.NodeID,
	}, nil
}

// Publish sends data to the named JetStream subject.
func (b *NATSBroker) Publish(ctx context.Context, subject string, data []byte) error {
	if b.closed.Load() {
		return ErrBrokerClosed
	}
	_, err := b.js.Publish(ctx, subject, data)
	return err
}

// Subscribe creates a durable push consumer for the given subject filter
// and delivers messages to handler. Returns a cancel function to stop
// consumption.
func (b *NATSBroker) Subscribe(ctx context.Context, subject string, handler func(string, []byte)) (func(), error) {
	if b.closed.Load() {
		return nil, ErrBrokerClosed
	}

	b.mu.Lock()
	b.subSeq++
	consumerName := consumerPrefix + b.nodeID + "-" + subject
	// Sanitize consumer name: replace dots and > with dashes.
	sanitized := sanitizeConsumerName(consumerName)
	b.mu.Unlock()

	consumer, err := b.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Name:          sanitized,
		Durable:       sanitized,
		FilterSubject: subject,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return nil, err
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		handler(msg.Subject(), msg.Data())
		_ = msg.Ack()
	})
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.subs = append(b.subs, consumeCtx)
	b.mu.Unlock()

	return func() {
		consumeCtx.Stop()
	}, nil
}

// Ready reports whether the NATS connection is active.
func (b *NATSBroker) Ready() bool {
	if b.closed.Load() {
		return false
	}
	return b.conn.IsConnected()
}

// Close stops all consumers and closes the NATS connection.
func (b *NATSBroker) Close() error {
	if b.closed.Swap(true) {
		return nil
	}

	b.mu.Lock()
	subs := b.subs
	b.subs = nil
	b.mu.Unlock()

	for _, sub := range subs {
		sub.Stop()
	}

	b.conn.Close()
	return nil
}

// sanitizeConsumerName replaces characters invalid in NATS consumer names.
func sanitizeConsumerName(name string) string {
	result := make([]byte, len(name))
	for i := 0; i < len(name); i++ {
		ch := name[i]
		if ch == '.' || ch == '>' || ch == '*' || ch == ' ' {
			result[i] = '-'
		} else {
			result[i] = ch
		}
	}
	return string(result)
}

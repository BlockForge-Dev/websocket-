package broker

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"
)

func TestNATSBroker(t *testing.T) {
	brokerURL := os.Getenv("BLOCKFORGE_BROKER_URL")
	if brokerURL == "" {
		t.Skip("BLOCKFORGE_BROKER_URL not set, skipping NATS broker test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := NATSOptions{
		URL:    brokerURL,
		NodeID: "test-node",
		Logger: slog.Default(),
	}

	b, err := NewNATSBroker(ctx, opts)
	if err != nil {
		t.Fatalf("failed to create NATS broker: %v", err)
	}
	defer b.Close()

	if !b.Ready() {
		t.Fatal("expected NATS broker to be ready")
	}

	subject := "blockforge.test.nats"
	payload := []byte(`{"message": "hello nats"}`)

	var wg sync.WaitGroup
	wg.Add(1)

	var receivedData []byte
	var receivedSubj string

	unsub, err := b.Subscribe(ctx, subject, func(subj string, data []byte) {
		receivedSubj = subj
		receivedData = data
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer unsub()

	// Wait for consumer/subscription readiness
	time.Sleep(200 * time.Millisecond)

	err = b.Publish(ctx, subject, payload)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Wait for event delivery
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message delivery")
	}

	if receivedSubj != subject {
		t.Errorf("expected subject %q, got %q", subject, receivedSubj)
	}
	if string(receivedData) != string(payload) {
		t.Errorf("expected payload %q, got %q", string(payload), string(receivedData))
	}
}

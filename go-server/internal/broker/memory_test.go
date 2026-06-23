package broker

import (
	"context"
	"sync"
	"testing"
)

func TestMemoryBrokerPublishSubscribe(t *testing.T) {
	t.Parallel()
	bus := NewMemoryBroker()
	defer bus.Close()

	var received []string
	var mu sync.Mutex

	_, err := bus.Subscribe(context.Background(), "test.topic", func(subject string, data []byte) {
		mu.Lock()
		received = append(received, string(data))
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := bus.Publish(context.Background(), "test.topic", []byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	mu.Lock()
	if len(received) != 1 || received[0] != "hello" {
		t.Fatalf("expected [hello], got %v", received)
	}
	mu.Unlock()
}

func TestMemoryBrokerWildcardSubscribe(t *testing.T) {
	t.Parallel()
	bus := NewMemoryBroker()
	defer bus.Close()

	var received []string
	var mu sync.Mutex

	_, err := bus.Subscribe(context.Background(), "blockforge.room.>", func(subject string, data []byte) {
		mu.Lock()
		received = append(received, subject+":"+string(data))
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	_ = bus.Publish(context.Background(), "blockforge.room.lobby", []byte("a"))
	_ = bus.Publish(context.Background(), "blockforge.room.game", []byte("b"))
	_ = bus.Publish(context.Background(), "blockforge.private.user1", []byte("c")) // should not match

	mu.Lock()
	if len(received) != 2 {
		t.Fatalf("expected 2 matches, got %d: %v", len(received), received)
	}
	mu.Unlock()
}

func TestMemoryBrokerUnsubscribe(t *testing.T) {
	t.Parallel()
	bus := NewMemoryBroker()
	defer bus.Close()

	callCount := 0
	cancel, err := bus.Subscribe(context.Background(), "test.topic", func(string, []byte) {
		callCount++
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	_ = bus.Publish(context.Background(), "test.topic", []byte("1"))
	if callCount != 1 {
		t.Fatalf("expected 1 call, got %d", callCount)
	}

	cancel()
	_ = bus.Publish(context.Background(), "test.topic", []byte("2"))
	if callCount != 1 {
		t.Fatalf("expected 1 call after unsubscribe, got %d", callCount)
	}
}

func TestMemoryBrokerClose(t *testing.T) {
	t.Parallel()
	bus := NewMemoryBroker()

	if !bus.Ready() {
		t.Fatal("expected ready before close")
	}

	_ = bus.Close()

	if bus.Ready() {
		t.Fatal("expected not ready after close")
	}

	err := bus.Publish(context.Background(), "test", []byte("x"))
	if err != ErrBrokerClosed {
		t.Fatalf("expected ErrBrokerClosed, got %v", err)
	}

	_, err = bus.Subscribe(context.Background(), "test", func(string, []byte) {})
	if err != ErrBrokerClosed {
		t.Fatalf("expected ErrBrokerClosed from subscribe, got %v", err)
	}
}

func TestMemoryBrokerMultipleSubscribers(t *testing.T) {
	t.Parallel()
	bus := NewMemoryBroker()
	defer bus.Close()

	count1 := 0
	count2 := 0

	_, _ = bus.Subscribe(context.Background(), "topic", func(string, []byte) { count1++ })
	_, _ = bus.Subscribe(context.Background(), "topic", func(string, []byte) { count2++ })

	_ = bus.Publish(context.Background(), "topic", []byte("msg"))

	if count1 != 1 || count2 != 1 {
		t.Fatalf("expected both subscribers to receive, got count1=%d count2=%d", count1, count2)
	}
}

func TestMemoryBrokerDataIsolation(t *testing.T) {
	t.Parallel()
	bus := NewMemoryBroker()
	defer bus.Close()

	var receivedData []byte
	_, _ = bus.Subscribe(context.Background(), "topic", func(_ string, data []byte) {
		receivedData = data
	})

	original := []byte("original")
	_ = bus.Publish(context.Background(), "topic", original)

	// Mutate original after publish
	original[0] = 'X'

	if receivedData[0] == 'X' {
		t.Fatal("handler received a reference to original data instead of a copy")
	}
}

func TestMatchSubject(t *testing.T) {
	t.Parallel()
	tests := []struct {
		pattern string
		subject string
		want    bool
	}{
		{"foo", "foo", true},
		{"foo", "bar", false},
		{"foo.>", "foo.bar", true},
		{"foo.>", "foo.bar.baz", true},
		{"foo.>", "foo", false},
		{"foo.>", "foobar", false},
		{">", "anything", true},
		{">", "a.b.c", true},
	}

	for _, tt := range tests {
		got := matchSubject(tt.pattern, tt.subject)
		if got != tt.want {
			t.Errorf("matchSubject(%q, %q) = %v, want %v", tt.pattern, tt.subject, got, tt.want)
		}
	}
}

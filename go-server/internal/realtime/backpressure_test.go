package realtime

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/blockforgelabs/go-websocket/internal/observability"
	"github.com/gorilla/websocket"
)

type blockingSocket struct {
	blocked chan struct{}
}

func (s *blockingSocket) SetReadDeadline(time.Time) error  { return nil }
func (s *blockingSocket) SetWriteDeadline(time.Time) error { return nil }
func (s *blockingSocket) ReadMessage() (int, []byte, error) {
	select {}
}
func (s *blockingSocket) WriteMessage(messageType int, data []byte) error {
	<-s.blocked
	return nil
}
func (s *blockingSocket) Close() error                      { return nil }
func (s *blockingSocket) SetPongHandler(func(string) error) {}
func (s *blockingSocket) SetReadLimit(int64)                {}

type mockSocket struct {
	writes chan []byte
}

func (s *mockSocket) SetReadDeadline(time.Time) error  { return nil }
func (s *mockSocket) SetWriteDeadline(time.Time) error { return nil }
func (s *mockSocket) ReadMessage() (int, []byte, error) {
	select {}
}
func (s *mockSocket) WriteMessage(messageType int, data []byte) error {
	select {
	case s.writes <- data:
	default:
	}
	return nil
}
func (s *mockSocket) Close() error                      { return nil }
func (s *mockSocket) SetPongHandler(func(string) error) {}
func (s *mockSocket) SetReadLimit(int64)                {}

func TestSlowClientIsDisconnectedAndMetricsIncremented(t *testing.T) {
	initialDisconnects := observability.DefaultMetrics.QueueFullDisconnects.Load()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)

	sock := &mockSocket{} // No writeLoop is started, so nothing drains the channel

	var client *Client
	client = newClient(sock, ClientOptions{
		ConnectionID:  "conn_slow",
		UserID:        "user_slow",
		QueueCapacity: 1,
		Logger:        discardLogger(),
		Hub:           hub,
		OnClose: func(reason string) {
			hub.Unregister(client)
		},
	})
	hub.Register(client)

	ok1 := client.Send(websocket.TextMessage, []byte(`{"msg":1}`))
	ok2 := client.Send(websocket.TextMessage, []byte(`{"msg":2}`))

	if !ok1 {
		t.Fatal("expected first send to succeed")
	}
	if ok2 {
		t.Fatal("expected second send to fail due to queue full")
	}

	select {
	case <-client.done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected client done channel to close")
	}

	finalDisconnects := observability.DefaultMetrics.QueueFullDisconnects.Load()
	if finalDisconnects != initialDisconnects+1 {
		t.Fatalf("expected QueueFullDisconnects to increment by 1, got initial=%d, final=%d", initialDisconnects, finalDisconnects)
	}

	if _, exists := hub.Lookup("user_slow"); exists {
		t.Fatal("expected slow client to be unregistered from hub")
	}
}

func TestSlowClientDoesNotBlockHealthyClient(t *testing.T) {
	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)

	// Construct healthy client
	healthyWrites := make(chan []byte, 10)
	healthySock := &mockSocket{writes: healthyWrites}
	var healthyClient *Client
	healthyClient = newClient(healthySock, ClientOptions{
		ConnectionID:  "conn_healthy",
		UserID:        "healthy",
		QueueCapacity: 5,
		Logger:        discardLogger(),
		Hub:           hub,
		OnClose: func(reason string) {
			hub.Unregister(healthyClient)
		},
	})
	hub.Register(healthyClient)
	_ = hub.Join(healthyClient, "room_A")
	go healthyClient.writeLoop()

	// Construct slow client
	slowBlocked := make(chan struct{})
	slowSock := &blockingSocket{blocked: slowBlocked}
	var slowClient *Client
	slowClient = newClient(slowSock, ClientOptions{
		ConnectionID:  "conn_slow",
		UserID:        "slow",
		QueueCapacity: 2,
		Logger:        discardLogger(),
		Hub:           hub,
		OnClose: func(reason string) {
			hub.Unregister(slowClient)
		},
	})
	hub.Register(slowClient)
	_ = hub.Join(slowClient, "room_A")
	go slowClient.writeLoop()

	// We broadcast 4 messages to the room from the hub (sender "system").
	// With slow client capacity=2:
	// - Broadcast 1: read by writeLoop, blocks on sock.WriteMessage.
	// - Broadcast 2: fills slot 1 in c.send.
	// - Broadcast 3: fills slot 2 in c.send (channel full).
	// - Broadcast 4: fails to enqueue, triggers close reason "outbound_queue_full".
	hub.Broadcast("system", "room_A", json.RawMessage(`{"text":"hello 1"}`))
	hub.Broadcast("system", "room_A", json.RawMessage(`{"text":"hello 2"}`))
	hub.Broadcast("system", "room_A", json.RawMessage(`{"text":"hello 3"}`))
	hub.Broadcast("system", "room_A", json.RawMessage(`{"text":"hello 4"}`))

	// Healthy client should receive all 4 broadcasts in its mock socket writes channel
	for i := 1; i <= 4; i++ {
		select {
		case data := <-healthyWrites:
			var msg OutboundMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			payloadMap, ok := msg.Payload.(map[string]any)
			if !ok {
				t.Fatalf("expected payload to be map[string]any, got %T", msg.Payload)
			}
			innerPayload, ok := payloadMap["payload"].(map[string]any)
			if !ok {
				t.Fatalf("expected inner payload to be map[string]any, got %T", payloadMap["payload"])
			}
			text, ok := innerPayload["text"].(string)
			if !ok {
				t.Fatalf("expected text to be string, got %T", innerPayload["text"])
			}
			expected := fmt.Sprintf("hello %d", i)
			if text != expected {
				t.Fatalf("expected text %q, got %q", expected, text)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timeout waiting for broadcast %d on healthy client", i)
		}
	}

	// Slow client should be disconnected
	select {
	case <-slowClient.done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected slow client session to terminate")
	}

	// Double check that slow client is unregistered
	if _, exists := hub.Lookup("slow"); exists {
		t.Fatal("expected slow client to be unregistered")
	}

	close(slowBlocked)
}

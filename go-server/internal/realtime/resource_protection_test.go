package realtime

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// mockReadSocket allows pushing read messages or error responses manually.
type mockReadSocket struct {
	reads  chan readResult
	writes chan []byte
	closed chan struct{}
}

type readResult struct {
	msgType int
	payload []byte
	err     error
}

func newMockReadSocket() *mockReadSocket {
	return &mockReadSocket{
		reads:  make(chan readResult, 10),
		writes: make(chan []byte, 10),
		closed: make(chan struct{}),
	}
}

func (s *mockReadSocket) SetReadDeadline(time.Time) error   { return nil }
func (s *mockReadSocket) SetWriteDeadline(time.Time) error  { return nil }
func (s *mockReadSocket) SetPongHandler(func(string) error) {}
func (s *mockReadSocket) SetReadLimit(int64)                {}

func (s *mockReadSocket) ReadMessage() (int, []byte, error) {
	select {
	case r := <-s.reads:
		return r.msgType, r.payload, r.err
	case <-s.closed:
		return 0, nil, errors.New("socket closed")
	}
}

func (s *mockReadSocket) WriteMessage(msgType int, data []byte) error {
	select {
	case s.writes <- data:
	default:
	}
	return nil
}

func (s *mockReadSocket) Close() error {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	return nil
}

func TestOversizedMessageClose(t *testing.T) {
	t.Parallel()

	sock := newMockReadSocket()
	closed := make(chan string, 1)

	client := newClient(sock, ClientOptions{
		ConnectionID:   "conn_oversized",
		UserID:         "user_oversized",
		QueueCapacity:  2,
		Logger:         discardLogger(),
		MaxMessageSize: 100,
		OnClose: func(reason string) {
			closed <- reason
		},
	})

	// Inject the Gorilla websocket CloseError for message too big
	sock.reads <- readResult{
		err: &websocket.CloseError{
			Code: websocket.CloseMessageTooBig,
			Text: "oversized frame",
		},
	}

	done := make(chan struct{})
	go func() {
		client.Run()
		close(done)
	}()

	select {
	case reason := <-closed:
		if reason != "oversized_message" {
			t.Fatalf("expected close reason 'oversized_message', got %q", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for client close")
	}
}

func TestRateLimiterIntegration(t *testing.T) {
	t.Parallel()

	sock := newMockReadSocket()
	closed := make(chan string, 1)

	// Rate limiter with limit = 1, interval = 1 minute
	limiter := NewRateLimiter(1, time.Minute)

	client := newClient(sock, ClientOptions{
		ConnectionID:  "conn_rate",
		UserID:        "user_rate",
		QueueCapacity: 5,
		Logger:        discardLogger(),
		RateLimiter:   limiter,
		OnClose: func(reason string) {
			closed <- reason
		},
	})

	done := make(chan struct{})
	go func() {
		client.Run()
		close(done)
	}()

	// Read the connection.ready message
	select {
	case <-sock.writes:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for connection.ready")
	}

	// 1. Send first message (should succeed/be acknowledged)
	msg1 := `{"version":"1","type":"room.join","request_id":"req_1","room_id":"room_a"}`
	sock.reads <- readResult{msgType: websocket.TextMessage, payload: []byte(msg1)}

	// We expect a command.ack
	select {
	case data := <-sock.writes:
		var ack OutboundMessage
		if err := json.Unmarshal(data, &ack); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if ack.Type != EventCommandAck || ack.RequestID != "req_1" {
			t.Fatalf("expected command.ack for req_1, got %+v", ack)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ack 1")
	}

	// 2. Send second message instantly (should be rate limited)
	msg2 := `{"version":"1","type":"room.join","request_id":"req_2","room_id":"room_b"}`
	sock.reads <- readResult{msgType: websocket.TextMessage, payload: []byte(msg2)}

	// We expect a rate_limit_exceeded error
	select {
	case data := <-sock.writes:
		var errMsg OutboundMessage
		if err := json.Unmarshal(data, &errMsg); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if errMsg.Type != EventError || errMsg.RequestID != "req_2" {
			t.Fatalf("expected error for req_2, got %+v", errMsg)
		}
		payloadBytes, _ := json.Marshal(errMsg.Payload)
		var payload ErrorPayload
		_ = json.Unmarshal(payloadBytes, &payload)
		if payload.Code != ErrorRateLimitExceeded {
			t.Fatalf("expected error code %q, got %q", ErrorRateLimitExceeded, payload.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for rate limit error")
	}

	// The client session should STILL be alive.
	select {
	case reason := <-closed:
		t.Fatalf("client connection unexpectedly closed with reason %q", reason)
	case <-time.After(100 * time.Millisecond):
		// Clean up
		client.Close("test_finished")
	}
}

func TestRateLimiterIndependence(t *testing.T) {
	t.Parallel()

	// Client A with a limit = 1, interval = 1 minute
	sockA := newMockReadSocket()
	limiterA := NewRateLimiter(1, time.Minute)
	clientA := newClient(sockA, ClientOptions{
		ConnectionID:  "conn_a",
		UserID:        "user_a",
		QueueCapacity: 5,
		Logger:        discardLogger(),
		RateLimiter:   limiterA,
	})

	// Client B with a limit = 1, interval = 1 minute (independent)
	sockB := newMockReadSocket()
	limiterB := NewRateLimiter(1, time.Minute)
	clientB := newClient(sockB, ClientOptions{
		ConnectionID:  "conn_b",
		UserID:        "user_b",
		QueueCapacity: 5,
		Logger:        discardLogger(),
		RateLimiter:   limiterB,
	})

	go clientA.Run()
	go clientB.Run()

	defer clientA.Close("test_finished")
	defer clientB.Close("test_finished")

	// Drain connection.ready events
	<-sockA.writes
	<-sockB.writes

	// 1. Burn Client A's token
	msgA := `{"version":"1","type":"room.join","request_id":"req_a","room_id":"room_a"}`
	sockA.reads <- readResult{msgType: websocket.TextMessage, payload: []byte(msgA)}
	<-sockA.writes // wait for ack

	// 2. Sending a message on Client B should still succeed since Client B is independent
	msgB := `{"version":"1","type":"room.join","request_id":"req_b","room_id":"room_b"}`
	sockB.reads <- readResult{msgType: websocket.TextMessage, payload: []byte(msgB)}

	select {
	case data := <-sockB.writes:
		var ack OutboundMessage
		if err := json.Unmarshal(data, &ack); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if ack.Type != EventCommandAck || ack.RequestID != "req_b" {
			t.Fatalf("expected command.ack for req_b, got %+v", ack)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for Client B ack")
	}
}

func TestMaxRoomsPerClientEnforced(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 2) // Limit is 2 rooms per client

	sock := newMockReadSocket()
	client := newClient(sock, ClientOptions{
		ConnectionID:  "conn_rooms",
		UserID:        "user_rooms",
		QueueCapacity: 5,
		Logger:        discardLogger(),
		Hub:           hub,
	})
	hub.Register(client)

	go client.Run()
	defer client.Close("test_finished")

	// Drain connection.ready event
	<-sock.writes

	// 1. Join room 1 (Success)
	join1 := `{"version":"1","type":"room.join","request_id":"req_j1","room_id":"room_1"}`
	sock.reads <- readResult{msgType: websocket.TextMessage, payload: []byte(join1)}
	select {
	case data := <-sock.writes:
		var ack OutboundMessage
		_ = json.Unmarshal(data, &ack)
		if ack.Type != EventCommandAck {
			t.Fatalf("expected ack, got %+v", ack)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ack 1")
	}

	// 2. Re-Join room 1 (Idempotent, Success, shouldn't increment room count)
	sock.reads <- readResult{msgType: websocket.TextMessage, payload: []byte(join1)}
	select {
	case data := <-sock.writes:
		var ack OutboundMessage
		_ = json.Unmarshal(data, &ack)
		if ack.Type != EventCommandAck {
			t.Fatalf("expected ack, got %+v", ack)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ack 1 re-join")
	}

	// 3. Join room 2 (Success - capacity reaches 2)
	join2 := `{"version":"1","type":"room.join","request_id":"req_j2","room_id":"room_2"}`
	sock.reads <- readResult{msgType: websocket.TextMessage, payload: []byte(join2)}
	select {
	case data := <-sock.writes:
		var ack OutboundMessage
		_ = json.Unmarshal(data, &ack)
		if ack.Type != EventCommandAck {
			t.Fatalf("expected ack, got %+v", ack)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ack 2")
	}

	// 4. Join room 3 (Failure - capacity exceeded)
	join3 := `{"version":"1","type":"room.join","request_id":"req_j3","room_id":"room_3"}`
	sock.reads <- readResult{msgType: websocket.TextMessage, payload: []byte(join3)}
	select {
	case data := <-sock.writes:
		var errMsg OutboundMessage
		if err := json.Unmarshal(data, &errMsg); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if errMsg.Type != EventError || errMsg.RequestID != "req_j3" {
			t.Fatalf("expected error event for req_j3, got %+v", errMsg)
		}
		payloadBytes, _ := json.Marshal(errMsg.Payload)
		var payload ErrorPayload
		_ = json.Unmarshal(payloadBytes, &payload)
		if payload.Code != ErrorUnauthorized {
			t.Fatalf("expected error code %q, got %q", ErrorUnauthorized, payload.Code)
		}
		if !strings.Contains(payload.Message, "maximum room membership limit reached") {
			t.Fatalf("expected capacity error message, got %q", payload.Message)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for room join failure error")
	}
}

package app

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/blockforgelabs/go-websocket/internal/broker"
	"github.com/blockforgelabs/go-websocket/internal/config"
)

func TestMultiNodeMemoryIntegration(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Configuration for Server 1
	cfg1 := config.Config{
		Environment:           "test",
		HTTPAddress:           "127.0.0.1:0",
		ReadHeader:            time.Second,
		ReadTimeout:           2 * time.Second,
		WriteTimeout:          2 * time.Second,
		IdleTimeout:           3 * time.Second,
		ShutdownTimeout:       100 * time.Millisecond,
		AllowedOrigins:        []string{"http://localhost:3000"},
		MaxConnections:        10,
		OutboundQueueCapacity: 8,
		WebSocketReadTimeout:  time.Second,
		WebSocketWriteTimeout: time.Second,
		NodeID:                "node-mem-1",
	}

	// Configuration for Server 2
	cfg2 := cfg1
	cfg2.NodeID = "node-mem-2"

	// Create Server 1
	s1, err := NewServer(cfg1, logger)
	if err != nil {
		t.Fatalf("failed to create server 1: %v", err)
	}

	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on server 1: %v", err)
	}
	defer l1.Close()

	// Create Server 2
	s2, err := NewServer(cfg2, logger)
	if err != nil {
		t.Fatalf("failed to create server 2: %v", err)
	}

	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on server 2: %v", err)
	}
	defer l2.Close()

	// Overwrite both servers' brokers with a shared memory broker
	sharedBroker := broker.NewMemoryBroker()
	defer sharedBroker.Close()

	s1.broker = sharedBroker
	s1.hub.SetBroker(sharedBroker, "node-mem-1")
	s1.health.SetChecker(sharedBroker.Ready)

	s2.broker = sharedBroker
	s2.hub.SetBroker(sharedBroker, "node-mem-2")
	s2.health.SetChecker(sharedBroker.Ready)

	// Start Server 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done1 := make(chan error, 1)
	go func() {
		done1 <- s1.Serve(ctx, l1)
	}()

	// Start Server 2
	done2 := make(chan error, 1)
	go func() {
		done2 <- s2.Serve(ctx, l2)
	}()

	// Wait for servers to be ready
	waitForReady(t, s1)
	waitForReady(t, s2)

	// Dial Client 1 on Server 1
	wsURL1 := "ws://" + l1.Addr().String() + "/ws?user_id=user-mem-1"
	header1 := http.Header{"Origin": []string{"http://localhost:3000"}}
	c1, _, err := websocket.DefaultDialer.Dial(wsURL1, header1)
	if err != nil {
		t.Fatalf("failed to dial client 1: %v", err)
	}
	defer c1.Close()
	drainReady(t, c1)

	// Dial Client 2 on Server 2
	wsURL2 := "ws://" + l2.Addr().String() + "/ws?user_id=user-mem-2"
	header2 := http.Header{"Origin": []string{"http://localhost:3000"}}
	c2, _, err := websocket.DefaultDialer.Dial(wsURL2, header2)
	if err != nil {
		t.Fatalf("failed to dial client 2: %v", err)
	}
	defer c2.Close()
	drainReady(t, c2)

	// Join Room on Client 1
	joinMsg1 := `{"version":"1","type":"room.join","request_id":"req-join-1","room_id":"room-mem"}`
	if err := c1.WriteMessage(websocket.TextMessage, []byte(joinMsg1)); err != nil {
		t.Fatalf("client 1 failed to join room: %v", err)
	}
	expectAck(t, c1, "req-join-1")

	// Join Room on Client 2
	joinMsg2 := `{"version":"1","type":"room.join","request_id":"req-join-2","room_id":"room-mem"}`
	if err := c2.WriteMessage(websocket.TextMessage, []byte(joinMsg2)); err != nil {
		t.Fatalf("client 2 failed to join room: %v", err)
	}
	expectAck(t, c2, "req-join-2")

	// Let subscriptions register
	time.Sleep(100 * time.Millisecond)

	// Broadcast from Client 1
	broadMsg := `{"version":"1","type":"room.broadcast","request_id":"req-broad-1","room_id":"room-mem","payload":{"text":"hello in-memory"}}`
	if err := c1.WriteMessage(websocket.TextMessage, []byte(broadMsg)); err != nil {
		t.Fatalf("client 1 failed to broadcast: %v", err)
	}
	expectAck(t, c1, "req-broad-1")

	// Verify Client 2 receives the broadcast
	var receivedBroad struct {
		Type    string `json:"type"`
		Payload struct {
			RoomID   string          `json:"room_id"`
			SenderID string          `json:"sender_id"`
			Payload  json.RawMessage `json:"payload"`
		} `json:"payload"`
	}

	c2.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := c2.ReadJSON(&receivedBroad); err != nil {
		t.Fatalf("client 2 timed out or failed to read broadcast: %v", err)
	}

	if receivedBroad.Type != "room.broadcast" {
		t.Errorf("expected type 'room.broadcast', got %q", receivedBroad.Type)
	}
	if receivedBroad.Payload.RoomID != "room-mem" {
		t.Errorf("expected room 'room-mem', got %q", receivedBroad.Payload.RoomID)
	}
	if receivedBroad.Payload.SenderID != "user-mem-1" {
		t.Errorf("expected sender 'user-mem-1', got %q", receivedBroad.Payload.SenderID)
	}

	// Send Private from Client 2 to Client 1
	privMsg := `{"version":"1","type":"private.send","request_id":"req-priv-1","recipient_id":"user-mem-1","payload":{"text":"secret-mem"}}`
	if err := c2.WriteMessage(websocket.TextMessage, []byte(privMsg)); err != nil {
		t.Fatalf("client 2 failed to send private: %v", err)
	}
	expectAck(t, c2, "req-priv-1")

	// Verify Client 1 receives the private message
	var receivedPriv struct {
		Type    string `json:"type"`
		Payload struct {
			SenderID    string          `json:"sender_id"`
			RecipientID string          `json:"recipient_id"`
			Payload     json.RawMessage `json:"payload"`
		} `json:"payload"`
	}

	c1.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := c1.ReadJSON(&receivedPriv); err != nil {
		t.Fatalf("client 1 timed out or failed to read private: %v", err)
	}

	if receivedPriv.Type != "private.send" {
		t.Errorf("expected type 'private.send', got %q", receivedPriv.Type)
	}
	if receivedPriv.Payload.SenderID != "user-mem-2" {
		t.Errorf("expected sender 'user-mem-2', got %q", receivedPriv.Payload.SenderID)
	}
	if receivedPriv.Payload.RecipientID != "user-mem-1" {
		t.Errorf("expected recipient 'user-mem-1', got %q", receivedPriv.Payload.RecipientID)
	}
}

func waitForReady(t *testing.T, s *Server) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !s.Ready() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for server ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func drainReady(t *testing.T, c *websocket.Conn) {
	t.Helper()
	var ready struct {
		Type string `json:"type"`
	}
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := c.ReadJSON(&ready); err != nil {
		t.Fatalf("failed to read ready event: %v", err)
	}
	if ready.Type != "connection.ready" {
		t.Fatalf("expected connection.ready, got %q", ready.Type)
	}
}

func expectAck(t *testing.T, c *websocket.Conn, reqID string) {
	t.Helper()
	var msg map[string]any
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := c.ReadJSON(&msg); err != nil {
		t.Fatalf("failed to read message: %v", err)
	}
	typ, _ := msg["type"].(string)
	if typ != "command.ack" {
		t.Fatalf("expected command.ack, got %q (full message: %+v)", typ, msg)
	}
	req, _ := msg["request_id"].(string)
	if req != reqID {
		t.Fatalf("expected request_id %q, got %q", reqID, req)
	}
}

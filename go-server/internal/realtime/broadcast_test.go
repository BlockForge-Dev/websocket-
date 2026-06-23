package realtime

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
)

type mockBroadcastAuthorizer struct {
	allowedRooms map[string]bool
	allowPublish map[string]bool
}

func (m *mockBroadcastAuthorizer) AuthorizeJoin(userID, roomID string) error {
	if m.allowedRooms[roomID] {
		return nil
	}
	return errors.New("forbidden join")
}

func (m *mockBroadcastAuthorizer) AuthorizePublish(userID, roomID string) error {
	if m.allowPublish[roomID] {
		return nil
	}
	return errors.New("forbidden publish")
}

func (m *mockBroadcastAuthorizer) AuthorizePrivateSend(senderID, recipientID string) error {
	return nil
}

func TestBroadcastDeliveryAndSenderExclusion(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	client1 := hubTestClient(hub, "conn_1", "user_1", nil)
	client2 := hubTestClient(hub, "conn_2", "user_2", nil)
	client3 := hubTestClient(hub, "conn_3", "user_3", nil)

	// Join room_A
	if err := hub.Join(client1, "room_A"); err != nil {
		t.Fatalf("join failed: %v", err)
	}
	if err := hub.Join(client2, "room_A"); err != nil {
		t.Fatalf("join failed: %v", err)
	}
	// client3 does not join room_A

	// Broadcast from client1
	payload := json.RawMessage(`{"text":"hello"}`)
	hub.Broadcast("user_1", "room_A", payload)

	// Check client2 receives it (non-sender in room)
	select {
	case msg := <-client2.send:
		var event OutboundMessage
		if err := json.Unmarshal(msg.payload, &event); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		if event.Type != CommandRoomBroadcast {
			t.Fatalf("unexpected type: %q", event.Type)
		}
	default:
		t.Fatal("expected client2 to receive broadcast")
	}

	// Check client1 (sender) does NOT receive the broadcast
	select {
	case msg := <-client1.send:
		t.Fatalf("sender client1 unexpectedly received broadcast event: %s", string(msg.payload))
	default:
		// Passed
	}

	// Check client3 (non-member) does NOT receive the broadcast
	select {
	case msg := <-client3.send:
		t.Fatalf("non-member client3 unexpectedly received broadcast event: %s", string(msg.payload))
	default:
		// Passed
	}
}

func TestBroadcastAuthorization(t *testing.T) {
	t.Parallel()

	auth := &mockBroadcastAuthorizer{
		allowedRooms: map[string]bool{"room_A": true},
		allowPublish: map[string]bool{"room_B": true},
	}
	hub := NewHub(discardLogger(), auth, 10)

	client1 := hubTestClient(hub, "conn_1", "user_1", nil)
	client2 := hubTestClient(hub, "conn_2", "user_2", nil)

	// client1 joins room_A (member)
	if err := hub.Join(client1, "room_A"); err != nil {
		t.Fatalf("join failed: %v", err)
	}

	// 1. Member can broadcast
	if err := hub.AuthorizeBroadcast(client1, "room_A"); err != nil {
		t.Fatalf("expected member to be authorized: %v", err)
	}

	// 2. Non-member with publish permission can broadcast
	if err := hub.AuthorizeBroadcast(client2, "room_B"); err != nil {
		t.Fatalf("expected non-member with publish permission to be authorized: %v", err)
	}

	// 3. Non-member without publish permission is rejected
	if err := hub.AuthorizeBroadcast(client2, "room_A"); err == nil {
		t.Fatal("expected non-member without publish permission to be rejected")
	}
}

func TestBroadcastEmptyRoom(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	payload := json.RawMessage(`{"text":"hello"}`)

	// Broadcast to non-existent room: should not panic or fail
	hub.Broadcast("user_1", "room_empty", payload)
}

func TestConcurrentJoinLeaveAndBroadcast(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	clients := make([]*Client, 50)
	for i := 0; i < len(clients); i++ {
		clients[i] = hubTestClient(hub, string(rune(i)), string(rune(i)), nil)
	}

	var wg sync.WaitGroup
	// Run concurrent join, leave, and broadcasts
	for i := 0; i < len(clients); i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			hub.Register(clients[i])
			_ = hub.Join(clients[i], "room_A")
			hub.Broadcast(clients[i].UserID(), "room_A", json.RawMessage(`{"index":1}`))
			hub.Leave(clients[i], "room_A")
			hub.Unregister(clients[i])
		}()
	}

	wg.Wait()
}

func TestClientBroadcastWebSocketIntegration(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade error: %v", err)
			return
		}
		var client *Client
		client = NewClient(conn, ClientOptions{
			ConnectionID:  r.URL.Query().Get("conn_id"),
			UserID:        r.URL.Query().Get("user_id"),
			QueueCapacity: 8,
			Logger:        discardLogger(),
			Hub:           hub,
			OnClose: func(reason string) {
				hub.Unregister(client)
			},
		})
		hub.Register(client)
		client.Run()
	}))
	defer server.Close()

	// Dial client 1
	c1, _, err := websocket.DefaultDialer.Dial(webSocketTestURL(server.URL)+"?conn_id=conn_1&user_id=user_1", nil)
	if err != nil {
		t.Fatalf("dial c1: %v", err)
	}
	defer c1.Close()

	// Dial client 2
	c2, _, err := websocket.DefaultDialer.Dial(webSocketTestURL(server.URL)+"?conn_id=conn_2&user_id=user_2", nil)
	if err != nil {
		t.Fatalf("dial c2: %v", err)
	}
	defer c2.Close()

	// Read ready events
	var ready OutboundMessage
	if err := c1.ReadJSON(&ready); err != nil {
		t.Fatalf("c1 read ready: %v", err)
	}
	if err := c2.ReadJSON(&ready); err != nil {
		t.Fatalf("c2 read ready: %v", err)
	}

	// Join both to "room_shared"
	joinCmd := `{"version":"1","type":"room.join","request_id":"req_join","room_id":"room_shared"}`
	if err := c1.WriteMessage(websocket.TextMessage, []byte(joinCmd)); err != nil {
		t.Fatalf("c1 write join: %v", err)
	}
	if err := c2.WriteMessage(websocket.TextMessage, []byte(joinCmd)); err != nil {
		t.Fatalf("c2 write join: %v", err)
	}

	// Read acks
	var ack OutboundMessage
	if err := c1.ReadJSON(&ack); err != nil {
		t.Fatalf("c1 read ack: %v", err)
	}
	if err := c2.ReadJSON(&ack); err != nil {
		t.Fatalf("c2 read ack: %v", err)
	}

	// Client 1 broadcasts
	broadcastCmd := `{"version":"1","type":"room.broadcast","request_id":"req_bc","room_id":"room_shared","payload":{"msg":"hello"}}`
	if err := c1.WriteMessage(websocket.TextMessage, []byte(broadcastCmd)); err != nil {
		t.Fatalf("c1 write broadcast: %v", err)
	}

	// Client 1 (sender) gets command.ack only
	if err := c1.ReadJSON(&ack); err != nil {
		t.Fatalf("c1 read ack: %v", err)
	}
	if ack.Type != EventCommandAck || ack.RequestID != "req_bc" {
		t.Fatalf("expected c1 ack, got: %+v", ack)
	}

	// Client 2 (recipient) gets the room.broadcast event
	var event OutboundMessage
	if err := c2.ReadJSON(&event); err != nil {
		t.Fatalf("c2 read broadcast event: %v", err)
	}
	if event.Type != CommandRoomBroadcast {
		t.Fatalf("expected event type %q, got: %q", CommandRoomBroadcast, event.Type)
	}

	// Verify payload contains room_id, sender_id, and message payload
	payloadBytes, err := json.Marshal(event.Payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	var bcPayload struct {
		RoomID   string          `json:"room_id"`
		SenderID string          `json:"sender_id"`
		Payload  json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(payloadBytes, &bcPayload); err != nil {
		t.Fatalf("unmarshal broadcast payload: %v", err)
	}
	if bcPayload.RoomID != "room_shared" || bcPayload.SenderID != "user_1" {
		t.Fatalf("unexpected broadcast event payload: %+v", bcPayload)
	}
}

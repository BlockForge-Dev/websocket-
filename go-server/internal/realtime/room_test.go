package realtime

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
)

type mockRoomAuthorizer struct {
	allowedRooms map[string]bool
}

func (m *mockRoomAuthorizer) AuthorizeJoin(userID, roomID string) error {
	if m.allowedRooms[roomID] {
		return nil
	}
	return errors.New("forbidden")
}

func (m *mockRoomAuthorizer) AuthorizePublish(userID, roomID string) error {
	return nil
}

func (m *mockRoomAuthorizer) AuthorizePrivateSend(senderID, recipientID string) error {
	return nil
}

func TestRoomMembershipIdempotencyAndEmptyRooms(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	client1 := hubTestClient(hub, "conn_1", "user_1", nil)
	client2 := hubTestClient(hub, "conn_2", "user_2", nil)

	// Idempotent join
	if err := hub.Join(client1, "room_A"); err != nil {
		t.Fatalf("unexpected error joining room: %v", err)
	}
	if err := hub.Join(client1, "room_A"); err != nil {
		t.Fatalf("unexpected error joining room again: %v", err)
	}

	hub.mu.RLock()
	clients, ok := hub.rooms["room_A"]
	if !ok || len(clients) != 1 || clients["conn_1"] != client1 {
		hub.mu.RUnlock()
		t.Fatalf("unexpected room state: %v", hub.rooms)
	}
	hub.mu.RUnlock()

	// Another client joins
	if err := hub.Join(client2, "room_A"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hub.mu.RLock()
	if len(hub.rooms["room_A"]) != 2 {
		hub.mu.RUnlock()
		t.Fatalf("expected 2 clients, got %d", len(hub.rooms["room_A"]))
	}
	hub.mu.RUnlock()

	// Idempotent leave
	hub.Leave(client1, "room_A")
	hub.Leave(client1, "room_A")

	hub.mu.RLock()
	if len(hub.rooms["room_A"]) != 1 || hub.rooms["room_A"]["conn_2"] != client2 {
		hub.mu.RUnlock()
		t.Fatalf("unexpected state after client1 leaves: %v", hub.rooms)
	}
	hub.mu.RUnlock()

	// Empty room removal
	hub.Leave(client2, "room_A")
	hub.mu.RLock()
	if _, exists := hub.rooms["room_A"]; exists {
		hub.mu.RUnlock()
		t.Fatalf("expected room to be removed when empty")
	}
	hub.mu.RUnlock()
}

func TestRoomMembershipAuthorization(t *testing.T) {
	t.Parallel()

	auth := &mockRoomAuthorizer{
		allowedRooms: map[string]bool{"public": true},
	}
	hub := NewHub(discardLogger(), auth, 10)
	client := hubTestClient(hub, "conn_1", "user_1", nil)

	// Authorized join
	if err := hub.Join(client, "public"); err != nil {
		t.Fatalf("expected public join to be authorized: %v", err)
	}

	// Unauthorized join
	if err := hub.Join(client, "private"); err == nil {
		t.Fatal("expected private join to be rejected")
	}

	hub.mu.RLock()
	if _, exists := hub.rooms["private"]; exists {
		hub.mu.RUnlock()
		t.Fatal("expected unauthorized room join not to mutate state")
	}
	hub.mu.RUnlock()
}

func TestRoomMembershipDisconnectCleanup(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	client := hubTestClient(hub, "conn_1", "user_1", nil)

	hub.Register(client)
	if err := hub.Join(client, "room_A"); err != nil {
		t.Fatalf("join failed: %v", err)
	}
	if err := hub.Join(client, "room_B"); err != nil {
		t.Fatalf("join failed: %v", err)
	}

	// Disconnect client
	if !hub.Unregister(client) {
		t.Fatal("unregister failed")
	}

	hub.mu.RLock()
	if _, exists := hub.rooms["room_A"]; exists {
		hub.mu.RUnlock()
		t.Fatal("expected room_A to be cleaned up")
	}
	if _, exists := hub.rooms["room_B"]; exists {
		hub.mu.RUnlock()
		t.Fatal("expected room_B to be cleaned up")
	}
	if _, exists := hub.clientRooms["conn_1"]; exists {
		hub.mu.RUnlock()
		t.Fatal("expected clientRooms record to be cleaned up")
	}
	hub.mu.RUnlock()
}

func TestRoomMembershipDuplicateSessionReplacement(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	oldClient := hubTestClient(hub, "conn_old", "user_1", nil)
	newClient := hubTestClient(hub, "conn_new", "user_1", nil)

	hub.Register(oldClient)
	if err := hub.Join(oldClient, "room_A"); err != nil {
		t.Fatalf("join failed: %v", err)
	}

	// Registering new connection replacements
	hub.Register(newClient)

	// Since Register calls replaced.Close, which runs onClose -> Unregister,
	// oldClient's room memberships must be cleaned up, and empty rooms removed.
	hub.mu.RLock()
	if _, exists := hub.rooms["room_A"]; exists {
		hub.mu.RUnlock()
		t.Fatal("expected oldClient's room membership to be cleaned up after replacement")
	}
	hub.mu.RUnlock()
}

func TestClientJoinLeaveWebSocketCommands(t *testing.T) {
	t.Parallel()

	auth := &mockRoomAuthorizer{
		allowedRooms: map[string]bool{"allowed": true},
	}
	hub := NewHub(discardLogger(), auth, 10)

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
			ConnectionID:  "conn_1",
			UserID:        "user_1",
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

	conn, _, err := websocket.DefaultDialer.Dial(webSocketTestURL(server.URL), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Read ready event
	var ready OutboundMessage
	if err := conn.ReadJSON(&ready); err != nil {
		t.Fatalf("read ready: %v", err)
	}

	// Send unauthorized join
	unauthorizedCmd := `{"version":"1","type":"room.join","request_id":"req_unauth","room_id":"denied"}`
	if err := conn.WriteMessage(websocket.TextMessage, []byte(unauthorizedCmd)); err != nil {
		t.Fatalf("write unauth join: %v", err)
	}

	var errorEvent OutboundMessage
	if err := conn.ReadJSON(&errorEvent); err != nil {
		t.Fatalf("read error event: %v", err)
	}
	if errorEvent.Type != EventError || errorEvent.RequestID != "req_unauth" {
		t.Fatalf("unexpected error response: %+v", errorEvent)
	}

	payloadBytes, _ := json.Marshal(errorEvent.Payload)
	var errPayload ErrorPayload
	if err := json.Unmarshal(payloadBytes, &errPayload); err != nil {
		t.Fatalf("unmarshal error payload: %v", err)
	}
	if errPayload.Code != ErrorUnauthorized {
		t.Fatalf("expected code %q, got %q", ErrorUnauthorized, errPayload.Code)
	}

	// Send authorized join
	authorizedCmd := `{"version":"1","type":"room.join","request_id":"req_auth","room_id":"allowed"}`
	if err := conn.WriteMessage(websocket.TextMessage, []byte(authorizedCmd)); err != nil {
		t.Fatalf("write auth join: %v", err)
	}

	var ackEvent OutboundMessage
	if err := conn.ReadJSON(&ackEvent); err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if ackEvent.Type != EventCommandAck || ackEvent.RequestID != "req_auth" {
		t.Fatalf("expected ack, got: %+v", ackEvent)
	}

	// Send leave command
	leaveCmd := `{"version":"1","type":"room.leave","request_id":"req_leave","room_id":"allowed"}`
	if err := conn.WriteMessage(websocket.TextMessage, []byte(leaveCmd)); err != nil {
		t.Fatalf("write leave: %v", err)
	}

	if err := conn.ReadJSON(&ackEvent); err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if ackEvent.Type != EventCommandAck || ackEvent.RequestID != "req_leave" {
		t.Fatalf("expected ack, got: %+v", ackEvent)
	}
}

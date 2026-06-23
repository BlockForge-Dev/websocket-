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

type mockPrivateAuthorizer struct {
	allowedPrivateSends map[string]bool
}

func (m *mockPrivateAuthorizer) AuthorizeJoin(userID, roomID string) error {
	return nil
}

func (m *mockPrivateAuthorizer) AuthorizePublish(userID, roomID string) error {
	return nil
}

func (m *mockPrivateAuthorizer) AuthorizePrivateSend(senderID, recipientID string) error {
	key := senderID + "->" + recipientID
	if m.allowedPrivateSends[key] {
		return nil
	}
	return errors.New("unauthorized private send")
}

func TestPrivateDeliverySuccessAndOffline(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	client1 := hubTestClient(hub, "conn_1", "user_1", nil)
	client2 := hubTestClient(hub, "conn_2", "user_2", nil)

	hub.Register(client1)
	hub.Register(client2)

	// 1. Send to online recipient
	payload := json.RawMessage(`{"text":"hello"}`)
	err := hub.SendPrivate("user_1", "user_2", payload)
	if err != nil {
		t.Fatalf("expected successful send: %v", err)
	}

	select {
	case msg := <-client2.send:
		var event OutboundMessage
		if err := json.Unmarshal(msg.payload, &event); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		if event.Type != CommandPrivateSend {
			t.Fatalf("unexpected event type: %q", event.Type)
		}
	default:
		t.Fatal("expected client2 to receive the private message")
	}

	// 2. Send to offline recipient
	err = hub.SendPrivate("user_1", "user_offline", payload)
	if !errors.Is(err, ErrRecipientOffline) {
		t.Fatalf("expected ErrRecipientOffline, got %v", err)
	}
}

func TestPrivateDeliveryAuthorization(t *testing.T) {
	t.Parallel()

	auth := &mockPrivateAuthorizer{
		allowedPrivateSends: map[string]bool{"user_1->user_2": true},
	}
	hub := NewHub(discardLogger(), auth, 10)

	client1 := hubTestClient(hub, "conn_1", "user_1", nil)
	client2 := hubTestClient(hub, "conn_2", "user_2", nil)

	hub.Register(client1)
	hub.Register(client2)

	// 1. Authorized private send
	if err := hub.authorizer.AuthorizePrivateSend("user_1", "user_2"); err != nil {
		t.Fatalf("expected authorized send: %v", err)
	}

	// 2. Unauthorized private send
	if err := hub.authorizer.AuthorizePrivateSend("user_1", "user_3"); err == nil {
		t.Fatal("expected unauthorized send to fail")
	}
}

func TestPrivateDeliveryOverloaded(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	client1 := hubTestClient(hub, "conn_1", "user_1", nil)
	var client2 *Client
	client2 = NewClient(nil, ClientOptions{
		ConnectionID:  "conn_2",
		UserID:        "user_2",
		QueueCapacity: 1,
		Logger:        discardLogger(),
		Hub:           hub,
		OnClose: func(reason string) {
			hub.Unregister(client2)
		},
	})
	hub.Register(client1)
	hub.Register(client2)

	payload := json.RawMessage(`{"text":"hello"}`)

	// Fill queue with one message
	err := hub.SendPrivate("user_1", "user_2", payload)
	if err != nil {
		t.Fatalf("expected successful send: %v", err)
	}

	// Second send should fail because queue capacity is 1 and it is full
	err = hub.SendPrivate("user_1", "user_2", payload)
	if !errors.Is(err, ErrRecipientOverloaded) {
		t.Fatalf("expected ErrRecipientOverloaded, got %v", err)
	}
}

func TestConcurrentPrivateDeliveryAndDisconnect(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	client1 := hubTestClient(hub, "conn_1", "user_1", nil)
	client2 := hubTestClient(hub, "conn_2", "user_2", nil)

	hub.Register(client1)
	hub.Register(client2)

	var wg sync.WaitGroup
	wg.Add(2)

	// Send messages concurrently
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = hub.SendPrivate("user_1", "user_2", json.RawMessage(`{"text":"test"}`))
		}
	}()

	// Disconnect client concurrently
	go func() {
		defer wg.Done()
		hub.Unregister(client2)
	}()

	wg.Wait()
}

func TestClientPrivateSendWebSocketIntegration(t *testing.T) {
	t.Parallel()

	auth := &mockPrivateAuthorizer{
		allowedPrivateSends: map[string]bool{"user_1->user_2": true, "user_1->user_offline": true},
	}
	hub := NewHub(discardLogger(), auth, 10)

	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade error: %v", r)
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

	// Dial user_1
	c1, _, err := websocket.DefaultDialer.Dial(webSocketTestURL(server.URL)+"?conn_id=conn_1&user_id=user_1", nil)
	if err != nil {
		t.Fatalf("dial c1: %v", err)
	}
	defer c1.Close()

	// Dial user_2
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

	// 1. Authorized private send
	privateCmd := `{"version":"1","type":"private.send","request_id":"req_pv1","recipient_id":"user_2","payload":{"text":"hello"}}`
	if err := c1.WriteMessage(websocket.TextMessage, []byte(privateCmd)); err != nil {
		t.Fatalf("write private send: %v", err)
	}

	// c1 (sender) gets command.ack
	var ack OutboundMessage
	if err := c1.ReadJSON(&ack); err != nil {
		t.Fatalf("c1 read ack: %v", err)
	}
	if ack.Type != EventCommandAck || ack.RequestID != "req_pv1" {
		t.Fatalf("expected c1 ack, got: %+v", ack)
	}

	// c2 (recipient) gets the private message event
	var event OutboundMessage
	if err := c2.ReadJSON(&event); err != nil {
		t.Fatalf("c2 read private event: %v", err)
	}
	if event.Type != CommandPrivateSend {
		t.Fatalf("expected event type %q, got: %q", CommandPrivateSend, event.Type)
	}

	// 2. Unauthorized private send
	unauthorizedCmd := `{"version":"1","type":"private.send","request_id":"req_pv2","recipient_id":"user_3","payload":{"text":"hello"}}`
	if err := c1.WriteMessage(websocket.TextMessage, []byte(unauthorizedCmd)); err != nil {
		t.Fatalf("write unauthorized private send: %v", err)
	}

	var errorEvent OutboundMessage
	if err := c1.ReadJSON(&errorEvent); err != nil {
		t.Fatalf("c1 read error event: %v", err)
	}
	if errorEvent.Type != EventError || errorEvent.RequestID != "req_pv2" {
		t.Fatalf("expected error event, got: %+v", errorEvent)
	}
	payloadBytes, _ := json.Marshal(errorEvent.Payload)
	var errPayload ErrorPayload
	if err := json.Unmarshal(payloadBytes, &errPayload); err != nil {
		t.Fatalf("unmarshal error payload: %v", err)
	}
	if errPayload.Code != ErrorUnauthorized {
		t.Fatalf("expected code %q, got %q", ErrorUnauthorized, errPayload.Code)
	}

	// 3. Send to offline recipient
	offlineCmd := `{"version":"1","type":"private.send","request_id":"req_pv3","recipient_id":"user_offline","payload":{"text":"hello"}}`
	if err := c1.WriteMessage(websocket.TextMessage, []byte(offlineCmd)); err != nil {
		t.Fatalf("write offline private send: %v", err)
	}

	if err := c1.ReadJSON(&errorEvent); err != nil {
		t.Fatalf("c1 read error event: %v", err)
	}
	if errorEvent.Type != EventError || errorEvent.RequestID != "req_pv3" {
		t.Fatalf("expected error event, got: %+v", errorEvent)
	}
	payloadBytes, _ = json.Marshal(errorEvent.Payload)
	if err := json.Unmarshal(payloadBytes, &errPayload); err != nil {
		t.Fatalf("unmarshal error payload: %v", err)
	}
	if errPayload.Code != ErrorRecipientOffline {
		t.Fatalf("expected code %q, got %q", ErrorRecipientOffline, errPayload.Code)
	}
}

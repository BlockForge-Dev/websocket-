package realtime

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHeartbeatKeepsHealthyClientConnected(t *testing.T) {
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
			ConnectionID:  "conn_healthy",
			UserID:        "user_healthy",
			QueueCapacity: 8,
			Logger:        discardLogger(),
			Hub:           hub,
			ReadTimeout:   100 * time.Millisecond,
			PingInterval:  20 * time.Millisecond,
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

	// Read from connection periodically to let Gorilla WebSocket automatically respond to pings with pongs
	stopChan := make(chan struct{})
	defer close(stopChan)

	go func() {
		for {
			select {
			case <-stopChan:
				return
			default:
				// Read message (including control frames like ping)
				_, _, err := conn.ReadMessage()
				if err != nil {
					return
				}
			}
		}
	}()

	// Wait 250ms, which is much longer than the 100ms ReadTimeout
	time.Sleep(250 * time.Millisecond)

	// Check if client is still registered in the Hub
	if hub.Count() != 1 {
		t.Fatal("expected healthy client to remain connected and registered")
	}
}

func TestHeartbeatTimeoutClosesStaleClient(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}

	closedReason := make(chan string, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade error: %v", err)
			return
		}
		var client *Client
		client = NewClient(conn, ClientOptions{
			ConnectionID:  "conn_stale",
			UserID:        "user_stale",
			QueueCapacity: 8,
			Logger:        discardLogger(),
			Hub:           hub,
			ReadTimeout:   40 * time.Millisecond,
			PingInterval:  10 * time.Millisecond,
			OnClose: func(reason string) {
				closedReason <- reason
				hub.Unregister(client)
			},
		})
		hub.Register(client)
		_ = hub.Join(client, "room_A")
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

	// We set a custom PingHandler on the dialer client that ignores/suppresses responding with pongs.
	// That way the server will never receive pongs and the connection will time out.
	conn.SetPingHandler(func(appData string) error {
		// Ignore the ping and do not send a pong
		return nil
	})

	// Start reading to process pings (which will execute the custom handler above that ignores them)
	go func() {
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}()

	// Wait for the close reason to be received
	select {
	case reason := <-closedReason:
		if reason != "heartbeat_timeout" {
			t.Fatalf("expected close reason 'heartbeat_timeout', got %q", reason)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected connection to timeout and close")
	}

	// Verify client is unregistered and room state is cleaned up
	if hub.Count() != 0 {
		t.Fatal("expected stale client to be unregistered")
	}
	hub.mu.RLock()
	if _, exists := hub.rooms["room_A"]; exists {
		hub.mu.RUnlock()
		t.Fatal("expected room_A to be cleaned up")
	}
	hub.mu.RUnlock()
}

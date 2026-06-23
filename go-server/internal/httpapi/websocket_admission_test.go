package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type blockingSessionHandler struct {
	started chan struct{}
	release chan struct{}
}

func (h *blockingSessionHandler) Handle(*websocket.Conn, Session) {
	close(h.started)
	<-h.release
}

func TestWebSocketAdmissionRejectsConnectionsBeyondCapacity(t *testing.T) {
	t.Parallel()

	health := NewHealth()
	health.SetReady(true)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sessionHandler := &blockingSessionHandler{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	server := httptest.NewServer(Router(health, NewWebSocketHandler(WebSocketOptions{
		Health:         health,
		Authenticator:  DevelopmentAuthenticator{},
		SessionHandler: sessionHandler,
		AllowedOrigins: []string{"http://localhost:3000"},
		MaxConnections: 1,
		Logger:         logger,
	})))
	t.Cleanup(server.Close)

	header := http.Header{"Origin": []string{"http://localhost:3000"}}
	first, _, err := websocket.DefaultDialer.Dial(
		webSocketURL(server.URL)+"/ws?user_id=user_1",
		header,
	)
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })

	select {
	case <-sessionHandler.started:
	case <-time.After(time.Second):
		t.Fatal("first session did not acquire admission capacity")
	}

	second, response, err := websocket.DefaultDialer.Dial(
		webSocketURL(server.URL)+"/ws?user_id=user_2",
		header,
	)
	if second != nil {
		_ = second.Close()
	}
	if err == nil {
		t.Fatal("second connection unexpectedly succeeded")
	}
	if response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second status = %v, want %d", responseStatus(response), http.StatusServiceUnavailable)
	}

	close(sessionHandler.release)
}

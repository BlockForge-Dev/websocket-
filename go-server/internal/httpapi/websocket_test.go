package httpapi

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWebSocketUpgradeHandsConnectionToSessionOwner(t *testing.T) {
	t.Parallel()

	health := NewHealth()
	health.SetReady(true)
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	sessionHandler := &recordingSessionHandler{
		session: make(chan Session, 1),
		release: make(chan struct{}),
	}
	server := httptest.NewServer(Router(health, NewWebSocketHandler(WebSocketOptions{
		Health:         health,
		Authenticator:  DevelopmentAuthenticator{},
		SessionHandler: sessionHandler,
		AllowedOrigins: []string{"http://localhost:3000"},
		MaxConnections: 2,
		Logger:         logger,
	})))
	t.Cleanup(server.Close)

	header := http.Header{"Origin": []string{"http://localhost:3000"}}
	connection, response, err := websocket.DefaultDialer.Dial(
		webSocketURL(server.URL)+"/ws?user_id=user_123",
		header,
	)
	if err != nil {
		t.Fatalf("dial WebSocket: %v (response=%v)", err, response)
	}
	t.Cleanup(func() { _ = connection.Close() })

	select {
	case session := <-sessionHandler.session:
		if !strings.HasPrefix(session.ConnectionID, "conn_") || session.UserID != "user_123" {
			t.Fatalf("unexpected handoff: %+v", session)
		}
	case <-time.After(time.Second):
		t.Fatal("accepted connection was not handed to the session owner")
	}
	close(sessionHandler.release)

	if !strings.Contains(logs.String(), "websocket_upgrade_accepted") {
		t.Fatalf("accepted upgrade was not classified in logs: %s", logs.String())
	}
}

func TestWebSocketUpgradeRejectsInvalidRequestsBeforeUpgrade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		authenticator Authenticator
		query         string
		origin        string
		wantStatus    int
	}{
		{
			name:          "missing development identity",
			authenticator: DevelopmentAuthenticator{},
			origin:        "http://localhost:3000",
			wantStatus:    http.StatusUnauthorized,
		},
		{
			name:          "invalid origin",
			authenticator: DevelopmentAuthenticator{},
			query:         "?user_id=user_123",
			origin:        "https://attacker.example",
			wantStatus:    http.StatusForbidden,
		},
		{
			name:          "production verifier unavailable",
			authenticator: RejectingAuthenticator{},
			query:         "?user_id=user_123",
			origin:        "https://app.example",
			wantStatus:    http.StatusUnauthorized,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			health := NewHealth()
			health.SetReady(true)
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			server := httptest.NewServer(Router(health, NewWebSocketHandler(WebSocketOptions{
				Health:         health,
				Authenticator:  test.authenticator,
				SessionHandler: closingSessionHandler{},
				AllowedOrigins: []string{"http://localhost:3000", "https://app.example"},
				MaxConnections: 1,
				Logger:         logger,
			})))
			t.Cleanup(server.Close)

			header := http.Header{"Origin": []string{test.origin}}
			connection, response, err := websocket.DefaultDialer.Dial(
				webSocketURL(server.URL)+"/ws"+test.query,
				header,
			)
			if connection != nil {
				_ = connection.Close()
			}
			if err == nil {
				t.Fatal("upgrade unexpectedly succeeded")
			}
			if response == nil || response.StatusCode != test.wantStatus {
				t.Fatalf("status = %v, want %d; error=%v", responseStatus(response), test.wantStatus, err)
			}
		})
	}
}

func TestWebSocketUpgradeRequiresReadyServerAndUpgradeHeaders(t *testing.T) {
	t.Parallel()

	health := NewHealth()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := Router(health, NewWebSocketHandler(WebSocketOptions{
		Health:         health,
		Authenticator:  DevelopmentAuthenticator{},
		SessionHandler: closingSessionHandler{},
		AllowedOrigins: []string{"http://localhost:3000"},
		MaxConnections: 1,
		Logger:         logger,
	}))

	request := httptest.NewRequest(http.MethodGet, "/ws?user_id=user_123", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unready status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}

	health.SetReady(true)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUpgradeRequired {
		t.Fatalf("plain HTTP status = %d, want %d", response.Code, http.StatusUpgradeRequired)
	}
}

func webSocketURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func responseStatus(response *http.Response) any {
	if response == nil {
		return nil
	}
	return response.StatusCode
}

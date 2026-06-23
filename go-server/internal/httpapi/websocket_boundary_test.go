package httpapi

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWebSocketRouteRejectsUnsupportedMethod(t *testing.T) {
	t.Parallel()

	health := NewHealth()
	health.SetReady(true)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := Router(health, NewWebSocketHandler(WebSocketOptions{
		Health:         health,
		Authenticator:  DevelopmentAuthenticator{},
		SessionHandler: closingSessionHandler{},
		AllowedOrigins: []string{"http://localhost:3000"},
		MaxConnections: 1,
		Logger:         logger,
	}))

	request := httptest.NewRequest(http.MethodPost, "/ws?user_id=user_123", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
}

func TestRejectedUpgradeIsClassifiedInStructuredLogs(t *testing.T) {
	t.Parallel()

	health := NewHealth()
	health.SetReady(true)
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	handler := NewWebSocketHandler(WebSocketOptions{
		Health:         health,
		Authenticator:  DevelopmentAuthenticator{},
		SessionHandler: closingSessionHandler{},
		AllowedOrigins: []string{"http://localhost:3000"},
		MaxConnections: 1,
		Logger:         logger,
	})

	request := httptest.NewRequest(http.MethodGet, "/ws?user_id=user_123", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUpgradeRequired {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUpgradeRequired)
	}
	if !strings.Contains(logs.String(), "websocket_upgrade_rejected") ||
		!strings.Contains(logs.String(), "upgrade_required") {
		t.Fatalf("rejection was not classified: %s", logs.String())
	}
}

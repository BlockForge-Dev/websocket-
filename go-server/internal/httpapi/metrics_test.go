package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blockforgelabs/go-websocket/internal/observability"
)

func TestMetricsEndpoint(t *testing.T) {
	t.Parallel()

	health := NewHealth()
	handler := Router(health)

	// Increment a test metric to check for persistence and correctness in endpoint response
	observability.IncrementUpgradesAttempted()

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content type = %q, want application/json", response.Header().Get("Content-Type"))
	}

	var snapshot observability.MetricsSnapshot
	if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
		t.Fatalf("failed to decode metrics JSON: %v", err)
	}

	if snapshot.UpgradesAttempted <= 0 {
		t.Fatalf("expected upgrades_attempted metric to be > 0, got %d", snapshot.UpgradesAttempted)
	}
}

func TestMetricsRoutesOnlyAllowGET(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	response := httptest.NewRecorder()
	Router(NewHealth()).ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
}

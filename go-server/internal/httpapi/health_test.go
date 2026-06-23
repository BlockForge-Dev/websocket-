package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthAndReadinessSemantics(t *testing.T) {
	t.Parallel()

	health := NewHealth()
	handler := Router(health)

	assertEndpoint(t, handler, "/healthz", http.StatusOK, "ok")
	assertEndpoint(t, handler, "/readyz", http.StatusServiceUnavailable, "not_ready")

	health.SetReady(true)
	assertEndpoint(t, handler, "/readyz", http.StatusOK, "ready")

	health.SetReady(false)
	assertEndpoint(t, handler, "/healthz", http.StatusOK, "ok")
	assertEndpoint(t, handler, "/readyz", http.StatusServiceUnavailable, "not_ready")
}

func TestHealthRoutesOnlyAllowGET(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	response := httptest.NewRecorder()
	Router(NewHealth()).ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
}

func assertEndpoint(t *testing.T, handler http.Handler, path string, status int, state string) {
	t.Helper()

	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != status {
		t.Fatalf("%s status = %d, want %d", path, response.Code, status)
	}
	if response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("%s content type = %q, want application/json", path, response.Header().Get("Content-Type"))
	}

	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode %s response: %v", path, err)
	}
	if body.Status != state {
		t.Fatalf("%s state = %q, want %q", path, body.Status, state)
	}
}

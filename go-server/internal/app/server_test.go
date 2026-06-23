package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/blockforgelabs/go-websocket/internal/config"
)

func TestServePublishesHealthAndStopsOnCancellation(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	server, err := NewServer(testConfig(), logger)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx, listener)
	}()

	waitFor(t, time.Second, server.Ready)
	assertHTTPStatus(t, "http://"+listener.Addr().String()+"/healthz", http.StatusOK)
	assertHTTPStatus(t, "http://"+listener.Addr().String()+"/readyz", http.StatusOK)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned an error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop within the expected deadline")
	}

	if server.Ready() {
		t.Fatal("server remained ready after shutdown")
	}
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness after shutdown = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}

	for _, event := range []string{"server_started", "server_shutdown_started", "server_stopped"} {
		if !strings.Contains(logs.String(), event) {
			t.Fatalf("structured logs do not contain %q: %s", event, logs.String())
		}
	}
}

func TestServerUsesExplicitHTTPTimeouts(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	server, err := NewServer(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	if server.httpServer.ReadHeaderTimeout != cfg.ReadHeader ||
		server.httpServer.ReadTimeout != cfg.ReadTimeout ||
		server.httpServer.WriteTimeout != cfg.WriteTimeout ||
		server.httpServer.IdleTimeout != cfg.IdleTimeout {
		t.Fatalf("HTTP server timeouts do not match configuration: %+v", server.httpServer)
	}
}

func TestGracefulShutdownClosesActiveConnections(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := NewServer(testConfig(), logger)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx, listener)
	}()

	waitFor(t, time.Second, server.Ready)

	wsURL := "ws://" + listener.Addr().String() + "/ws?user_id=user_graceful"
	connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial WebSocket: %v", err)
	}
	defer connection.Close()

	// Wait for connection ready event
	var ready struct {
		Version string `json:"version"`
		Type    string `json:"type"`
	}
	if err := connection.ReadJSON(&ready); err != nil {
		t.Fatalf("read ready event: %v", err)
	}
	if ready.Type != "connection.ready" {
		t.Fatalf("expected ready event, got %q", ready.Type)
	}

	// Verify the hub has 1 connection
	if server.ActiveClients() != 1 {
		t.Fatalf("expected 1 active client, got %d", server.ActiveClients())
	}

	// Trigger graceful shutdown
	cancel()

	// The first message after shutdown should be server.draining
	connection.SetReadDeadline(time.Now().Add(time.Second))
	var draining struct {
		Version string `json:"version"`
		Type    string `json:"type"`
	}
	if err := connection.ReadJSON(&draining); err != nil {
		t.Fatalf("read draining event: %v", err)
	}
	if draining.Type != "server.draining" {
		t.Fatalf("expected server.draining event, got %q", draining.Type)
	}

	// Verify server termination
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned an error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not stop within the expected deadline")
	}

	// Verify active client count drops to 0
	if server.ActiveClients() != 0 {
		t.Fatalf("expected 0 active clients after shutdown, got %d", server.ActiveClients())
	}
}

func TestGracefulDrainingRejectsNewConnections(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	server, err := NewServer(testConfig(), logger)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx, listener)
	}()

	waitFor(t, time.Second, server.Ready)

	// Establish a connection before shutdown
	wsURL := "ws://" + addr + "/ws?user_id=user_drain"
	conn1, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial WebSocket: %v", err)
	}
	defer conn1.Close()

	// Drain connection.ready
	conn1.SetReadDeadline(time.Now().Add(time.Second))
	var ready struct {
		Type string `json:"type"`
	}
	_ = conn1.ReadJSON(&ready)

	// Trigger shutdown
	cancel()

	// Read draining event
	var draining struct {
		Type string `json:"type"`
	}
	conn1.SetReadDeadline(time.Now().Add(time.Second))
	_ = conn1.ReadJSON(&draining)
	if draining.Type != "server.draining" {
		t.Fatalf("expected server.draining, got %q", draining.Type)
	}

	// Wait for server to finish
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not stop")
	}

	// Verify readiness became false
	if server.Ready() {
		t.Fatal("server should not be ready after shutdown")
	}

	// Verify logs contain the drain lifecycle events
	logOutput := logs.String()
	if !strings.Contains(logOutput, "server_shutdown_started") {
		t.Fatal("expected server_shutdown_started log")
	}
	if !strings.Contains(logOutput, "server_drain_started") {
		t.Fatal("expected server_drain_started log")
	}
}

func assertHTTPStatus(t *testing.T, url string, want int) {
	t.Helper()

	response, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer response.Body.Close()

	if response.StatusCode != want {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("GET %s status = %d, want %d; body=%s", url, response.StatusCode, want, body)
	}

	var payload map[string]string
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode GET %s: %v", url, err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not satisfied before timeout")
		}
		time.Sleep(time.Millisecond)
	}
}

func testConfig() config.Config {
	return config.Config{
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
	}
}

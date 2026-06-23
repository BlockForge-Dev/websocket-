package app

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/blockforgelabs/go-websocket/internal/realtime"
	"github.com/gorilla/websocket"
)

func TestDuplicateUserConnectionReplacesOldSession(t *testing.T) {
	t.Parallel()

	server, err := NewServer(testConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	waitFor(t, time.Second, server.Ready)

	url := "ws://" + listener.Addr().String() + "/ws?user_id=duplicate_user"
	header := http.Header{"Origin": []string{"http://localhost:3000"}}
	first, _, err := websocket.DefaultDialer.Dial(url, header)
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}
	defer first.Close()
	firstReady := readReadyEvent(t, first)

	second, _, err := websocket.DefaultDialer.Dial(url, header)
	if err != nil {
		t.Fatalf("second dial: %v", err)
	}
	defer second.Close()
	secondReady := readReadyEvent(t, second)

	if firstReady == secondReady {
		t.Fatalf("duplicate sessions received the same connection ID %q", firstReady)
	}
	waitFor(t, time.Second, func() bool { return server.ActiveClients() == 1 })

	if err := first.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set first deadline: %v", err)
	}
	if _, _, err := first.ReadMessage(); err == nil {
		t.Fatal("replaced connection remained readable")
	}
	if server.ActiveClients() != 1 {
		t.Fatalf("old cleanup removed replacement; count = %d", server.ActiveClients())
	}

	command := `{"version":"1","type":"room.join","request_id":"req_replacement","room_id":"payments"}`
	if err := second.WriteMessage(websocket.TextMessage, []byte(command)); err != nil {
		t.Fatalf("write through replacement: %v", err)
	}
	var acknowledgement realtime.OutboundMessage
	if err := second.ReadJSON(&acknowledgement); err != nil {
		t.Fatalf("replacement did not remain usable: %v", err)
	}
	if acknowledgement.Type != realtime.EventCommandAck ||
		acknowledgement.RequestID != "req_replacement" {
		t.Fatalf("unexpected replacement acknowledgement: %+v", acknowledgement)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned an error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not stop")
	}
	if server.ActiveClients() != 0 {
		t.Fatalf("shutdown left %d active clients", server.ActiveClients())
	}
}

func readReadyEvent(t *testing.T, connection *websocket.Conn) string {
	t.Helper()

	var event struct {
		Type    realtime.MessageType `json:"type"`
		Payload struct {
			ConnectionID string `json:"connection_id"`
		} `json:"payload"`
	}
	if err := connection.ReadJSON(&event); err != nil {
		t.Fatalf("read connection.ready: %v", err)
	}
	if event.Type != realtime.EventConnectionReady ||
		!strings.HasPrefix(event.Payload.ConnectionID, "conn_") {
		t.Fatalf("unexpected ready event: %+v", event)
	}
	return event.Payload.ConnectionID
}

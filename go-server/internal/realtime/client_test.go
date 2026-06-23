package realtime

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestClientRoutesProtocolResponsesThroughWriteLoop(t *testing.T) {
	t.Parallel()

	closed := make(chan string, 1)
	server := clientTestServer(t, ClientOptions{
		ConnectionID:  "conn_test",
		UserID:        "user_test",
		QueueCapacity: 8,
		ReadTimeout:   time.Second,
		WriteTimeout:  time.Second,
		Logger:        discardLogger(),
		OnClose:       func(reason string) { closed <- reason },
	})

	connection, _, err := websocket.DefaultDialer.Dial(webSocketTestURL(server.URL), nil)
	if err != nil {
		t.Fatalf("dial WebSocket: %v", err)
	}

	var ready OutboundMessage
	if err := connection.ReadJSON(&ready); err != nil {
		t.Fatalf("read ready event: %v", err)
	}
	if ready.Type != EventConnectionReady {
		t.Fatalf("ready type = %q", ready.Type)
	}

	command := `{"version":"1","type":"room.join","request_id":"req_123","room_id":"payments"}`
	if err := connection.WriteMessage(websocket.TextMessage, []byte(command)); err != nil {
		t.Fatalf("write command: %v", err)
	}
	var acknowledgement OutboundMessage
	if err := connection.ReadJSON(&acknowledgement); err != nil {
		t.Fatalf("read acknowledgement: %v", err)
	}
	if acknowledgement.Type != EventCommandAck || acknowledgement.RequestID != "req_123" {
		t.Fatalf("unexpected acknowledgement: %+v", acknowledgement)
	}

	if err := connection.WriteMessage(websocket.TextMessage, []byte(`{"version":`)); err != nil {
		t.Fatalf("write malformed command: %v", err)
	}
	var errorEvent OutboundMessage
	if err := connection.ReadJSON(&errorEvent); err != nil {
		t.Fatalf("read error event: %v", err)
	}
	if errorEvent.Type != EventError {
		t.Fatalf("error event type = %q", errorEvent.Type)
	}
	payload, err := json.Marshal(errorEvent.Payload)
	if err != nil {
		t.Fatalf("marshal error payload: %v", err)
	}
	var protocolPayload ErrorPayload
	if err := json.Unmarshal(payload, &protocolPayload); err != nil {
		t.Fatalf("decode error payload: %v", err)
	}
	if protocolPayload.Code != ErrorInvalidJSON {
		t.Fatalf("error code = %q", protocolPayload.Code)
	}

	if err := connection.Close(); err != nil {
		t.Fatalf("close peer: %v", err)
	}
	select {
	case reason := <-closed:
		if reason != "read_failed" {
			t.Fatalf("close reason = %q", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("session cleanup did not run")
	}
}
func TestClientCloseIsIdempotentAndQueueIsBounded(t *testing.T) {
	t.Parallel()

	var closeCalls atomic.Int32
	client := NewClient(nil, ClientOptions{
		ConnectionID:  "conn_test",
		UserID:        "user_test",
		QueueCapacity: 1,
		Logger:        discardLogger(),
		OnClose: func(string) {
			closeCalls.Add(1)
		},
	})

	if !client.Send(websocket.TextMessage, []byte("first")) {
		t.Fatal("first bounded queue send failed")
	}
	if client.Send(websocket.TextMessage, []byte("second")) {
		t.Fatal("second send unexpectedly fit in a capacity-one queue")
	}
	if client.Send(websocket.TextMessage, []byte("after-close")) {
		t.Fatal("closed client accepted another message")
	}

	client.Close("second_close")
	client.Close("third_close")
	if calls := closeCalls.Load(); calls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", calls)
	}
}

func TestClientReadDeadlineClosesIdleSession(t *testing.T) {
	t.Parallel()

	closed := make(chan string, 1)
	server := clientTestServer(t, ClientOptions{
		ConnectionID:  "conn_timeout",
		UserID:        "user_timeout",
		QueueCapacity: 2,
		ReadTimeout:   30 * time.Millisecond,
		WriteTimeout:  time.Second,
		Logger:        discardLogger(),
		OnClose: func(reason string) {
			closed <- reason
		},
	})

	connection, _, err := websocket.DefaultDialer.Dial(webSocketTestURL(server.URL), nil)
	if err != nil {
		t.Fatalf("dial WebSocket: %v", err)
	}
	defer connection.Close()

	if _, _, err := connection.ReadMessage(); err != nil {
		t.Fatalf("read ready event: %v", err)
	}
	select {
	case reason := <-closed:
		if reason != "read_failed" {
			t.Fatalf("close reason = %q, want read_failed", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("idle session did not close at its read deadline")
	}
}

func clientTestServer(t *testing.T, options ClientOptions) *httptest.Server {
	t.Helper()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		NewClient(connection, options).Run()
	}))
	t.Cleanup(server.Close)
	return server
}

func webSocketTestURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

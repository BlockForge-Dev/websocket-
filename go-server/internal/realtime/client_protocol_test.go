package realtime

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestClientProtocolErrorsAreStructuredAndSessionRemainsUsable(t *testing.T) {
	t.Parallel()

	server := clientTestServer(t, ClientOptions{
		ConnectionID:  "conn_protocol",
		UserID:        "user_protocol",
		QueueCapacity: 16,
		ReadTimeout:   time.Second,
		WriteTimeout:  time.Second,
		Logger:        discardLogger(),
	})

	connection, _, err := websocket.DefaultDialer.Dial(webSocketTestURL(server.URL), nil)
	if err != nil {
		t.Fatalf("dial WebSocket: %v", err)
	}
	defer connection.Close()

	var ready OutboundMessage
	if err := connection.ReadJSON(&ready); err != nil {
		t.Fatalf("read ready event: %v", err)
	}

	tests := []struct {
		name      string
		frameType int
		payload   string
		wantCode  ErrorCode
		requestID string
	}{
		{
			name:      "malformed JSON",
			frameType: websocket.TextMessage,
			payload:   `{"version":`,
			wantCode:  ErrorInvalidJSON,
		},
		{
			name:      "unknown type",
			frameType: websocket.TextMessage,
			payload:   `{"version":"1","type":"room.destroy","request_id":"req_unknown"}`,
			wantCode:  ErrorUnknownMessageType,
			requestID: "req_unknown",
		},
		{
			name:      "unsupported version",
			frameType: websocket.TextMessage,
			payload:   `{"version":"2","type":"room.join","request_id":"req_version","room_id":"payments"}`,
			wantCode:  ErrorUnsupportedVersion,
			requestID: "req_version",
		},
		{
			name:      "binary frame",
			frameType: websocket.BinaryMessage,
			payload:   `binary`,
			wantCode:  ErrorInvalidMessage,
		},
	}

	for _, test := range tests {
		if err := connection.WriteMessage(test.frameType, []byte(test.payload)); err != nil {
			t.Fatalf("%s write: %v", test.name, err)
		}
		var event OutboundMessage
		if err := connection.ReadJSON(&event); err != nil {
			t.Fatalf("%s read: %v", test.name, err)
		}
		if event.Type != EventError || event.RequestID != test.requestID {
			t.Fatalf("%s event = %+v", test.name, event)
		}
		errorPayload := decodeErrorPayload(t, event.Payload)
		if errorPayload.Code != test.wantCode || errorPayload.Retryable {
			t.Fatalf("%s payload = %+v, want code %q and non-retryable", test.name, errorPayload, test.wantCode)
		}
	}

	valid := `{"version":"1","type":"room.leave","request_id":"req_after_errors","room_id":"payments"}`
	if err := connection.WriteMessage(websocket.TextMessage, []byte(valid)); err != nil {
		t.Fatalf("write valid command after errors: %v", err)
	}
	var acknowledgement OutboundMessage
	if err := connection.ReadJSON(&acknowledgement); err != nil {
		t.Fatalf("read acknowledgement after errors: %v", err)
	}
	if acknowledgement.Type != EventCommandAck || acknowledgement.RequestID != "req_after_errors" {
		t.Fatalf("session did not remain usable: %+v", acknowledgement)
	}
}

func TestCommandAcknowledgementPayload(t *testing.T) {
	t.Parallel()

	server := clientTestServer(t, ClientOptions{
		ConnectionID:  "conn_ack",
		UserID:        "user_ack",
		QueueCapacity: 4,
		ReadTimeout:   time.Second,
		WriteTimeout:  time.Second,
		Logger:        discardLogger(),
	})
	connection, _, err := websocket.DefaultDialer.Dial(webSocketTestURL(server.URL), nil)
	if err != nil {
		t.Fatalf("dial WebSocket: %v", err)
	}
	defer connection.Close()

	var ready OutboundMessage
	if err := connection.ReadJSON(&ready); err != nil {
		t.Fatalf("read ready event: %v", err)
	}

	command := `{"version":"1","type":"private.send","request_id":"req_private","recipient_id":"user_2","payload":{"text":"hello"}}`
	if err := connection.WriteMessage(websocket.TextMessage, []byte(command)); err != nil {
		t.Fatalf("write command: %v", err)
	}
	var event OutboundMessage
	if err := connection.ReadJSON(&event); err != nil {
		t.Fatalf("read acknowledgement: %v", err)
	}

	payload, err := json.Marshal(event.Payload)
	if err != nil {
		t.Fatalf("marshal acknowledgement payload: %v", err)
	}
	var acknowledgement Acknowledgement
	if err := json.Unmarshal(payload, &acknowledgement); err != nil {
		t.Fatalf("decode acknowledgement payload: %v", err)
	}
	if acknowledgement.Status != "accepted" || acknowledgement.CommandType != CommandPrivateSend {
		t.Fatalf("acknowledgement = %+v", acknowledgement)
	}
}

func decodeErrorPayload(t *testing.T, value any) ErrorPayload {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal error payload: %v", err)
	}
	var result ErrorPayload
	if err := json.Unmarshal(payload, &result); err != nil {
		t.Fatalf("decode error payload: %v", err)
	}
	return result
}

package realtime

import (
	"encoding/json"
	"testing"
)

func TestInboundMessageValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		message  InboundMessage
		wantCode ErrorCode
	}{
		{
			name: "room join",
			message: InboundMessage{
				Version: ProtocolVersion, Type: CommandRoomJoin,
				RequestID: "req_join", RoomID: "payments",
			},
		},
		{
			name: "room leave",
			message: InboundMessage{
				Version: ProtocolVersion, Type: CommandRoomLeave,
				RequestID: "req_leave", RoomID: "payments",
			},
		},
		{
			name: "room broadcast",
			message: InboundMessage{
				Version: ProtocolVersion, Type: CommandRoomBroadcast,
				RequestID: "req_broadcast", RoomID: "payments",
				Payload: json.RawMessage(`{"text":"hello"}`),
			},
		},
		{
			name: "private send",
			message: InboundMessage{
				Version: ProtocolVersion, Type: CommandPrivateSend,
				RequestID: "req_private", RecipientID: "user_2",
				Payload: json.RawMessage(`{"text":"hello"}`),
			},
		},
		{
			name: "unsupported version",
			message: InboundMessage{
				Version: "2", Type: CommandRoomJoin,
				RequestID: "req_version", RoomID: "payments",
			},
			wantCode: ErrorUnsupportedVersion,
		},
		{
			name: "missing request id",
			message: InboundMessage{
				Version: ProtocolVersion, Type: CommandRoomJoin, RoomID: "payments",
			},
			wantCode: ErrorInvalidMessage,
		},
		{
			name: "unknown type",
			message: InboundMessage{
				Version: ProtocolVersion, Type: "room.destroy",
				RequestID: "req_unknown",
			},
			wantCode: ErrorUnknownMessageType,
		},
		{
			name: "join missing room",
			message: InboundMessage{
				Version: ProtocolVersion, Type: CommandRoomJoin,
				RequestID: "req_join",
			},
			wantCode: ErrorInvalidMessage,
		},
		{
			name: "broadcast missing payload",
			message: InboundMessage{
				Version: ProtocolVersion, Type: CommandRoomBroadcast,
				RequestID: "req_broadcast", RoomID: "payments",
			},
			wantCode: ErrorInvalidMessage,
		},
		{
			name: "private missing recipient",
			message: InboundMessage{
				Version: ProtocolVersion, Type: CommandPrivateSend,
				RequestID: "req_private", Payload: json.RawMessage(`{"text":"hello"}`),
			},
			wantCode: ErrorInvalidMessage,
		},
		{
			name: "payload must be object",
			message: InboundMessage{
				Version: ProtocolVersion, Type: CommandPrivateSend,
				RequestID: "req_private", RecipientID: "user_2",
				Payload: json.RawMessage(`["hello"]`),
			},
			wantCode: ErrorInvalidMessage,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			protocolError := test.message.Validate()
			if test.wantCode == "" {
				if protocolError != nil {
					t.Fatalf("Validate returned error: %v", protocolError)
				}
				return
			}
			if protocolError == nil || protocolError.Code != test.wantCode {
				t.Fatalf("error = %v, want code %q", protocolError, test.wantCode)
			}
		})
	}
}

func TestDecodeInboundRejectsMalformedAndMultipleJSONValues(t *testing.T) {
	t.Parallel()

	tests := []string{
		`{"version":`,
		`{"version":"1","type":"room.join","request_id":"req","room_id":"a"} {}`,
	}
	for _, payload := range tests {
		_, protocolError := DecodeInbound([]byte(payload))
		if protocolError == nil || protocolError.Code != ErrorInvalidJSON {
			t.Fatalf("payload %q error = %v, want invalid_json", payload, protocolError)
		}
	}
}

func TestDecodeInboundPreservesRequestCorrelationOnValidationError(t *testing.T) {
	t.Parallel()

	_, protocolError := DecodeInbound([]byte(
		`{"version":"1","type":"room.join","request_id":"req_123"}`,
	))
	if protocolError == nil {
		t.Fatal("expected validation error")
	}
	if protocolError.RequestID != "req_123" {
		t.Fatalf("request ID = %q, want req_123", protocolError.RequestID)
	}
}

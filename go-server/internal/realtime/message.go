package realtime

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
)

const ProtocolVersion = "1"

type MessageType string

const (
	CommandRoomJoin      MessageType = "room.join"
	CommandRoomLeave     MessageType = "room.leave"
	CommandRoomBroadcast MessageType = "room.broadcast"
	CommandPrivateSend   MessageType = "private.send"

	EventConnectionReady MessageType = "connection.ready"
	EventCommandAck      MessageType = "command.ack"
	EventError           MessageType = "error"
	EventServerDraining  MessageType = "server.draining"
)

type ErrorCode string

const (
	ErrorInvalidJSON         ErrorCode = "invalid_json"
	ErrorInvalidMessage      ErrorCode = "invalid_message"
	ErrorUnsupportedVersion  ErrorCode = "unsupported_version"
	ErrorUnknownMessageType  ErrorCode = "unknown_message_type"
	ErrorUnauthorized        ErrorCode = "unauthorized"
	ErrorRecipientOffline    ErrorCode = "recipient_offline"
	ErrorRecipientOverloaded ErrorCode = "recipient_overloaded"
	ErrorRateLimitExceeded   ErrorCode = "rate_limit_exceeded"
)

// InboundMessage is the version-one command envelope sent by a client.
type InboundMessage struct {
	Version     string          `json:"version"`
	Type        MessageType     `json:"type"`
	RequestID   string          `json:"request_id"`
	RoomID      string          `json:"room_id,omitempty"`
	RecipientID string          `json:"recipient_id,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	SentAt      *time.Time      `json:"sent_at,omitempty"`
}

// OutboundMessage is the shared envelope for server acknowledgements, errors,
// and events.
type OutboundMessage struct {
	Version   string      `json:"version"`
	Type      MessageType `json:"type"`
	RequestID string      `json:"request_id,omitempty"`
	EventID   string      `json:"event_id"`
	Payload   any         `json:"payload"`
	SentAt    time.Time   `json:"sent_at"`
}

// Acknowledgement states which command passed protocol validation.
type Acknowledgement struct {
	Status      string      `json:"status"`
	CommandType MessageType `json:"command_type"`
}

// ErrorPayload is safe for clients and stable enough for programmatic use.
type ErrorPayload struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	Retryable bool      `json:"retryable"`
}

// ProtocolError classifies a client-visible protocol failure.
type ProtocolError struct {
	Code      ErrorCode
	Message   string
	RequestID string
}

func (e *ProtocolError) Error() string {
	return string(e.Code) + ": " + e.Message
}

// DecodeInbound parses and validates one complete text-frame payload.
func DecodeInbound(payload []byte) (InboundMessage, *ProtocolError) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return InboundMessage{}, &ProtocolError{
			Code:    ErrorInvalidJSON,
			Message: "The message must contain one valid JSON object.",
		}
	}

	var message InboundMessage
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := decoder.Decode(&message); err != nil {
		return InboundMessage{}, &ProtocolError{
			Code:    ErrorInvalidJSON,
			Message: "The message must contain one valid JSON object.",
		}
	}

	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return InboundMessage{}, &ProtocolError{
			Code:      ErrorInvalidJSON,
			Message:   "The frame must contain exactly one JSON object.",
			RequestID: message.RequestID,
		}
	}

	if protocolError := message.Validate(); protocolError != nil {
		return InboundMessage{}, protocolError
	}
	return message, nil
}

// Validate applies message-type-specific version-one rules.
func (m InboundMessage) Validate() *ProtocolError {
	requestID := strings.TrimSpace(m.RequestID)
	if strings.TrimSpace(m.Version) != ProtocolVersion {
		return &ProtocolError{
			Code:      ErrorUnsupportedVersion,
			Message:   "Only protocol version 1 is supported.",
			RequestID: requestID,
		}
	}
	if requestID == "" {
		return &ProtocolError{
			Code:    ErrorInvalidMessage,
			Message: "request_id is required.",
		}
	}
	if len(requestID) > 128 {
		return &ProtocolError{
			Code:      ErrorInvalidMessage,
			Message:   "request_id must not exceed 128 characters.",
			RequestID: requestID,
		}
	}

	switch m.Type {
	case CommandRoomJoin:
		if strings.TrimSpace(m.RoomID) == "" {
			return invalidField(requestID, "room_id is required for room.join.")
		}
	case CommandRoomLeave:
		if strings.TrimSpace(m.RoomID) == "" {
			return invalidField(requestID, "room_id is required for room.leave.")
		}
	case CommandRoomBroadcast:
		if strings.TrimSpace(m.RoomID) == "" {
			return invalidField(requestID, "room_id is required for room.broadcast.")
		}
		if !validPayload(m.Payload) {
			return invalidField(requestID, "payload must be a JSON object for room.broadcast.")
		}
	case CommandPrivateSend:
		if strings.TrimSpace(m.RecipientID) == "" {
			return invalidField(requestID, "recipient_id is required for private.send.")
		}
		if !validPayload(m.Payload) {
			return invalidField(requestID, "payload must be a JSON object for private.send.")
		}
	default:
		return &ProtocolError{
			Code:      ErrorUnknownMessageType,
			Message:   "The message type is not supported.",
			RequestID: requestID,
		}
	}

	if len(m.RoomID) > 256 || len(m.RecipientID) > 256 {
		return invalidField(requestID, "routing identifiers must not exceed 256 characters.")
	}
	return nil
}

func validPayload(payload json.RawMessage) bool {
	if len(payload) == 0 {
		return false
	}
	var object map[string]any
	return json.Unmarshal(payload, &object) == nil && object != nil
}

func invalidField(requestID, message string) *ProtocolError {
	return &ProtocolError{
		Code:      ErrorInvalidMessage,
		Message:   message,
		RequestID: requestID,
	}
}

func newOutboundMessage(messageType MessageType, requestID string, payload any) (OutboundMessage, error) {
	eventID, err := newEventID()
	if err != nil {
		return OutboundMessage{}, err
	}
	return OutboundMessage{
		Version:   ProtocolVersion,
		Type:      messageType,
		RequestID: requestID,
		EventID:   eventID,
		Payload:   payload,
		SentAt:    time.Now().UTC(),
	}, nil
}

func newEventID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "evt_" + hex.EncodeToString(value[:]), nil
}

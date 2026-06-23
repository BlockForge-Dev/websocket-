package broker

import "encoding/json"

// BrokerEvent is the wire envelope carried over the broker between nodes.
// NodeID identifies the origin node so receivers can skip local echo.
type BrokerEvent struct {
	NodeID      string          `json:"node_id"`
	Type        string          `json:"type"`
	RoomID      string          `json:"room_id,omitempty"`
	SenderID    string          `json:"sender_id"`
	RecipientID string          `json:"recipient_id,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}

// Encode serializes a BrokerEvent to JSON bytes for publishing.
func (e BrokerEvent) Encode() ([]byte, error) {
	return json.Marshal(e)
}

// DecodeBrokerEvent deserializes a BrokerEvent from broker message data.
func DecodeBrokerEvent(data []byte) (BrokerEvent, error) {
	var event BrokerEvent
	err := json.Unmarshal(data, &event)
	return event, err
}

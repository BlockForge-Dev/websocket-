package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"

	"github.com/blockforgelabs/go-websocket/internal/broker"
	"github.com/blockforgelabs/go-websocket/internal/observability"
)

// Hub is the sole owner of the active client registry and room state.
type Hub struct {
	mu                sync.RWMutex
	clients           map[string]*Client
	rooms             map[string]map[string]*Client  // roomID -> connectionID -> Client
	clientRooms       map[string]map[string]struct{} // connectionID -> roomID -> struct{}
	authorizer        Authorizer
	logger            *slog.Logger
	maxRoomsPerClient int
	broker            broker.Broker
	nodeID            string
}

// NewHub creates an empty process-local client registry.
func NewHub(logger *slog.Logger, authorizer Authorizer, maxRoomsPerClient int) *Hub {
	return &Hub{
		clients:           make(map[string]*Client),
		rooms:             make(map[string]map[string]*Client),
		clientRooms:       make(map[string]map[string]struct{}),
		authorizer:        authorizer,
		logger:            logger,
		maxRoomsPerClient: maxRoomsPerClient,
	}
}

// Register installs client as the active session for its user. If another
// session already exists, the new session wins and the old session is closed
// after the registry lock is released.
func (h *Hub) Register(client *Client) {
	h.mu.Lock()
	replaced := h.clients[client.UserID()]
	h.clients[client.UserID()] = client
	count := len(h.clients)
	h.mu.Unlock()

	h.logger.Info(
		"websocket_client_registered",
		"connection_id", client.ConnectionID(),
		"user_id", client.UserID(),
		"active_clients", count,
		"replaced_connection", replaced != nil,
	)

	if replaced != nil && replaced != client {
		replaced.Close("duplicate_session_replaced")
	}
}

// Unregister removes client only if it is still the active session for its
// user, but always cleans up its room memberships. Cleanup from a replaced
// session therefore cannot delete its successor.
func (h *Hub) Unregister(client *Client) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Clean up room memberships for this client session, regardless of whether it's the active one.
	connID := client.ConnectionID()
	if roomsJoined, exists := h.clientRooms[connID]; exists {
		for roomID := range roomsJoined {
			if clients, ok := h.rooms[roomID]; ok {
				delete(clients, connID)
				if len(clients) == 0 {
					delete(h.rooms, roomID)
				}
			}
		}
		delete(h.clientRooms, connID)
	}

	current, ok := h.clients[client.UserID()]
	if !ok || current != client || current.ConnectionID() != client.ConnectionID() {
		return false
	}
	delete(h.clients, client.UserID())
	count := len(h.clients)

	h.logger.Info(
		"websocket_client_unregistered",
		"connection_id", client.ConnectionID(),
		"user_id", client.UserID(),
		"active_clients", count,
	)
	return true
}

// Join adds client to roomID if the user is authorized.
func (h *Hub) Join(client *Client, roomID string) error {
	if err := h.authorizer.AuthorizeJoin(client.UserID(), roomID); err != nil {
		return err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	connID := client.ConnectionID()
	if rooms, ok := h.clientRooms[connID]; ok {
		if _, joined := rooms[roomID]; !joined && h.maxRoomsPerClient > 0 && len(rooms) >= h.maxRoomsPerClient {
			return errors.New("maximum room membership limit reached")
		}
	}

	if _, ok := h.rooms[roomID]; !ok {
		h.rooms[roomID] = make(map[string]*Client)
	}
	h.rooms[roomID][connID] = client

	if _, ok := h.clientRooms[connID]; !ok {
		h.clientRooms[connID] = make(map[string]struct{})
	}
	h.clientRooms[connID][roomID] = struct{}{}

	h.logger.Info(
		"websocket_room_joined",
		"connection_id", connID,
		"user_id", client.UserID(),
		"room_id", roomID,
	)
	return nil
}

// Leave removes client from roomID.
func (h *Hub) Leave(client *Client, roomID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	connID := client.ConnectionID()
	if rooms, ok := h.clientRooms[connID]; ok {
		delete(rooms, roomID)
		if len(rooms) == 0 {
			delete(h.clientRooms, connID)
		}
	}

	if clients, ok := h.rooms[roomID]; ok {
		delete(clients, connID)
		if len(clients) == 0 {
			delete(h.rooms, roomID)
		}
	}

	h.logger.Info(
		"websocket_room_left",
		"connection_id", connID,
		"user_id", client.UserID(),
		"room_id", roomID,
	)
}

// AuthorizeBroadcast returns nil if client is authorized to publish to roomID.
// This is true if client is a member of the room or has publishing permission.
func (h *Hub) AuthorizeBroadcast(client *Client, roomID string) error {
	h.mu.RLock()
	clientsMap, roomExists := h.rooms[roomID]
	isMember := false
	if roomExists {
		_, isMember = clientsMap[client.ConnectionID()]
	}
	h.mu.RUnlock()

	if isMember {
		return nil
	}

	return h.authorizer.AuthorizePublish(client.UserID(), roomID)
}

// SetBroker associates a message broker and unique node ID with this hub.
func (h *Hub) SetBroker(b broker.Broker, nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.broker = b
	h.nodeID = nodeID
}

// Broadcast distributes payload to all active members of roomID except senderID.
func (h *Hub) Broadcast(senderID string, roomID string, payload json.RawMessage) {
	h.localBroadcast(senderID, roomID, payload)

	h.mu.RLock()
	br := h.broker
	nid := h.nodeID
	h.mu.RUnlock()

	if br != nil {
		event := broker.BrokerEvent{
			NodeID:   nid,
			Type:     "room_broadcast",
			RoomID:   roomID,
			SenderID: senderID,
			Payload:  payload,
		}
		data, err := event.Encode()
		if err != nil {
			h.logger.Error("websocket_broker_encode_failed", "error", err)
			observability.IncrementBrokerPublishErrors()
			return
		}
		subject := "blockforge.rooms." + roomID
		if err := br.Publish(context.TODO(), subject, data); err != nil {
			h.logger.Error("websocket_broker_publish_failed", "subject", subject, "error", err)
			observability.IncrementBrokerPublishErrors()
		} else {
			observability.IncrementBrokerPublishCount()
		}
	}
}

// localBroadcast distributes payload to local members of roomID except senderID.
func (h *Hub) localBroadcast(senderID string, roomID string, payload json.RawMessage) {
	h.mu.RLock()
	clientsMap, roomExists := h.rooms[roomID]
	if !roomExists || len(clientsMap) == 0 {
		h.mu.RUnlock()
		return
	}

	// Snapshot recipients to avoid holding lock during socket writes / enqueues
	recipients := make([]*Client, 0, len(clientsMap))
	for _, client := range clientsMap {
		if client.UserID() == senderID {
			continue // Exclude the sender
		}
		recipients = append(recipients, client)
	}
	h.mu.RUnlock()

	// Prepare outbound message
	event, err := newOutboundMessage(CommandRoomBroadcast, "", struct {
		RoomID   string          `json:"room_id"`
		SenderID string          `json:"sender_id"`
		Payload  json.RawMessage `json:"payload"`
	}{
		RoomID:   roomID,
		SenderID: senderID,
		Payload:  payload,
	})
	if err != nil {
		h.logger.Error("websocket_broadcast_event_failed", "room_id", roomID, "error", err)
		return
	}

	// Enqueue to recipients
	for _, client := range recipients {
		if !client.SendJSON(event) {
			h.logger.Warn(
				"websocket_broadcast_recipient_unavailable",
				"room_id", roomID,
				"recipient_user_id", client.UserID(),
				"recipient_connection_id", client.ConnectionID(),
				"queue_depth", client.QueueDepth(),
			)
		}
	}
}

// Lookup returns the current active session for a user.
func (h *Hub) Lookup(userID string) (*Client, bool) {
	h.mu.RLock()
	client, ok := h.clients[userID]
	h.mu.RUnlock()
	return client, ok
}

// Count returns the number of active user sessions.
func (h *Hub) Count() int {
	h.mu.RLock()
	count := len(h.clients)
	h.mu.RUnlock()
	return count
}

// Snapshot returns the current sessions without exposing the registry map.
func (h *Hub) Snapshot() []*Client {
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for _, client := range h.clients {
		clients = append(clients, client)
	}
	h.mu.RUnlock()
	return clients
}

// CloseAll enumerates a stable snapshot and closes every active session.
func (h *Hub) CloseAll(reason string) {
	for _, client := range h.Snapshot() {
		client.Close(reason)
	}
}

// BroadcastDraining sends a server.draining event to all active client sessions.
func (h *Hub) BroadcastDraining() {
	event, err := newOutboundMessage(EventServerDraining, "", struct {
		Message string `json:"message"`
	}{
		Message: "Server is shutting down. Please reconnect.",
	})
	if err != nil {
		h.logger.Error("websocket_draining_event_failed", "error", err)
		return
	}
	for _, client := range h.Snapshot() {
		client.SendJSON(event)
	}
}

// ErrRecipientOffline is returned when recipient is not registered in the Hub.
var ErrRecipientOffline = errors.New("recipient is offline")

// ErrRecipientOverloaded is returned when recipient's queue is full.
var ErrRecipientOverloaded = errors.New("recipient queue is full")

// SendPrivate routes a private message from senderID to recipientID.
func (h *Hub) SendPrivate(senderID, recipientID string, payload json.RawMessage) error {
	h.mu.RLock()
	recipient, ok := h.clients[recipientID]
	br := h.broker
	nid := h.nodeID
	h.mu.RUnlock()

	var localErr error
	if ok {
		event, err := newOutboundMessage(CommandPrivateSend, "", struct {
			SenderID    string          `json:"sender_id"`
			RecipientID string          `json:"recipient_id"`
			Payload     json.RawMessage `json:"payload"`
		}{
			SenderID:    senderID,
			RecipientID: recipientID,
			Payload:     payload,
		})
		if err != nil {
			return err
		}

		if !recipient.SendJSON(event) {
			h.logger.Warn(
				"websocket_private_recipient_overloaded",
				"sender_user_id", senderID,
				"recipient_user_id", recipientID,
				"recipient_connection_id", recipient.ConnectionID(),
				"queue_depth", recipient.QueueDepth(),
			)
			localErr = ErrRecipientOverloaded
		}
	}

	if br != nil {
		event := broker.BrokerEvent{
			NodeID:      nid,
			Type:        "private_send",
			RecipientID: recipientID,
			SenderID:    senderID,
			Payload:     payload,
		}
		data, err := event.Encode()
		if err != nil {
			h.logger.Error("websocket_broker_encode_failed", "error", err)
			observability.IncrementBrokerPublishErrors()
			if ok {
				return localErr
			}
			return err
		}
		subject := "blockforge.users." + recipientID
		if err := br.Publish(context.TODO(), subject, data); err != nil {
			h.logger.Error("websocket_broker_publish_failed", "subject", subject, "error", err)
			observability.IncrementBrokerPublishErrors()
			if ok {
				return localErr
			}
			return err
		}
		observability.IncrementBrokerPublishCount()
		return nil
	}

	if ok {
		return localErr
	}
	return ErrRecipientOffline
}

// localSendPrivate distributes payload to local recipientID.
func (h *Hub) localSendPrivate(senderID, recipientID string, payload json.RawMessage) {
	h.mu.RLock()
	recipient, ok := h.clients[recipientID]
	h.mu.RUnlock()

	if !ok {
		return
	}

	event, err := newOutboundMessage(CommandPrivateSend, "", struct {
		SenderID    string          `json:"sender_id"`
		RecipientID string          `json:"recipient_id"`
		Payload     json.RawMessage `json:"payload"`
	}{
		SenderID:    senderID,
		RecipientID: recipientID,
		Payload:     payload,
	})
	if err != nil {
		h.logger.Error("websocket_private_event_failed", "error", err)
		return
	}

	if !recipient.SendJSON(event) {
		h.logger.Warn(
			"websocket_private_recipient_overloaded_broker",
			"sender_user_id", senderID,
			"recipient_user_id", recipientID,
			"recipient_connection_id", recipient.ConnectionID(),
			"queue_depth", recipient.QueueDepth(),
		)
	}
}

// HandleBrokerEvent processes an inbound event from the broker.
func (h *Hub) HandleBrokerEvent(event broker.BrokerEvent) {
	h.mu.RLock()
	nid := h.nodeID
	h.mu.RUnlock()

	if nid != "" && event.NodeID == nid {
		return
	}

	observability.IncrementBrokerReceiveCount()

	switch event.Type {
	case "room_broadcast":
		h.localBroadcast(event.SenderID, event.RoomID, event.Payload)
	case "private_send":
		h.localSendPrivate(event.SenderID, event.RecipientID, event.Payload)
	default:
		h.logger.Warn("websocket_broker_event_unknown_type", "type", event.Type)
	}
}

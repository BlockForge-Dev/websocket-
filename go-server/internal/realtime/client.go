package realtime

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/blockforgelabs/go-websocket/internal/observability"
)

// ClientOptions defines one WebSocket session's identity and resource limits.
type ClientOptions struct {
	ConnectionID   string
	UserID         string
	QueueCapacity  int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	Logger         *slog.Logger
	OnClose        func(reason string)
	Hub            *Hub
	PingInterval   time.Duration
	MaxMessageSize int64
	RateLimiter    *RateLimiter
}

type outboundMessage struct {
	messageType int
	payload     []byte
}

type socket interface {
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	ReadMessage() (int, []byte, error)
	WriteMessage(int, []byte) error
	Close() error
	SetPongHandler(func(string) error)
	SetReadLimit(int64)
}

// Client owns one upgraded WebSocket connection.
//
// The read loop is the only normal socket reader and the write loop is the
// only normal socket writer. Other components deliver outbound work through
// Send.
type Client struct {
	connection socket

	connectionID   string
	userID         string
	readTimeout    time.Duration
	writeTimeout   time.Duration
	logger         *slog.Logger
	onClose        func(string)
	hub            *Hub
	pingInterval   time.Duration
	maxMessageSize int64
	limiter        *RateLimiter

	send      chan outboundMessage
	done      chan struct{}
	stateMu   sync.Mutex
	closed    bool
	closeOnce sync.Once
}

// NewClient constructs a session with a bounded outbound queue.
func NewClient(connection *websocket.Conn, options ClientOptions) *Client {
	if connection == nil {
		return newClient(nil, options)
	}
	return newClient(connection, options)
}

func newClient(connection socket, options ClientOptions) *Client {
	c := &Client{
		connection:     connection,
		connectionID:   options.ConnectionID,
		userID:         options.UserID,
		readTimeout:    options.ReadTimeout,
		writeTimeout:   options.WriteTimeout,
		logger:         options.Logger,
		onClose:        options.OnClose,
		hub:            options.Hub,
		pingInterval:   options.PingInterval,
		maxMessageSize: options.MaxMessageSize,
		limiter:        options.RateLimiter,
		send:           make(chan outboundMessage, options.QueueCapacity),
		done:           make(chan struct{}),
	}
	if c.connection != nil && options.MaxMessageSize > 0 {
		c.connection.SetReadLimit(options.MaxMessageSize)
	}
	observability.IncrementActiveConnections()
	return c
}

// ConnectionID returns the immutable session identifier.
func (c *Client) ConnectionID() string { return c.connectionID }

// UserID returns the authenticated identity that owns the session.
func (c *Client) UserID() string { return c.userID }

// Run starts the write loop, sends connection.ready through the queue, and
// then owns the read loop until either side fails.
func (c *Client) Run() {
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		c.writeLoop()
	}()

	readyEvent, err := c.readyEvent()
	if err != nil {
		c.Close("event_id_failed")
		<-writerDone
		return
	}
	if !c.SendJSON(readyEvent) {
		c.Close("ready_queue_unavailable")
		<-writerDone
		return
	}

	reason := c.readLoop()
	c.Close(reason)
	<-writerDone
}

// Send enqueues one outbound frame without performing socket I/O. It returns
// false when the session is closed or the bounded queue is full.
func (c *Client) Send(messageType int, payload []byte) bool {
	message := outboundMessage{
		messageType: messageType,
		payload:     append([]byte(nil), payload...),
	}

	c.stateMu.Lock()
	if c.closed {
		c.stateMu.Unlock()
		return false
	}

	select {
	case c.send <- message:
		observability.UpdateMaxQueueDepth(int64(len(c.send)))
		c.stateMu.Unlock()
		return true
	default:
		c.stateMu.Unlock()
		c.logger.Warn(
			"websocket_queue_full",
			"connection_id", c.connectionID,
			"user_id", c.userID,
			"queue_depth", len(c.send),
			"queue_capacity", cap(c.send),
		)
		c.Close("outbound_queue_full")
		return false
	}
}

// SendJSON serializes a server event and enqueues it for the write loop.
func (c *Client) SendJSON(value any) bool {
	payload, err := json.Marshal(value)
	if err != nil {
		c.logger.Error(
			"websocket_message_encode_failed",
			"connection_id", c.connectionID,
			"user_id", c.userID,
			"error", err,
		)
		c.Close("message_encode_failed")
		return false
	}
	return c.Send(websocket.TextMessage, payload)
}

// QueueDepth returns the number of pending messages in the outbound queue.
func (c *Client) QueueDepth() int {
	return len(c.send)
}

// Close is safe to call from multiple failure paths.
func (c *Client) Close(reason string) {
	c.closeOnce.Do(func() {
		c.stateMu.Lock()
		c.closed = true
		close(c.done)
		c.stateMu.Unlock()
		if c.connection != nil {
			_ = c.connection.Close()
		}
		if c.onClose != nil {
			c.onClose(reason)
		}
		c.logger.Info(
			"websocket_session_closed",
			"connection_id", c.connectionID,
			"user_id", c.userID,
			"reason", reason,
		)
		observability.DecrementActiveConnections()
		if reason == "heartbeat_timeout" {
			observability.IncrementHeartbeatTimeouts()
		} else if reason == "outbound_queue_full" {
			observability.IncrementQueueFullDisconnects()
		}
	})
}

func (c *Client) readLoop() string {
	if c.readTimeout > 0 {
		c.connection.SetPongHandler(func(string) error {
			_ = c.connection.SetReadDeadline(time.Now().Add(c.readTimeout))
			return nil
		})
		if err := c.connection.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
			return "read_deadline_failed"
		}
	}

	for {
		messageType, payload, err := c.connection.ReadMessage()
		if err != nil {
			var closeErr *websocket.CloseError
			if errors.As(err, &closeErr) && closeErr.Code == websocket.CloseMessageTooBig {
				return "oversized_message"
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				if c.pingInterval > 0 {
					return "heartbeat_timeout"
				}
			}
			return "read_failed"
		}
		observability.IncrementMessagesReceived()

		var rateLimitExceeded bool
		if c.limiter != nil && !c.limiter.Allow() {
			rateLimitExceeded = true
		}

		if c.readTimeout > 0 {
			if err := c.connection.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
				return "read_deadline_failed"
			}
		}

		exitReason := func() string {
			startTime := time.Now()
			var msgType string = "unknown"
			var resultSuccess bool = false

			defer func() {
				observability.RecordProcessingLatency(time.Since(startTime))
				observability.IncrementInboundType(msgType)
				observability.IncrementInboundResult(resultSuccess)
			}()

			if messageType != websocket.TextMessage {
				if !c.sendProtocolError(&ProtocolError{
					Code:    ErrorInvalidMessage,
					Message: "Only JSON text frames are supported.",
				}) {
					return "outbound_unavailable"
				}
				return ""
			}

			message, protocolError := DecodeInbound(payload)
			if message.Type != "" {
				msgType = string(message.Type)
			}

			if rateLimitExceeded {
				var reqID string
				if protocolError == nil {
					reqID = message.RequestID
				} else {
					reqID = protocolError.RequestID
				}
				if !c.sendError(reqID, ErrorRateLimitExceeded, "Rate limit exceeded.") {
					return "outbound_unavailable"
				}
				return ""
			}

			if protocolError != nil {
				if !c.sendProtocolError(protocolError) {
					return "outbound_unavailable"
				}
				return ""
			}

			switch message.Type {
			case CommandRoomJoin:
				if c.hub != nil {
					if err := c.hub.Join(c, message.RoomID); err != nil {
						c.logger.Warn("websocket_command_rejected",
							"connection_id", c.connectionID,
							"user_id", c.userID,
							"request_id", message.RequestID,
							"command_type", string(message.Type),
							"reason", err.Error(),
						)
						if !c.sendError(message.RequestID, ErrorUnauthorized, err.Error()) {
							return "outbound_unavailable"
						}
						return ""
					}
				}
				resultSuccess = true
				if !c.sendAcknowledgement(message) {
					return "outbound_unavailable"
				}
			case CommandRoomLeave:
				if c.hub != nil {
					c.hub.Leave(c, message.RoomID)
				}
				resultSuccess = true
				if !c.sendAcknowledgement(message) {
					return "outbound_unavailable"
				}
			case CommandRoomBroadcast:
				if c.hub != nil {
					if err := c.hub.AuthorizeBroadcast(c, message.RoomID); err != nil {
						c.logger.Warn("websocket_command_rejected",
							"connection_id", c.connectionID,
							"user_id", c.userID,
							"request_id", message.RequestID,
							"command_type", string(message.Type),
							"reason", err.Error(),
						)
						if !c.sendError(message.RequestID, ErrorUnauthorized, err.Error()) {
							return "outbound_unavailable"
						}
						return ""
					}
					c.hub.Broadcast(c.UserID(), message.RoomID, message.Payload)
				}
				resultSuccess = true
				if !c.sendAcknowledgement(message) {
					return "outbound_unavailable"
				}
			case CommandPrivateSend:
				if c.hub != nil {
					if err := c.hub.authorizer.AuthorizePrivateSend(c.UserID(), message.RecipientID); err != nil {
						c.logger.Warn("websocket_command_rejected",
							"connection_id", c.connectionID,
							"user_id", c.userID,
							"request_id", message.RequestID,
							"command_type", string(message.Type),
							"reason", err.Error(),
						)
						if !c.sendError(message.RequestID, ErrorUnauthorized, err.Error()) {
							return "outbound_unavailable"
						}
						return ""
					}

					err := c.hub.SendPrivate(c.UserID(), message.RecipientID, message.Payload)
					if err != nil {
						var code ErrorCode = ErrorRecipientOffline
						if errors.Is(err, ErrRecipientOverloaded) {
							code = ErrorRecipientOverloaded
						}
						c.logger.Warn("websocket_command_failed",
							"connection_id", c.connectionID,
							"user_id", c.userID,
							"request_id", message.RequestID,
							"command_type", string(message.Type),
							"reason", err.Error(),
						)
						if !c.sendError(message.RequestID, code, err.Error()) {
							return "outbound_unavailable"
						}
						return ""
					}
				}
				resultSuccess = true
				if !c.sendAcknowledgement(message) {
					return "outbound_unavailable"
				}
			default:
				resultSuccess = true
				if !c.sendAcknowledgement(message) {
					return "outbound_unavailable"
				}
			}
			return ""
		}()

		if exitReason != "" {
			return exitReason
		}
	}
}

func (c *Client) writeLoop() {
	var pingTicker *time.Ticker
	var pingChan <-chan time.Time
	if c.pingInterval > 0 {
		pingTicker = time.NewTicker(c.pingInterval)
		defer pingTicker.Stop()
		pingChan = pingTicker.C
	}

	for {
		select {
		case <-c.done:
			return
		default:
		}

		select {
		case <-c.done:
			return
		case message := <-c.send:
			if c.writeTimeout > 0 {
				if err := c.connection.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
					c.Close("write_deadline_failed")
					return
				}
			}
			if err := c.connection.WriteMessage(message.messageType, message.payload); err != nil {
				c.Close("write_failed")
				return
			}
			observability.IncrementMessagesSent()
		case <-pingChan:
			if c.writeTimeout > 0 {
				if err := c.connection.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
					c.Close("write_deadline_failed")
					return
				}
			}
			if err := c.connection.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.Close("write_failed")
				return
			}
		}
	}
}

func (c *Client) sendAcknowledgement(message InboundMessage) bool {
	event, err := newOutboundMessage(EventCommandAck, message.RequestID, Acknowledgement{
		Status:      "accepted",
		CommandType: message.Type,
	})
	if err != nil {
		c.logger.Error("websocket_event_id_failed", "error", err)
		c.Close("event_id_failed")
		return false
	}
	return c.SendJSON(event)
}

func (c *Client) sendProtocolError(protocolError *ProtocolError) bool {
	event, err := newOutboundMessage(EventError, protocolError.RequestID, ErrorPayload{
		Code:      protocolError.Code,
		Message:   protocolError.Message,
		Retryable: false,
	})
	if err != nil {
		c.logger.Error("websocket_event_id_failed", "error", err)
		c.Close("event_id_failed")
		return false
	}
	return c.SendJSON(event)
}

func (c *Client) sendError(requestID string, code ErrorCode, message string) bool {
	event, err := newOutboundMessage(EventError, requestID, ErrorPayload{
		Code:      code,
		Message:   message,
		Retryable: false,
	})
	if err != nil {
		c.logger.Error("websocket_event_id_failed", "error", err)
		c.Close("event_id_failed")
		return false
	}
	return c.SendJSON(event)
}

func (c *Client) readyEvent() (OutboundMessage, error) {
	return newOutboundMessage(EventConnectionReady, "", struct {
		ConnectionID string `json:"connection_id"`
		UserID       string `json:"user_id"`
	}{
		ConnectionID: c.connectionID,
		UserID:       c.userID,
	})
}

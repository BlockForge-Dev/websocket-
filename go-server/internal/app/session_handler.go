package app

import (
	"log/slog"

	"github.com/blockforgelabs/go-websocket/internal/config"
	"github.com/blockforgelabs/go-websocket/internal/httpapi"
	"github.com/blockforgelabs/go-websocket/internal/realtime"
	"github.com/gorilla/websocket"
)

// realtimeSessionHandler adapts the HTTP handoff contract to the realtime
// client-session owner without moving socket lifecycle into the HTTP package.
type realtimeSessionHandler struct {
	config config.Config
	logger *slog.Logger
	hub    *realtime.Hub
}

func (h realtimeSessionHandler) Handle(connection *websocket.Conn, session httpapi.Session) {
	var limiter *realtime.RateLimiter
	if h.config.RateLimitMessages > 0 && h.config.RateLimitInterval > 0 {
		limiter = realtime.NewRateLimiter(h.config.RateLimitMessages, h.config.RateLimitInterval)
	}

	var client *realtime.Client
	client = realtime.NewClient(connection, realtime.ClientOptions{
		ConnectionID:  session.ConnectionID,
		UserID:        session.UserID,
		QueueCapacity: h.config.OutboundQueueCapacity,
		ReadTimeout:   h.config.WebSocketReadTimeout,
		WriteTimeout:  h.config.WebSocketWriteTimeout,
		Logger:        h.logger,
		OnClose: func(string) {
			h.hub.Unregister(client)
		},
		Hub:            h.hub,
		PingInterval:   h.config.WebSocketReadTimeout * 9 / 10,
		MaxMessageSize: h.config.WebSocketMaxMessageSize,
		RateLimiter:    limiter,
	})
	h.hub.Register(client)
	client.Run()
}

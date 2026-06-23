package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/blockforgelabs/go-websocket/internal/observability"
)

var (
	// ErrMissingIdentity indicates that authentication credentials were absent.
	ErrMissingIdentity = errors.New("identity is required")
	// ErrProductionAuthenticatorUnavailable keeps production closed until a
	// verified token or session implementation is injected.
	ErrProductionAuthenticatorUnavailable = errors.New("production authenticator is not configured")
)

// Principal is the identity established before a connection is upgraded.
type Principal struct {
	UserID string
}

// Authenticator is the production seam for verified connection identity.
type Authenticator interface {
	Authenticate(*http.Request) (Principal, error)
}

// DevelopmentAuthenticator accepts a non-empty user_id query parameter. It is
// intentionally unavailable in production.
type DevelopmentAuthenticator struct{}

// Authenticate establishes a development-only identity.
func (DevelopmentAuthenticator) Authenticate(request *http.Request) (Principal, error) {
	userID := strings.TrimSpace(request.URL.Query().Get("user_id"))
	if userID == "" {
		return Principal{}, ErrMissingIdentity
	}
	return Principal{UserID: userID}, nil
}

// RejectingAuthenticator fails closed until a production verifier is injected.
type RejectingAuthenticator struct{}

// Authenticate rejects every request.
func (RejectingAuthenticator) Authenticate(*http.Request) (Principal, error) {
	return Principal{}, ErrProductionAuthenticatorUnavailable
}

// Session identifies one accepted WebSocket connection.
type Session struct {
	ConnectionID string
	UserID       string
}

// SessionHandler owns an upgraded connection after the HTTP boundary hands it
// off. The production implementation creates the realtime client session.
type SessionHandler interface {
	Handle(*websocket.Conn, Session)
}

// WebSocketOptions configures connection establishment.
type WebSocketOptions struct {
	Health         *Health
	Authenticator  Authenticator
	SessionHandler SessionHandler
	AllowedOrigins []string
	MaxConnections int
	Logger         *slog.Logger
}

type webSocketHandler struct {
	health         *Health
	authenticator  Authenticator
	sessionHandler SessionHandler
	allowedOrigins map[string]struct{}
	admission      chan struct{}
	logger         *slog.Logger
	upgrader       websocket.Upgrader
}

// NewWebSocketHandler creates the validated HTTP-to-WebSocket boundary.
func NewWebSocketHandler(options WebSocketOptions) http.Handler {
	origins := make(map[string]struct{}, len(options.AllowedOrigins))
	for _, origin := range options.AllowedOrigins {
		origins[origin] = struct{}{}
	}

	handler := &webSocketHandler{
		health:         options.Health,
		authenticator:  options.Authenticator,
		sessionHandler: options.SessionHandler,
		allowedOrigins: origins,
		admission:      make(chan struct{}, options.MaxConnections),
		logger:         options.Logger,
	}
	handler.upgrader = websocket.Upgrader{
		HandshakeTimeout: 5 * time.Second,
		CheckOrigin:      handler.checkOrigin,
	}
	return handler
}

func (h *webSocketHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	observability.IncrementUpgradesAttempted()

	if !h.health.Ready() {
		h.reject(response, request, http.StatusServiceUnavailable, "not_ready")
		return
	}
	if !websocket.IsWebSocketUpgrade(request) {
		h.reject(response, request, http.StatusUpgradeRequired, "upgrade_required")
		return
	}
	if !h.checkOrigin(request) {
		h.reject(response, request, http.StatusForbidden, "origin_rejected")
		return
	}

	principal, err := h.authenticator.Authenticate(request)
	if err != nil {
		h.reject(response, request, http.StatusUnauthorized, "authentication_rejected")
		return
	}

	select {
	case h.admission <- struct{}{}:
		defer func() { <-h.admission }()
	default:
		h.reject(response, request, http.StatusServiceUnavailable, "connection_limit")
		return
	}

	connection, err := h.upgrader.Upgrade(response, request, nil)
	if err != nil {
		observability.IncrementUpgradesRejected()
		h.logger.Info("websocket_upgrade_rejected", "reason", "upgrade_failed", "error", err)
		return
	}

	connectionID, err := newConnectionID()
	if err != nil {
		observability.IncrementUpgradesRejected()
		h.logger.Error("websocket_connection_id_failed", "error", err)
		_ = connection.Close()
		return
	}

	h.logger.Info(
		"websocket_upgrade_accepted",
		"connection_id", connectionID,
		"user_id", principal.UserID,
		"remote_address", request.RemoteAddr,
	)
	h.sessionHandler.Handle(connection, Session{
		ConnectionID: connectionID,
		UserID:       principal.UserID,
	})
}

func (h *webSocketHandler) checkOrigin(request *http.Request) bool {
	origin := strings.TrimSpace(request.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	_, ok := h.allowedOrigins[parsed.Scheme+"://"+parsed.Host]
	return ok
}

func (h *webSocketHandler) reject(response http.ResponseWriter, request *http.Request, status int, reason string) {
	observability.IncrementUpgradesRejected()
	h.logger.Info(
		"websocket_upgrade_rejected",
		"reason", reason,
		"remote_address", request.RemoteAddr,
		"origin", request.Header.Get("Origin"),
	)
	writeStatus(response, status, reason)
}

func newConnectionID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "conn_" + hex.EncodeToString(value[:]), nil
}

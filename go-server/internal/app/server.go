// Package app composes and runs the service process.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/blockforgelabs/go-websocket/internal/broker"
	"github.com/blockforgelabs/go-websocket/internal/config"
	"github.com/blockforgelabs/go-websocket/internal/httpapi"
	"github.com/blockforgelabs/go-websocket/internal/realtime"
)

// Server owns the HTTP server, hub, and process readiness lifecycle.
type Server struct {
	httpServer      *http.Server
	health          *httpapi.Health
	logger          *slog.Logger
	hub             *realtime.Hub
	broker          broker.Broker
	shutdownTimeout time.Duration
}

// NewServer constructs the server with the environment's default authenticator.
func NewServer(cfg config.Config, logger *slog.Logger) (*Server, error) {
	var authenticator httpapi.Authenticator = httpapi.DevelopmentAuthenticator{}
	if cfg.Environment == "production" {
		authenticator = httpapi.RejectingAuthenticator{}
	}
	return NewServerWithAuthenticator(cfg, logger, authenticator)
}

// NewServerWithAuthenticator exposes the production authentication seam.
func NewServerWithAuthenticator(cfg config.Config, logger *slog.Logger, authenticator httpapi.Authenticator) (*Server, error) {
	health := httpapi.NewHealth()
	hub := realtime.NewHub(logger, realtime.AllowAllAuthorizer{}, cfg.MaxRoomsPerClient)

	var b broker.Broker
	var err error
	if cfg.BrokerURL != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		b, err = broker.NewNATSBroker(ctx, broker.NATSOptions{
			URL:    cfg.BrokerURL,
			NodeID: cfg.NodeID,
			Logger: logger,
		})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("failed to create NATS broker: %w", err)
		}
	} else {
		b = broker.NewMemoryBroker()
	}

	hub.SetBroker(b, cfg.NodeID)
	health.SetChecker(b.Ready)

	webSocketHandler := httpapi.NewWebSocketHandler(httpapi.WebSocketOptions{
		Health:         health,
		Authenticator:  authenticator,
		SessionHandler: realtimeSessionHandler{config: cfg, logger: logger, hub: hub},
		AllowedOrigins: cfg.AllowedOrigins,
		MaxConnections: cfg.MaxConnections,
		Logger:         logger,
	})
	return &Server{
		httpServer: &http.Server{
			Addr:              cfg.HTTPAddress,
			Handler:           httpapi.Router(health, webSocketHandler),
			ReadHeaderTimeout: cfg.ReadHeader,
			ReadTimeout:       cfg.ReadTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
		},
		health:          health,
		logger:          logger,
		hub:             hub,
		broker:          b,
		shutdownTimeout: cfg.ShutdownTimeout,
	}, nil
}

// Handler exposes the configured handler for black-box HTTP tests.
func (s *Server) Handler() http.Handler { return s.httpServer.Handler }

// Ready reports whether the process is accepting new work.
func (s *Server) Ready() bool { return s.health.Ready() }

// ActiveClients reports the number of registered active user sessions.
func (s *Server) ActiveClients() int { return s.hub.Count() }

// Serve starts serving and blocks until cancellation or a server failure.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	defer s.broker.Close()

	// Subscribe to broker subject namespace.
	unsub, err := s.broker.Subscribe(ctx, "blockforge.>", func(subject string, data []byte) {
		event, err := broker.DecodeBrokerEvent(data)
		if err != nil {
			s.logger.Error("server_broker_decode_failed", "subject", subject, "error", err)
			return
		}
		s.hub.HandleBrokerEvent(event)
	})
	if err != nil {
		return fmt.Errorf("broker subscribe failed: %w", err)
	}
	defer unsub()

	s.health.SetReady(true)
	s.logger.Info("server_started", "address", listener.Addr().String())

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-runCtx.Done()
		s.health.SetReady(false)
		s.logger.Info("server_shutdown_started", "timeout", s.shutdownTimeout.String())

		// Notify all active clients that the server is draining.
		s.hub.BroadcastDraining()

		// Allow a bounded drain period (80% of shutdown timeout) for clients
		// to finish work and disconnect voluntarily.
		drainTimeout := s.shutdownTimeout * 8 / 10
		if drainTimeout > 0 {
			s.logger.Info("server_drain_started", "drain_timeout", drainTimeout.String())
			time.Sleep(drainTimeout)
		}

		// Force-close any sessions that did not disconnect during the drain.
		s.hub.CloseAll("server_shutdown")

		remainingTimeout := s.shutdownTimeout - drainTimeout
		if remainingTimeout <= 0 {
			remainingTimeout = time.Second
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), remainingTimeout)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			s.logger.Error("server_shutdown_failed", "error", err)
			_ = s.httpServer.Close()
		}
	}()

	err = s.httpServer.Serve(listener)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.health.SetReady(false)
		s.hub.CloseAll("server_failure")
		cancelRun()
		<-shutdownDone
		return err
	}

	<-shutdownDone
	s.logger.Info("server_stopped")
	return nil
}

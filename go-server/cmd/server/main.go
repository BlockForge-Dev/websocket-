// Command server is the process entry point for the BlockForge Labs realtime
// service.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/blockforgelabs/go-websocket/internal/app"
	"github.com/blockforgelabs/go-websocket/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("server_exit_failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", cfg.HTTPAddress)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info(
		"configuration_loaded",
		"environment", cfg.Environment,
		"http_address", cfg.HTTPAddress,
		"shutdown_timeout", cfg.ShutdownTimeout.String(),
		"max_connections", cfg.MaxConnections,
		"allowed_origins", cfg.AllowedOrigins,
		"outbound_queue_capacity", cfg.OutboundQueueCapacity,
		"websocket_read_timeout", cfg.WebSocketReadTimeout.String(),
		"websocket_write_timeout", cfg.WebSocketWriteTimeout.String(),
	)
	srv, err := app.NewServer(cfg, logger)
	if err != nil {
		return err
	}
	return srv.Serve(ctx, listener)
}

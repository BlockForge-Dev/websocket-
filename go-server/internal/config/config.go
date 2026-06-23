package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultEnvironment       = "development"
	defaultHTTPAddress       = ":8080"
	defaultReadHeader        = 5 * time.Second
	defaultReadTimeout       = 15 * time.Second
	defaultWriteTimeout      = 15 * time.Second
	defaultIdleTimeout       = 60 * time.Second
	defaultShutdownTimeout   = 10 * time.Second
	defaultMaxConnections    = 1000
	defaultOutboundQueue     = 64
	defaultWSReadTimeout     = 60 * time.Second
	defaultWSWriteTimeout    = 10 * time.Second
	defaultWSMaxMsgSize      = int64(4096)
	defaultRateLimitMsgs     = 100
	defaultRateLimitInt      = 1 * time.Minute
	defaultMaxRoomsPerClient = 10
)

var defaultDevelopmentOrigins = []string{"http://localhost:3000", "http://127.0.0.1:3000"}

// Config contains process-level settings for HTTP and WebSocket sessions.
type Config struct {
	Environment             string
	HTTPAddress             string
	ReadHeader              time.Duration
	ReadTimeout             time.Duration
	WriteTimeout            time.Duration
	IdleTimeout             time.Duration
	ShutdownTimeout         time.Duration
	AllowedOrigins          []string
	MaxConnections          int
	OutboundQueueCapacity   int
	WebSocketReadTimeout    time.Duration
	WebSocketWriteTimeout   time.Duration
	WebSocketMaxMessageSize int64
	RateLimitMessages       int
	RateLimitInterval       time.Duration
	MaxRoomsPerClient       int
	BrokerURL               string
	NodeID                  string
}

// Load reads configuration from the process environment and validates it.
func Load() (Config, error) { return LoadFrom(os.LookupEnv) }

// LoadFrom reads configuration through lookup for deterministic tests.
func LoadFrom(lookup func(string) (string, bool)) (Config, error) {
	environment := valueOrDefault(lookup, "BLOCKFORGE_ENV", defaultEnvironment)
	nodeID := valueOrDefault(lookup, "BLOCKFORGE_NODE_ID", "")
	if nodeID == "" {
		var value [16]byte
		if _, err := rand.Read(value[:]); err != nil {
			return Config{}, fmt.Errorf("failed to generate node ID: %w", err)
		}
		nodeID = "node_" + hex.EncodeToString(value[:])
	}

	cfg := Config{
		Environment:             environment,
		HTTPAddress:             valueOrDefault(lookup, "BLOCKFORGE_HTTP_ADDRESS", defaultHTTPAddress),
		ReadHeader:              defaultReadHeader,
		ReadTimeout:             defaultReadTimeout,
		WriteTimeout:            defaultWriteTimeout,
		IdleTimeout:             defaultIdleTimeout,
		ShutdownTimeout:         defaultShutdownTimeout,
		MaxConnections:          defaultMaxConnections,
		OutboundQueueCapacity:   defaultOutboundQueue,
		WebSocketReadTimeout:    defaultWSReadTimeout,
		WebSocketWriteTimeout:   defaultWSWriteTimeout,
		WebSocketMaxMessageSize: defaultWSMaxMsgSize,
		RateLimitMessages:       defaultRateLimitMsgs,
		RateLimitInterval:       defaultRateLimitInt,
		MaxRoomsPerClient:       defaultMaxRoomsPerClient,
		BrokerURL:               valueOrDefault(lookup, "BLOCKFORGE_BROKER_URL", ""),
		NodeID:                  nodeID,
	}

	if environment != "production" {
		cfg.AllowedOrigins = append([]string(nil), defaultDevelopmentOrigins...)
	}
	if value, ok := lookup("BLOCKFORGE_ALLOWED_ORIGINS"); ok && strings.TrimSpace(value) != "" {
		cfg.AllowedOrigins = splitCSV(value)
	}

	integers := []struct {
		key    string
		target *int
	}{
		{"BLOCKFORGE_MAX_CONNECTIONS", &cfg.MaxConnections},
		{"BLOCKFORGE_OUTBOUND_QUEUE_CAPACITY", &cfg.OutboundQueueCapacity},
		{"BLOCKFORGE_WS_RATE_LIMIT_MESSAGES", &cfg.RateLimitMessages},
		{"BLOCKFORGE_WS_MAX_ROOMS_PER_CLIENT", &cfg.MaxRoomsPerClient},
	}
	for _, item := range integers {
		value, ok := lookup(item.key)
		if !ok || strings.TrimSpace(value) == "" {
			continue
		}
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return Config{}, fmt.Errorf("%s must be an integer: %w", item.key, err)
		}
		*item.target = parsed
	}

	if value, ok := lookup("BLOCKFORGE_WS_MAX_MESSAGE_SIZE"); ok && strings.TrimSpace(value) != "" {
		parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("BLOCKFORGE_WS_MAX_MESSAGE_SIZE must be an integer: %w", err)
		}
		cfg.WebSocketMaxMessageSize = parsed
	}

	durations := []struct {
		key    string
		target *time.Duration
	}{
		{"BLOCKFORGE_HTTP_READ_HEADER_TIMEOUT", &cfg.ReadHeader},
		{"BLOCKFORGE_HTTP_READ_TIMEOUT", &cfg.ReadTimeout},
		{"BLOCKFORGE_HTTP_WRITE_TIMEOUT", &cfg.WriteTimeout},
		{"BLOCKFORGE_HTTP_IDLE_TIMEOUT", &cfg.IdleTimeout},
		{"BLOCKFORGE_SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout},
		{"BLOCKFORGE_WS_READ_TIMEOUT", &cfg.WebSocketReadTimeout},
		{"BLOCKFORGE_WS_WRITE_TIMEOUT", &cfg.WebSocketWriteTimeout},
		{"BLOCKFORGE_WS_RATE_LIMIT_INTERVAL", &cfg.RateLimitInterval},
	}
	for _, item := range durations {
		value, ok := lookup(item.key)
		if !ok || strings.TrimSpace(value) == "" {
			continue
		}
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be a Go duration: %w", item.key, err)
		}
		*item.target = parsed
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate rejects ambiguous or unsafe process settings.
func (c Config) Validate() error {
	var errs []error
	switch c.Environment {
	case "development", "test", "production":
	default:
		errs = append(errs, errors.New("BLOCKFORGE_ENV must be development, test, or production"))
	}
	if err := validateAddress(c.HTTPAddress); err != nil {
		errs = append(errs, fmt.Errorf("BLOCKFORGE_HTTP_ADDRESS: %w", err))
	}
	if c.MaxConnections <= 0 {
		errs = append(errs, errors.New("BLOCKFORGE_MAX_CONNECTIONS must be greater than zero"))
	}
	if c.OutboundQueueCapacity <= 0 {
		errs = append(errs, errors.New("BLOCKFORGE_OUTBOUND_QUEUE_CAPACITY must be greater than zero"))
	}
	if c.WebSocketMaxMessageSize <= 0 {
		errs = append(errs, errors.New("BLOCKFORGE_WS_MAX_MESSAGE_SIZE must be greater than zero"))
	}
	if c.RateLimitMessages <= 0 {
		errs = append(errs, errors.New("BLOCKFORGE_WS_RATE_LIMIT_MESSAGES must be greater than zero"))
	}
	if c.MaxRoomsPerClient <= 0 {
		errs = append(errs, errors.New("BLOCKFORGE_WS_MAX_ROOMS_PER_CLIENT must be greater than zero"))
	}
	if c.Environment == "production" && len(c.AllowedOrigins) == 0 {
		errs = append(errs, errors.New("BLOCKFORGE_ALLOWED_ORIGINS is required in production"))
	}
	for _, origin := range c.AllowedOrigins {
		if err := validateOrigin(origin); err != nil {
			errs = append(errs, fmt.Errorf("BLOCKFORGE_ALLOWED_ORIGINS contains %q: %w", origin, err))
		}
	}

	durations := []struct {
		name  string
		value time.Duration
	}{
		{"BLOCKFORGE_HTTP_READ_HEADER_TIMEOUT", c.ReadHeader},
		{"BLOCKFORGE_HTTP_READ_TIMEOUT", c.ReadTimeout},
		{"BLOCKFORGE_HTTP_WRITE_TIMEOUT", c.WriteTimeout},
		{"BLOCKFORGE_HTTP_IDLE_TIMEOUT", c.IdleTimeout},
		{"BLOCKFORGE_SHUTDOWN_TIMEOUT", c.ShutdownTimeout},
		{"BLOCKFORGE_WS_READ_TIMEOUT", c.WebSocketReadTimeout},
		{"BLOCKFORGE_WS_WRITE_TIMEOUT", c.WebSocketWriteTimeout},
		{"BLOCKFORGE_WS_RATE_LIMIT_INTERVAL", c.RateLimitInterval},
	}
	for _, item := range durations {
		if item.value <= 0 {
			errs = append(errs, fmt.Errorf("%s must be greater than zero", item.name))
		}
	}
	return errors.Join(errs...)
}

func valueOrDefault(lookup func(string) (string, bool), key, fallback string) string {
	value, ok := lookup(key)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			values = append(values, trimmed)
		}
	}
	return values
}

func validateAddress(address string) error {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("must be in host:port form: %w", err)
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 0 || number > 65535 {
		return errors.New("port must be a number from 0 to 65535")
	}
	return nil
}

func validateOrigin(origin string) error {
	if origin == "*" || strings.Contains(origin, "*") {
		return errors.New("wildcards are not allowed")
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("must be an absolute http or https origin")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("scheme must be http or https")
	}
	if parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("must not include a path, query, or fragment")
	}
	return nil
}

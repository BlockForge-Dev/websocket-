package config

import (
	"strings"
	"testing"
	"time"
)

func TestClientSessionConfigurationOverrides(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"BLOCKFORGE_OUTBOUND_QUEUE_CAPACITY": "32",
		"BLOCKFORGE_WS_READ_TIMEOUT":         "45s",
		"BLOCKFORGE_WS_WRITE_TIMEOUT":        "4s",
		"BLOCKFORGE_WS_MAX_MESSAGE_SIZE":     "8192",
		"BLOCKFORGE_WS_RATE_LIMIT_MESSAGES":  "200",
		"BLOCKFORGE_WS_RATE_LIMIT_INTERVAL":  "2m",
		"BLOCKFORGE_WS_MAX_ROOMS_PER_CLIENT": "15",
	}
	cfg, err := LoadFrom(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatalf("LoadFrom returned an error: %v", err)
	}

	if cfg.OutboundQueueCapacity != 32 {
		t.Fatalf("OutboundQueueCapacity = %d, want 32", cfg.OutboundQueueCapacity)
	}
	if cfg.WebSocketReadTimeout != 45*time.Second {
		t.Fatalf("WebSocketReadTimeout = %v, want 45s", cfg.WebSocketReadTimeout)
	}
	if cfg.WebSocketWriteTimeout != 4*time.Second {
		t.Fatalf("WebSocketWriteTimeout = %v, want 4s", cfg.WebSocketWriteTimeout)
	}
	if cfg.WebSocketMaxMessageSize != 8192 {
		t.Fatalf("WebSocketMaxMessageSize = %d, want 8192", cfg.WebSocketMaxMessageSize)
	}
	if cfg.RateLimitMessages != 200 {
		t.Fatalf("RateLimitMessages = %d, want 200", cfg.RateLimitMessages)
	}
	if cfg.RateLimitInterval != 2*time.Minute {
		t.Fatalf("RateLimitInterval = %v, want 2m", cfg.RateLimitInterval)
	}
	if cfg.MaxRoomsPerClient != 15 {
		t.Fatalf("MaxRoomsPerClient = %d, want 15", cfg.MaxRoomsPerClient)
	}
}

func TestClientSessionConfigurationRejectsUnsafeValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key       string
		value     string
		wantError string
	}{
		{"BLOCKFORGE_OUTBOUND_QUEUE_CAPACITY", "0", "greater than zero"},
		{"BLOCKFORGE_OUTBOUND_QUEUE_CAPACITY", "many", "must be an integer"},
		{"BLOCKFORGE_WS_READ_TIMEOUT", "0s", "greater than zero"},
		{"BLOCKFORGE_WS_WRITE_TIMEOUT", "later", "Go duration"},
		{"BLOCKFORGE_WS_MAX_MESSAGE_SIZE", "0", "greater than zero"},
		{"BLOCKFORGE_WS_MAX_MESSAGE_SIZE", "big", "must be an integer"},
		{"BLOCKFORGE_WS_RATE_LIMIT_MESSAGES", "0", "greater than zero"},
		{"BLOCKFORGE_WS_RATE_LIMIT_MESSAGES", "fast", "must be an integer"},
		{"BLOCKFORGE_WS_RATE_LIMIT_INTERVAL", "0s", "greater than zero"},
		{"BLOCKFORGE_WS_RATE_LIMIT_INTERVAL", "fast", "Go duration"},
		{"BLOCKFORGE_WS_MAX_ROOMS_PER_CLIENT", "0", "greater than zero"},
		{"BLOCKFORGE_WS_MAX_ROOMS_PER_CLIENT", "five", "must be an integer"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.key, func(t *testing.T) {
			t.Parallel()
			_, err := LoadFrom(func(key string) (string, bool) {
				if key == test.key {
					return test.value, true
				}
				return "", false
			})
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want text %q", err, test.wantError)
			}
		})
	}
}

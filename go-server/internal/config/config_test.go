package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadFromUsesDocumentedDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := LoadFrom(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatalf("LoadFrom returned an error: %v", err)
	}

	if cfg.Environment != "development" {
		t.Fatalf("Environment = %q, want development", cfg.Environment)
	}
	if cfg.HTTPAddress != ":8080" {
		t.Fatalf("HTTPAddress = %q, want :8080", cfg.HTTPAddress)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("ShutdownTimeout = %v, want 10s", cfg.ShutdownTimeout)
	}
}

func TestLoadFromReadsOverrides(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"BLOCKFORGE_ENV":                      "production",
		"BLOCKFORGE_HTTP_ADDRESS":             "127.0.0.1:9090",
		"BLOCKFORGE_HTTP_READ_HEADER_TIMEOUT": "2s",
		"BLOCKFORGE_HTTP_READ_TIMEOUT":        "20s",
		"BLOCKFORGE_HTTP_WRITE_TIMEOUT":       "25s",
		"BLOCKFORGE_HTTP_IDLE_TIMEOUT":        "90s",
		"BLOCKFORGE_SHUTDOWN_TIMEOUT":         "7s",
		"BLOCKFORGE_ALLOWED_ORIGINS":          "https://app.example,https://admin.example",
		"BLOCKFORGE_MAX_CONNECTIONS":          "250",
	}
	cfg, err := LoadFrom(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatalf("LoadFrom returned an error: %v", err)
	}

	if cfg.Environment != "production" || cfg.HTTPAddress != "127.0.0.1:9090" {
		t.Fatalf("unexpected identity settings: %+v", cfg)
	}
	if cfg.ReadHeader != 2*time.Second || cfg.ReadTimeout != 20*time.Second ||
		cfg.WriteTimeout != 25*time.Second || cfg.IdleTimeout != 90*time.Second ||
		cfg.ShutdownTimeout != 7*time.Second {
		t.Fatalf("unexpected timeout settings: %+v", cfg)
	}
}

func TestLoadFromRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		key       string
		value     string
		wantError string
	}{
		{"environment", "BLOCKFORGE_ENV", "staging", "development, test, or production"},
		{"address", "BLOCKFORGE_HTTP_ADDRESS", "localhost", "host:port"},
		{"duration syntax", "BLOCKFORGE_SHUTDOWN_TIMEOUT", "soon", "Go duration"},
		{"non-positive duration", "BLOCKFORGE_HTTP_IDLE_TIMEOUT", "0s", "greater than zero"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
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

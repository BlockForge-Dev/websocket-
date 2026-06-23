package config

import (
	"strings"
	"testing"
)

func TestProductionRequiresExplicitRestrictiveOrigins(t *testing.T) {
	t.Parallel()

	_, err := LoadFrom(func(key string) (string, bool) {
		if key == "BLOCKFORGE_ENV" {
			return "production", true
		}
		return "", false
	})
	if err == nil || !strings.Contains(err.Error(), "required in production") {
		t.Fatalf("error = %v, want production origin requirement", err)
	}

	_, err = LoadFrom(func(key string) (string, bool) {
		values := map[string]string{
			"BLOCKFORGE_ENV":             "production",
			"BLOCKFORGE_ALLOWED_ORIGINS": "*",
		}
		value, ok := values[key]
		return value, ok
	})
	if err == nil || !strings.Contains(err.Error(), "wildcards are not allowed") {
		t.Fatalf("error = %v, want wildcard rejection", err)
	}
}

func TestConnectionAdmissionConfiguration(t *testing.T) {
	t.Parallel()

	cfg, err := LoadFrom(func(key string) (string, bool) {
		values := map[string]string{
			"BLOCKFORGE_MAX_CONNECTIONS": "250",
		}
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatalf("LoadFrom returned an error: %v", err)
	}
	if cfg.MaxConnections != 250 {
		t.Fatalf("MaxConnections = %d, want 250", cfg.MaxConnections)
	}
	if len(cfg.AllowedOrigins) != 2 {
		t.Fatalf("development origins = %v, want two defaults", cfg.AllowedOrigins)
	}
}

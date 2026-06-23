package httpapi

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"github.com/blockforgelabs/go-websocket/internal/observability"
)

// Health tracks whether the process is available to receive new work.
type Health struct {
	ready   atomic.Bool
	checker func() bool
}

// NewHealth creates health state that is initially unready.
func NewHealth() *Health { return &Health{} }

// SetReady updates readiness for new traffic.
func (h *Health) SetReady(ready bool) { h.ready.Store(ready) }

// SetChecker registers a dynamic readiness checker function.
func (h *Health) SetChecker(f func() bool) { h.checker = f }

// Ready reports the current readiness state.
func (h *Health) Ready() bool {
	if !h.ready.Load() {
		return false
	}
	if h.checker != nil {
		return h.checker()
	}
	return true
}

// Router returns the process HTTP handler. A WebSocket handler may be supplied
// once connection establishment is configured.
func Router(health *Health, webSocketHandlers ...http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeStatus(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !health.Ready() {
			writeStatus(w, http.StatusServiceUnavailable, "not_ready")
			return
		}
		writeStatus(w, http.StatusOK, "ready")
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(observability.DefaultMetrics.Snapshot())
	})
	if len(webSocketHandlers) > 0 && webSocketHandlers[0] != nil {
		mux.Handle("GET /ws", webSocketHandlers[0])
	}
	return mux
}

func writeStatus(w http.ResponseWriter, status int, state string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Status string `json:"status"`
	}{Status: state})
}

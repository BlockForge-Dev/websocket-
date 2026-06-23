# BlockForge Labs Go WebSocket Service

A production-ready, highly-scalable realtime backend built in Go.

---

## Current Status

**Milestone 16: Production Review and Release (v1.0.0)**

The project is fully complete and verified. The architecture supports:
- **Horizontal Scaling**: Relays events cross-node using **NATS JetStream** with automatic local-echo deduplication.
- **Single-Node Dev Fallback**: Uses an in-memory event bus with NATS-style wildcard routing when NATS is not configured.
- **Resource Protection & Security**: Enforces connection limits, maximum frame size limits, sliding-window rate limiters, heartbeat pong cleanup, and strict backpressure queues that disconnect slow clients.
- **Observability**: Exposes Prometheus-style counts, heartbeat timeouts, message rates, and processing latency histograms under `/metrics`, alongside structured JSON logs.
- **Zero-Downtime Deployment**: Separates liveness/readiness probes (`/readyz` goes unready during rolling updates) and broadcasts `server.draining` to active connections during shutdown.

The roadmap is located in [`../blockforge-websocket-presenter-pack/04-milestone-roadmap.md`](../blockforge-websocket-presenter-pack/04-milestone-roadmap.md).

---

## Prerequisites

- Go 1.26.2+
- Git
- NATS (optional, for multi-node setups)
- GNU Make, or PowerShell 7+ on Windows

---

## Setup & Run

### 1. Initial Setup
From this directory:
```bat
scripts\setup.cmd
```
On Unix systems:
```sh
make setup
```

### 2. Run the Server
```bat
go run ./cmd/server
```

The default address is `:8080`.
- Health check: `GET http://localhost:8080/healthz`
- Readiness check: `GET http://localhost:8080/readyz`
- Metrics report: `GET http://localhost:8080/metrics`

Press `Ctrl+C` to gracefully drain client connections and stop the server.

### 3. Run the Load Tester
Stress-test your local or remote server using the concurrent load testing client:
```bat
# Spin up 100 concurrent clients, running for 10 seconds, broadcasting 1 message/sec per client
go run ./cmd/loadtest -clients=100 -duration=10s -rate=1
```

---

## Validate Codebase

Run `scripts\check.cmd` on Windows or `make check` on Unix to:
1. Verify `gofmt` code formatting.
2. Run Go compiler vet warnings (`go vet`).
3. Execute all unit and integration tests (including the multi-node in-memory integration test).

---

## Repository Structure

```text
go-server/
  cmd/server/              WebSocket service entry point & runtime loop
  cmd/loadtest/            Go-based concurrent stress testing tool
  internal/app/            Server dependency setup and server lifecycle
  internal/broker/         Broker abstraction (NATS JetStream & Memory Broker)
  internal/config/         Configuration loading, defaults, and validation
  internal/httpapi/        HTTP Router, healthz/readyz handlers, upgrades
  internal/realtime/       WebSocket client session loops, hub state, routing
  internal/observability/  Metrics counters & structured logging
  tests/                   Black-box integration tests
  docs/                    Architecture specifications and runbooks
```

---

## Engineering Rules

1. **State Ownership**: The Hub is the sole owner of room membership and client registration state. Sockets are owned by client sessions.
2. **Decoupled I/O**: Sockets have exactly one reading goroutine and one writing goroutine. Outbound writes are mediated through a buffered channel (`Client.send`).
3. **Lock-Free Writes**: Outbound writes are enqueued asynchronously; the Hub lock is never held during I/O.
4. **Resources are Bounded**: Frame sizes, room sizes, and outbound queue capacity are bounded.
5. **Idempotency**: Clean connection shutdown follows exactly one atomic cleanup path.

---

## Documentation

- [Architecture](docs/architecture.md)
- [Architecture Review & Invariants](docs/architecture-review.md)
- [Threat Model & Security](docs/threat-model.md)
- [Operations Runbook](docs/runbook.md)
- [Multi-Node Scaling Specification](docs/multi-node.md)
- [Client Session Lifecycle](docs/client-lifecycle.md)
- [Hub Registration](docs/hub-registration.md)
- [Message Protocol Spec](docs/message-protocol.md)
- [Graceful Draining & Deployment](docs/graceful-draining.md)
- [V1.0.0 Release Notes](docs/release-notes-v1.0.0.md)

---

## License

All rights reserved.

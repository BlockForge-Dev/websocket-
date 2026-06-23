# Architecture

## Purpose

This service is a stateful realtime delivery boundary. It owns long-lived
connections and transient routing; it does not own product-domain decisions or
durable business state.

```text
Client
  |
  | HTTP request and WebSocket upgrade
  v
HTTP API
  |
  | validated identity and upgraded connection
  v
Client session <---- bounded outbound queue
  |                         ^
  | validated commands      | routed events
  v                         |
Hub ------------------------+
  |
  v
Application services
```

The future multi-node form adds a broker between WebSocket nodes while each
node continues to own its local sockets.

## Package responsibilities

### `cmd/server`

Owns process startup, dependency construction, signal handling, bounded
shutdown, and top-level execution. It contains composition, not routing logic.

### `internal/config`

Owns environment parsing, defaults, and validation for addresses, timeouts,
queue capacity, frame size, and rate limits.

### `internal/httpapi`

Owns route registration, health and readiness, request validation,
authentication at connection establishment, origin policy, protocol upgrade,
and handoff to a client session.

### `internal/realtime`

Owns client sessions, socket loops, the message protocol, active-client
registration, duplicate-session policy, room membership, routing, heartbeats,
local rate limiting, and backpressure.

The client session owns socket I/O. The hub exclusively owns the active-client
registry and later room state. Duplicate registration installs the replacement
before closing the stale session, and unregister removes only an exact session
match.

### `internal/observability`

Owns structured logging fields and metric instruments without leaking
credentials or sensitive payloads.

### `tests`

Owns behavioral and integration evidence across public process and protocol
boundaries.

## Core invariants

1. Every mutable state category has one clear owner.
2. Every socket has one normal reader and one normal writer.
3. Hub operations enqueue messages; they do not write to sockets.
4. Outbound queues and inbound frames have explicit limits.
5. All connection exits converge on idempotent cleanup.
6. Authentication does not replace per-operation authorization.
7. Accepted or queued messages are not described as durably delivered.
8. Shared state is process-local until the broker milestone.

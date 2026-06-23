# Graceful Draining And Deployment Readiness

This document describes the server's graceful draining behavior, client reconnect strategies, and reverse-proxy timeout alignment for zero-downtime rolling deployments.

## Overview

When the server process receives a termination signal (`SIGINT` or `SIGTERM`), it follows a structured shutdown sequence designed to minimize disruption to active WebSocket sessions:

```text
Signal Received
    │
    ▼
Set /readyz → 503 (reject new traffic)
    │
    ▼
Broadcast server.draining to all active clients
    │
    ▼
Wait bounded drain period (80% of ShutdownTimeout)
    │
    ▼
Force-close remaining sessions with reason "server_shutdown"
    │
    ▼
Shutdown HTTP server
    │
    ▼
Process exits
```

## Shutdown Sequence

### 1. Readiness Transition

Immediately upon receiving the cancellation signal:
- `/readyz` returns `503 Service Unavailable`.
- `/healthz` continues returning `200 OK` (the process is still alive).
- The load balancer should stop routing new connections to this instance.

### 2. Server Draining Event

The server broadcasts a `server.draining` event to every active client session:

```json
{
  "version": "1",
  "type": "server.draining",
  "request_id": "",
  "event_id": "evt_...",
  "payload": {
    "message": "Server is shutting down. Please reconnect."
  },
  "sent_at": "2026-06-22T15:00:00Z"
}
```

This event is advisory. Clients should use it to initiate a clean reconnection to another server instance.

### 3. Bounded Drain Period

After broadcasting, the server waits for **80% of the configured `ShutdownTimeout`** to allow clients to finish in-flight operations and disconnect voluntarily.

| ShutdownTimeout | Drain Period | Force-Close Budget |
| :--- | :--- | :--- |
| 10s (default) | 8s | 2s |
| 30s | 24s | 6s |

### 4. Force-Close Remaining Sessions

Any sessions that did not disconnect during the drain period are force-closed with close reason `"server_shutdown"`. The remaining timeout budget is used for the HTTP server's `Shutdown()` call.

---

## Client Reconnect Strategy

When a client receives the `server.draining` event, it should:

1. **Complete any in-flight operations** (e.g., finish pending broadcasts).
2. **Disconnect gracefully** by closing the WebSocket connection.
3. **Reconnect using exponential backoff with full jitter** to avoid a thundering herd.

### Recommended Backoff Algorithm

```text
base_delay  = 100ms
max_delay   = 30s
attempt     = 0

loop:
    delay = min(base_delay * 2^attempt, max_delay)
    jittered_delay = random(0, delay)
    sleep(jittered_delay)
    attempt += 1
    try connect
    if success: break
```

**Full jitter** (randomizing the entire delay, not just a portion) is critical to decorrelate reconnection attempts across many clients during a rolling deployment.

---

## Reverse-Proxy Timeout Alignment

When deploying behind a reverse proxy (Nginx, HAProxy, AWS ALB, etc.), the proxy's idle timeout must be aligned with the server's heartbeat configuration:

| Parameter | Recommended Value | Rationale |
| :--- | :--- | :--- |
| Server `WebSocketReadTimeout` | 60s (default) | How long the server waits for any data (including pong) before considering the connection stale. |
| Server `PingInterval` | ~54s (90% of read timeout) | How often the server sends WebSocket ping frames. |
| Proxy `proxy_read_timeout` (Nginx) | ≥ 75s | Must exceed `WebSocketReadTimeout` to avoid the proxy closing the connection before the server's heartbeat detects staleness. |
| Proxy `proxy_send_timeout` (Nginx) | ≥ 60s | Must accommodate the largest expected write burst. |

### Nginx Example

```nginx
location /ws {
    proxy_pass http://backend;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 75s;
    proxy_send_timeout 60s;
}
```

### Key Rule

> The proxy idle timeout must be **strictly greater** than the server's `WebSocketReadTimeout`. Otherwise, the proxy will sever the connection before the server's heartbeat mechanism has a chance to detect and clean up a stale client.

---

## Liveness vs. Readiness

| Endpoint | During Normal Operation | During Shutdown |
| :--- | :--- | :--- |
| `GET /healthz` | `200 OK` | `200 OK` |
| `GET /readyz` | `200 OK` | `503 Service Unavailable` |

The load balancer should health-check against `/readyz`. Kubernetes probes should use `/healthz` for liveness and `/readyz` for readiness.

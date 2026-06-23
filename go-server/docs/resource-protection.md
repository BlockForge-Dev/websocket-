# Resource Protection and Input Boundaries

This document details the design, algorithms, limits, and error handling for inbound resource protection and rate limiting on client connections.

## Overview

To protect the server from memory exhaustion, abuse, and accidental message floods, several layers of boundary enforcement are applied to every active WebSocket session:

1. **Max Message Size**: Bounded payload sizes at the WebSocket socket level.
2. **Message Rate Limiter**: Per-connection rate limiting to prevent spamming.
3. **Max Rooms per Client**: Bounds on the number of room memberships a single connection can hold.

```text
 Client
   |
   +---> WebSocket Frame Size Check ---> Exceeded? Yes: Close Connection ("oversized_message")
   |
   +---> Message Rate Limiter (Token Bucket) -> Exceeded? Yes: Error Payload ("rate_limit_exceeded")
   |
   +---> Join Room Limit check ----------> Exceeded? Yes: Error Payload ("unauthorized" / capacity message)
```

## Configured Limits

The boundaries are configurable at the process level via environment variables:

| Environment Variable | Config Struct Field | Default | Description |
| :--- | :--- | :--- | :--- |
| `BLOCKFORGE_WS_MAX_MESSAGE_SIZE` | `WebSocketMaxMessageSize` | `4096` bytes | Maximum size in bytes of a single incoming WebSocket frame. |
| `BLOCKFORGE_WS_RATE_LIMIT_MESSAGES` | `RateLimitMessages` | `100` | The capacity/burst allowance of the rate limit token bucket. |
| `BLOCKFORGE_WS_RATE_LIMIT_INTERVAL` | `RateLimitInterval` | `1m` | The refill interval for the rate limit token bucket. |
| `BLOCKFORGE_WS_MAX_ROOMS_PER_CLIENT` | `MaxRoomsPerClient` | `10` | The maximum number of rooms a single connection can join. |

All configuration limits must be greater than zero.

---

## 1. Max Message Size Enforcement

### Behavior
At the beginning of each connection, the read limit is configured on the underlying socket connection:
```go
connection.SetReadLimit(options.MaxMessageSize)
```

If the client sends a frame exceeding this limit:
- The WebSocket connection is immediately terminated by the server's read loop.
- The close event is classified under the close reason `"oversized_message"`.
- The connection is closed with the WebSocket status code `1009` (Message Too Big).

---

## 2. Message Rate Limiting

### Algorithm: Token Bucket
Each connection maintains a thread-safe `RateLimiter` based on the token bucket algorithm:
- The bucket starts full, containing `RateLimitMessages` tokens.
- Each incoming message consumes exactly `1.0` token.
- Tokens refill continuously over time at a rate of `RateLimitMessages` tokens per `RateLimitInterval`.
- The bucket has a hard cap equal to `RateLimitMessages` to prevent accumulation of infinite burst capacity.

An injectable clock source is used in the implementation to support fully deterministic verification in unit tests.

### Error Protocol
When a client exceeds their rate limit:
- The server does **not** terminate the connection. This allows transiently bursty but well-behaved clients to recover.
- The incoming message is discarded and not routed or acknowledged.
- An error event is sent back to the client immediately with the `rate_limit_exceeded` error code.

#### Exceeded Rate Limit Response
```json
{
  "version": "1",
  "type": "error",
  "request_id": "req_xyz",
  "event_id": "evt_...",
  "payload": {
    "code": "rate_limit_exceeded",
    "message": "Rate limit exceeded.",
    "retryable": false
  },
  "sent_at": "2026-06-22T12:00:00Z"
}
```

---

## 3. Room Membership Cap

### Behavior
To prevent memory exhaustion via high-membership connections, each client session is restricted to a maximum number of concurrent room joins:
- When a client issues a `room.join` command:
  1. The server checks if the client is already in the target room (enforcing idempotency).
  2. If the client is not in the room and the number of currently joined rooms is greater than or equal to `MaxRoomsPerClient`, the join is rejected.
- A rejected join returns an error payload to the client, while keeping the connection and all existing room memberships intact.

#### Exceeded Room Limit Response
```json
{
  "version": "1",
  "type": "error",
  "request_id": "req_join_failed",
  "event_id": "evt_...",
  "payload": {
    "code": "unauthorized",
    "message": "maximum room membership limit reached",
    "retryable": false
  },
  "sent_at": "2026-06-22T12:05:00Z"
}
```

---

## Limitations of Local Enforcement

1. **Per-Connection Enforcement**: Limits are evaluated per connection. If a user is authorized to open multiple concurrent WebSocket connections, each connection is evaluated and limited independently.
2. **Process-Local State**: The rate limiter and room limits are tracked in memory per-process. In a distributed multi-node architecture (e.g. behind a load balancer), these limits are enforced by individual server nodes and do not coordinate globally.

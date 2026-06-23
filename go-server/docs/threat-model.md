# Threat Model and Security Review

This document describes security boundaries, potential threats, and corresponding mitigations implemented in the BlockForge Labs realtime service.

---

## 1. Trust Boundaries

```text
Untrusted Client Frame
  |
  | (HTTP Handshake with Origin & Authenticator validation)
================== Trust Boundary (HTTP API Upgrade) ==================
  v
Upgraded WebSocket Connection (Identified User Session)
  |
  | (Per-command rate limits, frame limits, Authorizer checks)
================== Trust Boundary (Realtime Router) ==================
  v
Hub / Rooms Routing
```

---

## 2. Threat Analysis & Mitigations

### 2.1 Denial of Service (DoS / DDoS)

- **Threat**: Attackers flood the WebSocket server with connections, oversized payloads, or high frequency messages to exhaust CPU, socket descriptors, or memory.
- **Mitigations**:
  - **Connection Limits**: Max connections are enforced globally during the HTTP upgrade phase (`BLOCKFORGE_MAX_CONNECTIONS`). If exceeded, connections are immediately rejected with `503 Service Unavailable`.
  - **Max Frame Size**: Sockets enforce a strict incoming frame size limit (`BLOCKFORGE_WS_MAX_MESSAGE_SIZE`, default: 4KB). Payload buffers larger than this trigger an immediate client close.
  - **Rate Limiting**: Per-connection sliding window rate limiters (`BLOCKFORGE_WS_RATE_LIMIT_MESSAGES` per `BLOCKFORGE_WS_RATE_LIMIT_INTERVAL`) reject excessive client frames, returning a structured `rate_limit_exceeded` error.

### 2.2 Unauthenticated Access & Session Hijacking

- **Threat**: Clients connect without authorization or hijack active user sessions.
- **Mitigations**:
  - **Origin Policy**: Strict validation of the `Origin` header during handshake upgrade. Production mode rejects wildcards and requires explicit whitelist configuration (`BLOCKFORGE_ALLOWED_ORIGINS`).
  - **Authentication Seam**: Upgrade handlers delegate to the `Authenticator` interface to validate tokens. If authentication fails, the handshake is aborted (returns `401 Unauthorized`).
  - **Single Active Session**: The Hub registers clients by User ID. If a user establishes a new session on any node, the older session is closed with `duplicate_session_replaced`, preventing multi-device hijacking/cloning of the active session.

### 2.3 Cross-Room and Private Data Leakage

- **Threat**: A client listens to rooms they do not belong to, or spoof sender identities.
- **Mitigations**:
  - **Identity Anchoring**: The `Client` session holds the authenticated `UserID` resolved during the HTTP upgrade phase. The client cannot forge or alter their `UserID` in command frames; the server overrides any client-supplied sender fields with the anchored `UserID` on the backend.
  - **Room Authorization**: Before mutating room memberships (joining), the Hub invokes the `Authorizer` hook (`AuthorizeJoin`). Similarly, broadcasts require the client to be a member of the room or possess explicit publishing authorization (`AuthorizePublish`).

### 2.4 Slow Client Memory Leakage

- **Threat**: A slow client consumes messages slower than they are produced, causing outbound socket write buffers to grow indefinitely and exhaust server memory.
- **Mitigations**:
  - **Bounded Outbound Queues**: Outbound buffers are limited (`BLOCKFORGE_OUTBOUND_QUEUE_CAPACITY`, default: 64). 
  - **Strict Backpressure**: If the outbound queue is full, the server drops the message and disconnects the client with reason `slow_client_disconnect` rather than allowing unbounded queue growth.

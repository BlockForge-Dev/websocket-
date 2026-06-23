# Architecture Review

This document reviews the Go WebSocket service against the 8 core invariants defined in the architecture contract.

---

## 1. Single-Owner Mutable State

**Invariant**: *Every mutable state category has one clear owner.*

- **Verification**: 
  - All active user sessions are registered within `Hub.clients`.
  - All room memberships are stored within `Hub.rooms` and `Hub.clientRooms`.
  - Access to these maps is guarded exclusively by the `Hub.mu` read-write mutex.
  - Sockets and connection state are owned individually by each `Client` session. There is no external mutation of socket state.

---

## 2. Decoupled Socket I/O (One Reader, One Writer)

**Invariant**: *Every socket has one normal reader and one normal writer.*

- **Verification**:
  - **Reader Path**: Each connection spawns a single goroutine running `Client.readLoop()`. This is the only goroutine that calls `conn.ReadMessage()`.
  - **Writer Path**: Each connection spawns a single goroutine running `Client.writeLoop()`. This is the only goroutine that calls `conn.WriteMessage()`.
  - Concurrency safety is maintained; the socket reader and writer paths do not overlap or share lock boundaries.

---

## 3. Lock-Free Socket Writes during Hub Operations

**Invariant**: *Hub operations enqueue messages; they do not write to sockets.*

- **Verification**:
  - The `Hub` does not write directly to WebSocket connections during room broadcasts or private routing.
  - Instead, the `Hub` snapshots the recipient list under lock, releases the lock, and calls `Client.SendJSON(event)`.
  - `Client.SendJSON` enqueues the serialized payload into a buffered Go channel (`Client.send`). If the channel is full, it immediately returns false without blocking the Hub's thread of execution.

---

## 4. Bounded Input and Output Resource Limits

**Invariant**: *Outbound queues and inbound frames have explicit limits.*

- **Verification**:
  - **Inbound Protection**: The upgrade phase sets the maximum frame reader limit via `conn.SetReadLimit(cfg.WebSocketMaxMessageSize)`. Rate limiters protect the incoming frame processing.
  - **Outbound Protection**: The client outbound channel `Client.send` is constructed with a capacity of `cfg.OutboundQueueCapacity` (default: 64).

---

## 5. Idempotent Connection Cleanup

**Invariant**: *All connection exits converge on idempotent cleanup.*

- **Verification**:
  - All paths to termination (read failure, write failure, duplicate registration, heartbeat timeout, graceful draining) call `Client.Close(reason)`.
  - `Client.Close(reason)` uses a sync/atomic swap (`closed.Swap(true)`) to guarantee the cleanup routine runs exactly once.
  - Cleanup cancels the context (which terminates read and write loops), closes the WebSocket connection, and safely calls `Hub.Unregister(c)`.

---

## 6. Operation-Level Authorization

**Invariant**: *Authentication does not replace per-operation authorization.*

- **Verification**:
  - Authentication occurs strictly during the HTTP upgrade handshake via `Authenticator.Authenticate`.
  - Authorization is verified on every subsequent client frame:
    - Joining a room checks `Hub.authorizer.AuthorizeJoin(userID, roomID)`.
    - Broadcasting checks `Hub.AuthorizeBroadcast(client, roomID)` (allowing room members or authorized publishers).

---

## 7. Delivery Guarantees (Acknowledged vs Durable)

**Invariant**: *Accepted or queued messages are not described as durably delivered.*

- **Verification**:
  - The protocol returns a `command.ack` event to the sender once a command is validated and enqueued into local recipient buffers (or published to NATS JetStream).
  - This acknowledgement is characterized as a "successful route", never as a durable write to the client device. 

---

## 8. Coordination State Boundary

**Invariant**: *Shared state is process-local until the broker milestone.*

- **Verification**:
  - The broker abstraction (`internal/broker/Broker`) extends routing across nodes using NATS JetStream.
  - Each node remains the exclusive owner of its local client sockets. Nodes coordinate by publishing room and private events to the broker.
  - Local-echo deduplication via `NodeID` prevents double delivery, keeping cluster coordination isolated from client socket processing.

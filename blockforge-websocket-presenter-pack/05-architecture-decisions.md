# Version-One Architecture Decisions

These decisions keep the presentation and implementation internally consistent.

## ADR-001: Best-Effort Realtime Delivery

Version one delivers messages only to currently connected recipients. It does
not persist, replay, or guarantee client processing.

**Reason:** The first implementation isolates connection lifecycle and routing
without introducing durable message state.

**Consequence:** "Accepted" and "queued" must not be described as "delivered to
the user."

## ADR-002: One Active Connection Per User

Version one maps one authenticated user to one active session per process. A new
session replaces the previous session.

**Reason:** This creates a clear direct-routing model for the first iteration.

**Consequence:** Multi-device support requires changing the registry from one
session to a set of sessions.

## ADR-003: One Reader And One Writer Per Socket

Each session owns one read loop and one write loop. Other components enqueue
outbound messages but never perform normal socket writes.

**Reason:** This serializes writes, clarifies ownership, and reduces concurrency
defects.

## ADR-004: Bounded Outbound Queues

Every connection has a fixed-capacity outbound queue.

**Reason:** A slow client must not create unbounded memory growth or block room
fan-out.

**Consequence:** Queue overflow follows a documented disconnect policy in
version one.

## ADR-005: Hub Owns Shared Realtime State

The hub owns connected-client and room-membership state.

**Reason:** Shared mutable state requires one visible ownership boundary.

**Consequence:** Business-domain logic and direct socket I/O remain outside the
hub.

## ADR-006: Authorization Is Per Operation

Authentication happens at connection establishment. Join, publish, and private
delivery actions remain subject to authorization.

**Reason:** A valid connection is not permanent permission for every action.

## ADR-007: In-Memory State First

The first version stores connection and room state in one process.

**Reason:** It proves lifecycle and ownership invariants before distributed
coordination is introduced.

**Consequence:** The first version does not claim horizontal routing.

## ADR-008: Broker Coordinates Nodes

Redis or NATS will carry cross-node events. Each WebSocket node retains ownership
of its local connections.

**Reason:** A broker coordinates process boundaries but should not own socket
lifecycle.

## ADR-009: Protocol Is Versioned

Messages use an explicit envelope and protocol version.

**Reason:** Client/server contracts need stable validation, errors, and an
evolution path.

## ADR-010: Operational Limits Are Configuration

Heartbeat timings, queue capacity, frame size, rate limits, and drain timeout
are validated runtime configuration.

**Reason:** These values define resource and failure behavior and should not be
scattered as unexplained constants.


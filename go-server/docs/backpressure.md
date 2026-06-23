# Backpressure And Slow-Client Policy

This document details how the Go WebSocket server handles slow clients to protect process memory and system-wide availability.

## The Problem

WebSocket servers are stateful and handle long-lived connections. If a producer (like a room broadcast or a private message sender) produces data faster than a recipient can consume it (due to a slow socket, network latency, or a sluggish client library), the server needs to buffer those outbound messages.

If the buffer is unbounded, the server will consume more and more memory, eventually running out of memory (OOM) and crashing the entire process, impacting all connected clients.

## The Bounded Queue Policy (Version One)

To protect overall availability, the server implements a bounded queue and strict slow-client disconnect policy:

1. **Enforced Boundaries**: Every client session has a fixed capacity outbound queue configured via `BLOCKFORGE_OUTBOUND_QUEUE_CAPACITY` (default is `64` messages).
2. **Non-Blocking Enqueue**: When sending a message to a client, the enqueue operation is entirely non-blocking. If the client's queue is full, the message is not enqueued, and the connection is immediately terminated.
3. **Close Classification**: Slow connections are closed with a specific `"outbound_queue_full"` close reason.
4. **Clean Exit**: Terminating the session triggers the standard lifecycle cleanup (unregisters the client, removes it from all rooms, and cleans up socket descriptors).

## Metrics & Observability

To make backpressure decisions visible to operators, the following instrumentation is in place:

- **Queue Depth Tracking**: The `QueueDepth()` method on the client returns the number of pending items in the outbound channel.
- **Queue Full Warning Logs**: When a queue overflows, a `websocket_queue_full` warning log is emitted containing the `connection_id`, `user_id`, `queue_depth`, and `queue_capacity`.
- **Warning Logs in Routing**: When a room broadcast or private message cannot be routed because a recipient is slow, the routing loop logs a warning with the recipient's connection ID and current queue depth.
- **Queue-Full Disconnect Metric**: A thread-safe global counter tracks total `QueueFullDisconnects`.

## Product-Specific Alternatives

While disconnecting the client is the safest policy for general availability and resource limits, different products might require alternative handling:

### 1. Ring Buffer / Drop Oldest
- **Description**: When the queue is full, discard the oldest message in the buffer and enqueue the new one.
- **Use Case**: Best for high-frequency updates where newer data completely supersedes older data (e.g. stock market price feeds, vehicle GPS telemetry).

### 2. Drop Newest
- **Description**: Simply drop the new incoming message when the queue is full.
- **Use Case**: Best for low-priority notifications or optional telemetry logs where keeping the history of earlier events is more important than receiving the latest.

### 3. Persistent Spilling
- **Description**: Spill excess messages to an external disk or cache (e.g., Redis or database) when the in-memory queue is full, then replay them when the socket drains.
- **Use Case**: Critical for systems requiring at-least-once delivery guarantees (e.g. chat applications, transactional state changes).
- **Trade-offs**: Significantly increases latency, complexity, and resource usage on external infrastructure.

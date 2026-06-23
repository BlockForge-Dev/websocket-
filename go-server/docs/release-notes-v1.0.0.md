# Release Notes (v1.0.0)

We are proud to release `v1.0.0` of the BlockForge Labs Go WebSocket realtime service. This release marks the completion of the core feature set, multi-node scaling, and production reliability boundary requirements.

---

## 1. Compliance Matrix

| Objective / Guarantee | Implementation Details | Status |
|---|---|---|
| **Bounded Startup & Shutdown** | Signals handled cleanly, readiness flag updates, bounded drain phase. | **Verified** |
| **Upgrade Safety** | Validates upgrade headers, enforces whitelisted origin checks and authentication. | **Verified** |
| **Decoupled Socket Loop** | Dual-goroutine per connection (read loop + write loop), unshared socket I/O. | **Verified** |
| **Idempotent Connection Exit** | Atomic swap for closed states, uniform exit paths to hub cleanup. | **Verified** |
| **Horizontal Scalability** | Abstracted pub/sub broker (with NATS JetStream and Local Memory fallback). | **Verified** |
| **Backpressure Enforcement** | Strict queue bounds; slow-client disconnect policy prevents memory leak. | **Verified** |
| **Observability Instruments** | Structured logging, metrics endpoints (latencies, counts, rates). | **Verified** |

---

## 2. Key Known Limitations

### 2.1 Outbound Queue Drop & Disconnect Policy
- **Description**: Slow clients that fall behind the configured queue capacity (`BLOCKFORGE_OUTBOUND_QUEUE_CAPACITY`) are disconnected.
- **Rationale**: The server acts as a low-latency routing boundary, not a message queue. To avoid memory starvation, it prioritizes server stability over slow connections.
- **Workaround**: Clients must handle reconnection with backoff/jitter. If clients regularly hit this, increase `BLOCKFORGE_OUTBOUND_QUEUE_CAPACITY`.

### 2.2 Lack of Offline Persistence
- **Description**: If a user is offline, private messages sent to them are dropped. The server does not store messages.
- **Rationale**: This is a stateful realtime routing service, not a persistent database or chat storage engine.
- **Workaround**: Deliveries should be backed by an external persistence layer that holds history, using this service strictly for active online fan-out.

### 2.3 Connection-Local Rate Limiting
- **Description**: Rate limits are enforced on the individual connection level on each node. They do not share global token buckets across nodes.
- **Rationale**: Keeps node CPU overhead low and keeps operations independent of cross-node network lookups.
- **Workaround**: Deploy global rate limiting at the API gateway layer if strict distributed rate limiting is needed.

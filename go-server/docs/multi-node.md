# Multi-Node Fan-Out and Broker Abstraction

## Purpose

To scale horizontally, the service supports running multiple WebSocket server replicas. Since a client is connected to a single node, room broadcasts and private messages must be relayed across nodes. This is achieved via a Publish/Subscribe broker abstraction.

```text
Node A <--- WebSocket ---> Client 1 (Room X)
  |
  +-- Publish "blockforge.rooms.X"
  |
  v
NATS JetStream (Durable Stream "BLOCKFORGE")
  ^
  | Deliver "blockforge.rooms.X"
  |
  +-- Subscribe "blockforge.>"
  |
  v
Node B <--- WebSocket ---> Client 2 (Room X)
```

---

## Configuration

The broker is configured using environment variables:

- `BLOCKFORGE_BROKER_URL`: The URL of the NATS JetStream server (e.g., `nats://localhost:4222`). If empty, the server defaults to single-node development mode.
- `BLOCKFORGE_NODE_ID`: A unique string identifying this server instance. If empty, a unique node ID (prefixed with `node_`) is automatically generated on startup.

---

## Broker Abstraction

The contract is defined in `internal/broker/broker.go`:

```go
type Broker interface {
	Publish(ctx context.Context, subject string, data []byte) error
	Subscribe(ctx context.Context, subject string, handler func(subject string, data []byte)) (cancel func(), err error)
	Ready() bool
	Close() error
}
```

### Implementations

1. **In-Memory Broker (`MemoryBroker`)**:
   - Used when `BLOCKFORGE_BROKER_URL` is empty.
   - Provides process-local, channel-based routing with NATS-style wildcard matching (`>`).
   - Enables zero-dependency local development and local unit tests.

2. **NATS JetStream Broker (`NATSBroker`)**:
   - Used in production when a broker URL is provided.
   - Connects to NATS JetStream and manages a stream named `BLOCKFORGE` with subject workspace `blockforge.>`.
   - Uses durable push consumers to ensure reliable delivery.

---

## Naming and Routing

### Subject Workspace

Events published to the broker are routed based on subjects:

- **Room Broadcast**: `blockforge.rooms.<room_id>`
- **Private Message**: `blockforge.users.<recipient_user_id>`

### Wire Envelope

All messages carried by the broker are wrapped in `broker.BrokerEvent`:

```go
type BrokerEvent struct {
	NodeID      string          `json:"node_id"`
	Type        string          `json:"type"`
	RoomID      string          `json:"room_id,omitempty"`
	SenderID    string          `json:"sender_id"`
	RecipientID string          `json:"recipient_id,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}
```

---

## Reliability and Semantics

### Local Echo Deduplication

To minimize latency, when a client sends a message, the receiving node immediately delivers it to all other local clients.
Simultaneously, the node publishes the event to the broker.
To prevent the publishing node from delivering the message a second time when it receives it back from the broker:
1. Every published message is stamped with the origin's `NodeID`.
2. When the broker delivers an event, the receiving node compares the event's `NodeID` with its own `NodeID`.
3. If they match, the event is silently discarded.

### Delivery Guarantees

- **At-Least-Once Delivery**: Under normal operation, JetStream ensures at-least-once delivery of broker events to all active nodes.
- **Durable Stream**: The `BLOCKFORGE` stream is configured with `InterestPolicy` retention and a `MaxAge` of 5 minutes. Events are cleaned up once all active consumers (nodes) have acknowledged them.
- **Automatic Reconnection**: The NATS client library is configured with infinite reconnect retries. If the NATS cluster becomes temporarily unreachable, nodes will queue outbound publishes and automatically reconnect.

---

## Health and Observability

### Readiness Checks

The broker's health status feeds into the `/readyz` HTTP endpoint. If the NATS connection is lost:
1. `Ready()` returns `false`.
2. The `/readyz` endpoint reports `503 Service Unavailable`.
3. Load balancers/Kubernetes probes automatically redirect new traffic away from the unhealthy node.

### Metrics

Three metrics are tracked under `/metrics`:

- `broker_publish_count`: Total number of events successfully published to the broker.
- `broker_receive_count`: Total number of events received from the broker.
- `broker_publish_errors`: Total number of failed publish operations.

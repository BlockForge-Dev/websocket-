# Version-One Scope

## Included

Version one will provide:

- validated process configuration
- liveness, readiness, and bounded graceful shutdown
- authenticated WebSocket upgrade
- one active connection per user per process
- one read loop and one write loop per connection
- a versioned JSON protocol with structured acknowledgements and errors
- authorized room membership, broadcast, and online private delivery
- heartbeat-based stale-connection cleanup
- bounded outbound queues and a deterministic slow-client policy
- frame-size limits and connection-local rate limiting
- behavioral, integration, race, and failure tests
- structured logs, operational metrics, and graceful draining

## Delivery promise

Delivery is best effort and transient. Accepted or queued does not mean durable
storage, recipient processing, or human visibility.

## Deliberately excluded

- offline message storage
- durable delivery, replay, or message history
- globally ordered room events
- distributed presence in the single-node phase
- multi-region routing
- end-to-end encryption
- global distributed user-rate quotas
- unrestricted multi-device sessions

Milestone 15 adds cross-node transient fan-out through Redis Pub/Sub or NATS. It
does not silently strengthen the delivery guarantee.

## Product policies

- A newer connection for the same user replaces the older connection.
- Room membership and publishing are separately authorized.
- A full outbound queue triggers the documented slow-client policy.
- Empty-room, sender-inclusion, and token-expiry behavior must be finalized
  before their implementing milestones are marked complete.

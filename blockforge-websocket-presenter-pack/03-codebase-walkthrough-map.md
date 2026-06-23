# Go Codebase Walkthrough Map

This is the exact repository walkthrough to use during the architecture episode
and the later implementation episode.

## Reference tree

```text
go-server/
  cmd/
    server/
      main.go
  internal/
    config/
      config.go
    httpapi/
      router.go
    realtime/
      client.go
      hub.go
      message.go
      rate_limit.go
    observability/
      logging.go
      metrics.go
  tests/
    websocket_test.go
  go.mod
  Makefile
  README.md
```

## `cmd/server/main.go`

### What this file owns

- process startup
- configuration loading
- dependency construction
- HTTP server startup
- operating-system signal handling
- graceful shutdown

### What to point at

- `main` remains composition code
- the hub is constructed once and injected
- the HTTP router receives dependencies
- shutdown has a bounded timeout
- background components stop with the process context

### What to say

This is the process boundary. It wires the system together, but it does not
contain WebSocket routing or message logic. Keeping `main` small makes startup
and shutdown visible without turning it into the application.

### What not to explain

- basic package imports
- standard `http.Server` fields line by line
- elementary signal syntax

## `internal/config/config.go`

### What this file owns

- environment parsing
- defaults
- validation
- runtime limits

### What to point at

- heartbeat interval and timeout
- maximum message size
- outbound queue capacity
- rate-limit values
- HTTP address
- graceful drain timeout
- environment-specific origin policy

### What to say

Operational behavior should not be hidden in magic constants across several
files. These values control capacity and failure detection, so they are
configuration with validation, not incidental implementation details.

## `internal/httpapi/router.go`

### What this file owns

- route registration
- liveness and readiness endpoints
- upgrade request validation
- connection authentication
- WebSocket protocol upgrade
- construction and handoff of the client session

### What to point at

- `/healthz`, `/readyz`, and `/ws`
- origin validation
- credential extraction
- identity verification
- upgrader call
- client construction
- lifecycle handoff

### What to say

The HTTP layer establishes the connection. Once the upgrade succeeds, the
realtime session owns the connection. This handler should not implement room
routing or business rules.

### Review questions

- Is identity trusted from an unverified query parameter?
- Is `CheckOrigin` permissive in production?
- Can unauthenticated requests consume upgraded connections?
- Are upgrade failures classified and observable?
- Does readiness prevent new connections during shutdown?

## `internal/realtime/client.go`

### What this file owns

- one WebSocket connection
- client identity and connection metadata
- inbound frame processing
- outbound queue
- socket writes
- heartbeat
- deadlines
- connection-local rate limiting
- close and cleanup initiation

### What to point at

- client fields
- queue capacity
- `readLoop`
- `writeLoop`
- pong handler
- frame-size limit
- command dispatch
- idempotent close path

### What to say

This component owns one session. The key invariant is one reader and one writer
per socket. Other components may enqueue outbound work, but they do not write to
the socket directly.

### Review questions

- Can two goroutines write concurrently?
- Can shutdown close the same channel twice?
- Do all read failures trigger unregistration?
- Is the outbound queue bounded?
- Are write deadlines applied?
- Can malformed input allocate excessive memory?
- Is the queue-full policy explicit?

## `internal/realtime/hub.go`

### What this file owns

- active-client registry
- duplicate-connection policy
- room membership
- register and unregister operations
- room broadcast routing
- private-message routing

### What to point at

- client map key
- room membership representation
- synchronization model
- register replacement policy
- unregister cleanup
- join/leave idempotency
- broadcast recipient iteration
- direct lookup

### What to say

The hub owns shared realtime state. Mutations happen through named operations so
the invariants are reviewable. The hub coordinates delivery, but it does not own
business-domain decisions or direct socket writes.

### Review questions

- Is state modified outside the hub?
- Can a disconnected client remain in a room?
- Is an old duplicate connection able to unregister a newer one?
- Is a socket write performed while the hub lock is held?
- Is sender-included broadcast behavior explicit?
- Can a non-member publish to a room?

## `internal/realtime/message.go`

### What this file owns

- protocol version
- inbound message envelope
- outbound event envelope
- allowed command and event types
- validation
- structured errors and acknowledgements

### What to point at

- message type constants
- request ID
- routing fields
- payload representation
- validation function
- error code taxonomy

### What to say

WebSockets provide transport frames. This file defines the application protocol
carried by those frames. The protocol should be explicit enough that clients,
tests, logs, and documentation agree on the same behavior.

### Review questions

- Are unknown message types rejected?
- Are required fields validated by message type?
- Are errors machine-readable?
- Is protocol evolution possible?
- Are timestamps and IDs generated by the correct side?

## `internal/realtime/rate_limit.go`

### What this file owns

- connection-local message admission
- limit state and reset behavior

### What to point at

- algorithm
- configured capacity
- time source
- allow/deny result
- testability

### What to say

This limiter protects one process and one connection. It is not a global user
quota. I am stating that limitation clearly so the design does not accidentally
claim distributed enforcement.

### Review questions

- Does a fixed window permit boundary bursts?
- Is time injectable for tests?
- Is the limiter checked before expensive parsing or after minimum framing?
- Is the client told why traffic was rejected?

## `internal/observability`

### What this package owns

- structured event fields
- metric instruments
- connection and message measurements

### What to point at

- connection opened/closed
- close reason
- upgrade rejection reason
- queue-full counter
- heartbeat-timeout counter
- active connection gauge
- message processing latency

### What to say

Observability records architectural state transitions. I want to know why
connections close and where messages are rejected without logging sensitive
payloads.

## `tests/websocket_test.go`

### What this file proves

- upgrade and registration
- room join and leave
- broadcast routing
- private delivery
- cleanup after disconnect
- heartbeat timeout
- rate-limit rejection
- slow-client behavior
- graceful shutdown

### What to say

The tests are organized around behavioral guarantees, not private function
coverage. The important question is whether the lifecycle and routing
invariants remain correct under success and failure.

## Recommended walkthrough order

1. Show the repository tree.
2. Open `main.go` to establish the process boundary.
3. Open `router.go` to show connection establishment.
4. Open `client.go` to show lifecycle and socket ownership.
5. Open `hub.go` to show shared-state ownership.
6. Open `message.go` to show the protocol.
7. Return to `client.go` for heartbeat and backpressure.
8. Open `rate_limit.go` for admission control.
9. Open tests to prove behavior.
10. Return to the architecture diagram and restate the boundaries.


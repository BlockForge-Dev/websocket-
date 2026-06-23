# Go WebSocket Implementation Milestones

Every milestone has an objective, scope, demonstration, verification work, and a
definition of done. A milestone is not complete merely because files exist or
the server compiles.

---

## Milestone 0: Repository And Engineering Contract

### Objective

Establish the project as an engineering artifact before implementing runtime
behavior.

### Work

- initialize the Go module
- create the responsibility-based directory structure
- add formatting, linting, test, and run commands
- define configuration conventions
- document the architecture and version-one scope
- document the message contract draft
- add contribution and local setup instructions
- establish CI for formatting, static analysis, and tests

### Demonstration

- clone the repository
- run one setup command
- run formatting, linting, and tests
- show the documented package responsibilities

### Verification

- clean clone works with documented prerequisites
- CI runs on a trivial change
- no generated secrets or local environment files are committed
- Go version is pinned

### Done means

- a new contributor can clone, validate, and understand the repository without
  private instructions
- package boundaries and version-one exclusions are documented
- the default branch passes the initial CI pipeline

### Repository marker

`milestone-00-foundation`

---

## Milestone 1: Process Lifecycle And HTTP Health

### Objective

Create a production-shaped process boundary before adding persistent
connections.

### Work

- load and validate configuration
- construct `http.Server` with explicit timeouts
- add `/healthz`
- add `/readyz`
- handle `SIGINT` and `SIGTERM`
- implement bounded graceful shutdown
- add structured startup and shutdown logs

### Demonstration

- start the process
- call health and readiness endpoints
- send a termination signal
- show an orderly shutdown within the configured deadline

### Verification

- invalid configuration fails before serving traffic
- liveness responds while the process is healthy
- readiness can become false during shutdown
- shutdown does not hang indefinitely

### Done means

- process startup and shutdown are deterministic
- health endpoints have documented semantics
- automated tests cover configuration validation and HTTP health behavior

### Repository marker

`milestone-01-process-lifecycle`

---

## Milestone 2: Authenticated WebSocket Upgrade

### Objective

Establish a validated connection and hand it from the HTTP boundary to the
realtime boundary.

### Work

- register `/ws`
- validate HTTP method and upgrade headers
- enforce an origin policy
- establish a development identity mechanism
- define the production authentication seam
- enforce connection-level admission checks
- upgrade the connection
- generate a connection ID
- return a structured `connection.ready` event

### Demonstration

- connect with a valid development identity
- receive the readiness event
- reject a missing identity
- reject an invalid origin

### Verification

- invalid requests are rejected before upgrade
- production mode does not accept a permissive origin policy
- logs classify accepted and rejected upgrade attempts
- the HTTP handler hands ownership to a client session

### Done means

- a validated client can establish a WebSocket connection
- invalid callers cannot consume a persistent session
- the boundary between HTTP establishment and realtime ownership is visible in
  code and tests

### Repository marker

`milestone-02-websocket-upgrade`

---

## Milestone 3: Client Session, Read Loop, And Write Loop

### Objective

Give each connection one clear lifecycle owner and isolate socket reads from
socket writes.

### Work

- define the client session structure
- create a bounded outbound queue
- implement one read loop
- implement one write loop
- add read and write deadlines
- implement idempotent close behavior
- route all exits through cleanup
- prevent concurrent socket writes

### Demonstration

- connect a client
- send a simple valid frame
- receive a server response through the write queue
- close the client and show cleanup

### Verification

- race detector passes
- only the write loop performs normal socket writes
- read failure closes and unregisters the client
- write failure closes and unregisters the client
- multiple close attempts do not panic

### Done means

- one connection has a deterministic lifecycle
- no component writes directly to the socket outside the session's write path
- connection termination is safe under concurrent failure discovery

### Repository marker

`milestone-03-client-lifecycle`

---

## Milestone 4: Versioned Message Protocol

### Objective

Define the application contract carried over the WebSocket.

### Work

- define protocol version
- define inbound and outbound envelopes
- define command and event types
- add request correlation IDs
- add structured acknowledgements
- add structured errors with stable error codes
- validate required fields by message type
- reject unknown types and malformed payloads

### Demonstration

- send one valid command and receive an acknowledgement
- send malformed JSON and receive a structured error
- send an unknown message type and receive a structured error
- send an unsupported protocol version and show rejection

### Verification

- table-driven validation tests cover each command
- error responses do not expose internal details
- the contract is documented with examples
- server and tests use the same message definitions

### Done means

- every supported client action has an explicit schema and response behavior
- malformed and unknown input fails predictably
- protocol behavior can evolve through versioning

### Repository marker

`milestone-04-message-protocol`

---

## Milestone 5: Hub Registration And State Ownership

### Objective

Introduce one owner for active connections and duplicate-session policy.

### Work

- define the hub
- register clients
- unregister clients
- enforce the one-active-connection-per-user version-one policy
- close or replace duplicate sessions safely
- expose safe lookup operations
- ensure an old session cannot remove a newer replacement

### Demonstration

- connect one user
- connect the same user again
- show the documented replacement behavior
- disconnect the old connection and prove the new one remains registered

### Verification

- concurrent registration and unregistration pass the race detector
- stale-session cleanup cannot delete a replacement
- active-client count is correct
- shutdown can enumerate and close active sessions

### Done means

- the hub is the only owner of the connected-client registry
- duplicate identity behavior is deterministic and tested
- registration state cannot become stale through normal disconnect paths

### Repository marker

`milestone-05-hub-registration`

---

## Milestone 6: Room Membership

### Objective

Add authorized, idempotent grouping of active connections.

### Work

- add room membership state
- implement join
- implement leave
- prevent duplicate membership
- check authorization before state mutation
- remove all memberships during disconnect
- acknowledge join and leave results

### Demonstration

- join a room
- join the same room again
- leave the room
- disconnect while joined and show automatic cleanup
- attempt an unauthorized join

### Verification

- join and leave are idempotent
- disconnected clients do not remain in rooms
- unauthorized joins do not mutate state
- empty rooms are removed or retained according to a documented policy

### Done means

- room membership has one owner
- membership survives duplicate commands without corruption
- disconnect cleanup restores all room invariants

### Repository marker

`milestone-06-room-membership`

---

## Milestone 7: Room Broadcast

### Objective

Fan one authorized room event out to independent recipient queues.

### Work

- validate broadcast commands
- require membership or publishing permission
- define whether the sender receives its own event
- select current recipients safely
- enqueue without holding shared state during socket I/O
- define behavior for empty rooms
- classify unavailable or slow recipients

### Demonstration

- connect three clients
- join two to one room
- broadcast from an authorized member
- prove only expected recipients receive the event
- attempt a broadcast from a non-member

### Verification

- one slow recipient does not block healthy recipients
- no socket write occurs while a hub lock is held
- sender inclusion behavior is tested
- unauthorized broadcasts are rejected
- concurrent join, leave, and broadcast pass the race detector

### Done means

- room fan-out is correct, non-blocking with respect to socket I/O, and governed
  by an explicit authorization and sender policy

### Repository marker

`milestone-07-room-broadcast`

---

## Milestone 8: Private Delivery

### Objective

Route an event to one active identity with honest delivery semantics.

### Work

- validate private-message commands
- apply sender-to-recipient authorization
- look up the active recipient
- enqueue the message
- return accepted, unavailable, or overloaded results
- document that no offline persistence exists

### Demonstration

- send to an online recipient
- send to an offline recipient
- send to a recipient whose queue is unavailable
- show the difference between server acceptance and client acknowledgement

### Verification

- recipient identity cannot be spoofed through payload fields
- offline delivery returns a stable error code
- successful enqueue is not labeled as durable delivery
- private routing remains safe during concurrent disconnect

### Done means

- online private routing works
- offline and overloaded behavior are explicit
- the system makes no false guarantee that a user processed the event

### Repository marker

`milestone-08-private-delivery`

---

## Milestone 9: Heartbeats And Stale-Connection Cleanup

### Objective

Detect connections that have disappeared without a clean close.

### Work

- configure ping interval and pong timeout
- install pong handling
- refresh read deadlines
- send control frames through a safe write path
- close timed-out sessions
- classify heartbeat timeout as a close reason

### Demonstration

- maintain a healthy connection through pong responses
- suppress pong behavior
- show timeout, unregistration, and room cleanup

### Verification

- healthy clients remain connected
- stale clients close within the documented timeout
- heartbeat settings align with proxy timeout documentation
- timeout cleanup updates registry and room state

### Done means

- unreachable clients cannot remain indefinitely in active state
- heartbeat failure enters the standard cleanup path
- heartbeat behavior is configurable, tested, and observable

### Repository marker

`milestone-09-heartbeats`

---

## Milestone 10: Backpressure And Slow-Client Policy

### Objective

Protect the process when recipients consume data more slowly than producers
create it.

### Work

- enforce bounded outbound queues
- use non-blocking or bounded-time enqueue
- define queue-full behavior
- classify slow-client disconnects
- add queue-depth and queue-full metrics
- document product-specific alternatives

### Demonstration

- create one intentionally slow client
- generate enough events to fill its queue
- show that healthy clients continue receiving events
- show the slow client being handled by policy

### Verification

- queue memory is bounded
- hub progress is not tied to a slow socket write
- slow-client policy is deterministic
- queue-full events are logged and measured

### Done means

- one slow connection cannot block a room or create unbounded memory growth
- queue capacity and overflow behavior are documented operational contracts

### Repository marker

`milestone-10-backpressure`

---

## Milestone 11: Rate Limiting And Input Protection

### Objective

Bound the amount of inbound work and memory one connection can consume.

### Work

- enforce maximum frame size
- enforce payload validation limits
- add per-connection message-rate limiting
- define room-join and broadcast limits where needed
- return structured limit errors
- document the limitations of local enforcement

### Demonstration

- send an oversized frame
- exceed the message rate
- show rejection without process instability
- show that compliant clients continue normally

### Verification

- oversized input is rejected before uncontrolled allocation
- limiter behavior is deterministic under a fake clock or controlled time
- rate-limit errors do not terminate healthy sessions unless policy requires it
- local limits are not described as global distributed quotas

### Done means

- connection-level resource consumption is bounded
- abusive or accidental floods produce controlled responses and observable
  evidence

### Repository marker

`milestone-11-resource-protection`

---

## Milestone 12: Behavioral And Failure Testing

### Objective

Prove the externally visible guarantees and important concurrency invariants.

### Work

- add unit tests for protocol validation and limiter behavior
- add integration tests using real WebSocket connections
- add race-detector execution to CI
- test disconnect cleanup
- test duplicate session replacement
- test room broadcast and private delivery
- test heartbeat timeout
- test slow-client behavior
- test graceful shutdown

### Demonstration

- run the complete test suite
- run with the race detector
- show one failure test and explain the invariant it proves

### Verification

- tests are deterministic
- time-dependent behavior uses controlled clocks where practical
- integration tests clean up connections and ports
- CI reports race and static-analysis failures

### Done means

- every documented version-one guarantee has automated evidence
- the highest-risk lifecycle and concurrency paths are tested
- tests describe behavior rather than private implementation details

### Repository marker

`milestone-12-verification`

---

## Milestone 13: Observability

### Objective

Make lifecycle, traffic, pressure, and failure decisions visible in operation.

### Work

- add structured logging
- generate connection IDs
- propagate request IDs
- add active-connection metrics
- count upgrade outcomes
- count messages by type and result
- measure queue pressure and heartbeat timeouts
- measure processing latency
- define safe logging rules

### Demonstration

- connect, join, broadcast, exceed a queue, and disconnect
- show the corresponding logs and metrics
- trace one request ID through command and acknowledgement

### Verification

- sensitive payloads and credentials are not logged
- metric labels avoid unbounded user and room cardinality
- close reasons are classified
- dashboards can distinguish traffic growth from unhealthy connection growth

### Done means

- operators can explain why connections are opening and closing
- pressure and rejection behavior are measurable
- logs and metrics reflect the architecture's state transitions

### Repository marker

`milestone-13-observability`

---

## Milestone 14: Graceful Draining And Deployment Readiness

### Objective

Make rolling deployment behavior safe for long-lived connections.

### Work

- separate liveness and readiness
- stop accepting new connections during shutdown
- publish a server-draining event where appropriate
- allow a bounded drain period
- close remaining sessions with a classified reason
- document client reconnect backoff and jitter
- provide reverse-proxy timeout guidance

### Demonstration

- establish several connections
- begin shutdown
- show readiness becoming false
- show new connections rejected or routed elsewhere
- show existing connections draining and closing within the deadline

### Verification

- shutdown does not accept new work after unready state
- all sessions close before or at the drain deadline
- clients receive a consistent close code where possible
- process exit does not leave tests or goroutines hanging

### Done means

- the service can participate in a rolling deployment without abrupt,
  unexplained connection loss
- operational timeout assumptions are documented

### Repository marker

`milestone-14-graceful-draining`

---

## Milestone 15: Broker Abstraction And Multi-Node Fan-Out

### Objective

Extend routing across processes without moving socket ownership out of each
WebSocket node.

### Work

- define a small broker publish/subscribe contract
- implement Redis Pub/Sub or NATS
- define subjects/channels for room and private events
- prevent duplicate local echo where necessary
- handle broker reconnect and degraded readiness
- add integration tests with two WebSocket nodes
- document ordering and loss behavior

### Demonstration

- run two server nodes
- connect one client to each node
- join both to the same logical room
- publish on node one
- receive on node two
- interrupt the broker and show documented degradation

### Verification

- nodes retain ownership of local sessions
- cross-node fan-out works
- broker outage behavior is controlled and observable
- no unsupported durable-delivery claim is introduced
- duplicate-delivery risks are documented and tested where possible

### Done means

- room and private events can cross node boundaries
- the broker's responsibility and delivery semantics are explicit
- the service reports whether it is ready to accept work when coordination is
  unavailable

### Repository marker

`milestone-15-multi-node`

---

## Milestone 16: Production Review And Release

### Objective

Review the complete version against its stated architecture, threats, and
operational promises.

### Work

- perform architecture review
- perform threat-model review
- run load and soak tests
- profile memory and goroutine behavior
- verify dependency and container scanning
- create runbook and incident scenarios
- publish known limitations
- tag the release

### Demonstration

- show load-test behavior under healthy and slow-client scenarios
- show memory and connection metrics over time
- walk through one operational incident scenario
- compare actual behavior with the documented guarantees

### Verification

- no unbounded connection-owned resource remains
- load test does not reveal goroutine or memory leakage
- security defaults are production-safe
- runbook covers broker outage, connection spike, and high queue pressure
- known limitations are visible in the release notes

### Done means

- the release is supported by code, tests, operational evidence, and honest
  documentation
- version-one promises and exclusions still match actual behavior

### Repository marker

`v1.0.0`


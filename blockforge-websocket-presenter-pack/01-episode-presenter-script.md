# Episode 1 Presenter Script

## Designing a Production-Style Realtime Backend with WebSockets

**Target duration:** 35-45 minutes  
**Audience:** Engineers who already understand Go or another backend language  
**Format:** Architecture slides, focused codebase references, and system reasoning  
**Primary goal:** Give the audience a reusable mental model for designing
stateful realtime delivery systems.

---

## 1. Opening: The Real Engineering Problem

**On screen:** Title slide with the client, realtime gateway, application
services, and message broker visible as one system.

**Present:**

AI can generate a basic WebSocket server in a few seconds. It can create an
upgrade handler, start a read loop, start a write loop, and broadcast JSON
between connected clients.

But that is not the difficult part of building a realtime system.

The difficult part is deciding what the connection represents, who owns its
state, how messages are validated and routed, what happens when a client is
slow, what delivery guarantee the system actually provides, how dead
connections are detected, and what changes when one server becomes ten.

That is the focus of this project.

I am not treating WebSockets as a chat demo, and I am not going to spend this
episode explaining Go syntax or typing boilerplate line by line. I am treating
the WebSocket layer as a stateful delivery system with explicit boundaries,
failure semantics, reliability controls, and a path to scale.

The complete implementation belongs in the repository. In this episode, I am
building the mental model that makes the implementation understandable and
reviewable.

By the end, the important question is not whether we can open a socket. The
important question is whether we can explain exactly what the system promises,
where its state lives, how it behaves under pressure, and how we know it is
correct.

**Point out:**

- Clients are only one edge of the system.
- The WebSocket server is a delivery boundary, not the business domain.
- Application services and the broker remain separate components.

**Transition:**

To design this properly, I want to begin with the problem that persistent
connections are solving.

---

## 2. What Realtime Actually Means

**On screen:** A timeline comparing periodic polling with event-driven delivery.

**Present:**

Realtime does not mean zero latency. In most backend products, we are dealing
with soft realtime behavior: an event should reach the user within a predictable
and useful time window.

A payment dashboard should show a failed transaction without waiting for a
manual refresh. A trading interface should receive price and order updates as
they happen. A blockchain operations screen should surface indexer progress,
chain events, or RPC failures quickly enough for an operator to react.

The important shift is that the server knows an event has happened before the
client does. The delivery model therefore has to support server-initiated
communication.

Traditional HTTP works well when the client knows that it needs information.
The client sends a request, the server returns a response, and that interaction
ends. The model becomes inefficient when the client has to keep asking whether
anything has changed.

Polling creates repeated requests, repeated headers, repeated authentication
work, and a built-in delay between the event and the next poll. Long polling
reduces that delay, but it still recreates the request lifecycle repeatedly.
Server-Sent Events work well for one-directional event streams. WebSockets are
appropriate when both the client and the server need to send messages over one
long-lived connection.

This project is therefore not trying to replace HTTP. HTTP remains the correct
tool for many commands and queries. WebSockets are being introduced as the
delivery path for ongoing, bidirectional, low-latency interaction.

**Point out:**

- Polling spends work even when no event exists.
- WebSockets change the communication lifecycle, not the business event itself.
- HTTP APIs and WebSockets can coexist in the same system.

**Transition:**

Once we choose a persistent connection, the server stops being purely
request-response. It now owns a lifecycle.

---

## 3. How The Connection Starts

**On screen:** HTTP upgrade sequence: client, reverse proxy, HTTP handler,
authentication, protocol upgrade, registered connection.

**Present:**

A WebSocket connection begins as HTTP.

The client sends an upgrade request through the reverse proxy or load balancer.
Before accepting that upgrade, the server has an opportunity to validate the
origin, authenticate the caller, check the requested protocol version, enforce
connection limits, and reject traffic that should not become a persistent
connection.

Only after those checks does the server upgrade the request and create a
long-lived socket.

This gives us an important architectural boundary: the HTTP layer establishes
the connection, but the realtime layer owns it after the upgrade.

The upgrade handler should remain small. It validates the request, establishes
identity, upgrades the transport, constructs a connection session, registers it
with the realtime hub, and then hands lifecycle control to that session.

I do not want authentication, room routing, business logic, and socket I/O all
mixed inside one HTTP handler. That structure may work in a demo, but it becomes
difficult to reason about, test, or change.

For local development, a query parameter can stand in for identity. That is a
development convenience, not a production security model. In production, the
identity should come from something verifiable, such as a signed access token,
a secure session, or a short-lived connection ticket.

**Codebase stop:** `internal/httpapi/router.go`

**Present while showing the file:**

This file is the transport entry point. I am looking for a narrow sequence:
validate the request, establish identity, perform the upgrade, construct the
client session, and register it. If this file starts making room-policy
decisions or processing domain events, the boundary has already begun to leak.

**Point out in code:**

- Route registration for `/healthz` and `/ws`
- The WebSocket upgrader
- Identity extraction
- Construction of the realtime client
- Handoff to the hub and connection lifecycle

**Do not explain:**

- Basic Go handler syntax
- Every error check
- Library boilerplate

**Transition:**

After the upgrade, identity becomes part of a longer-lived session, which means
we need to be precise about what authentication does and does not authorize.

---

## 4. Identity, Sessions, And Authorization

**On screen:** User identity mapped to one or more connection IDs and devices.

**Present:**

Authentication tells the system who owns the connection. Authorization decides
what that connection is allowed to do.

Those are separate checks.

A connection may have been valid when it was established and still be
unauthorized to join a particular room, publish a particular event, or continue
using a permission that has since been revoked.

The design also has to state its session policy. Can one user connect from
multiple devices? Does a new connection replace the previous one? Are
connections tracked by user ID, connection ID, device ID, or all three? What
happens when the access token expires while the socket remains open?

For the first version, I am choosing a deliberately simple policy: one active
connection per user in one process. A new connection for the same user replaces
the old one. That keeps the routing model understandable while we validate the
core lifecycle.

This is not the only correct policy. A production notification system may need
one user to own several device connections. The important point is that the
policy is explicit rather than emerging accidentally from the shape of a map.

**Point out:**

- User identity is not necessarily the same as connection identity.
- Authentication during upgrade does not eliminate per-message authorization.
- Duplicate connection behavior is a product and architecture decision.

**Transition:**

Now that the session has an identity, we can follow the complete connection
lifecycle from registration to cleanup.

---

## 5. The Connection Lifecycle

**On screen:** State machine:
`connecting -> validated -> upgraded -> registered -> active -> closing -> closed`.

**Present:**

In a stateful network system, lifecycle management is as important as message
handling.

The happy path is straightforward. The client connects, the server validates the
request, upgrades the protocol, registers the session, starts the read and write
paths, exchanges messages, and eventually closes the connection.

The real design work appears in the unhappy paths.

The browser can close without a clean shutdown. A mobile device can change
networks. A reverse proxy can terminate an idle connection. The server can miss
a heartbeat, reject a malformed message, detect abusive traffic, or decide that
a slow client is consuming too many resources.

Every exit path must converge on the same cleanup behavior.

Cleanup removes the session from the connected-client registry, removes its room
memberships, stops pending writes, closes the socket, updates metrics, and
prevents stale presence from remaining in memory.

I want connection shutdown to be idempotent. Several goroutines may discover the
same failure at roughly the same time, but the system should still close and
unregister the session exactly once from the perspective of shared state.

**Codebase stop:** `internal/realtime/client.go`

**Present while showing the file:**

This file owns one client session. The important thing is not every line. The
important thing is that the socket lifecycle has one home. I can identify where
reads happen, where writes happen, where deadlines are set, where heartbeat
state is refreshed, and where closure triggers cleanup.

**Point out in code:**

- Client/session fields: identity, socket, send queue, hub reference
- Session constructor
- `readLoop`
- `writeLoop`
- `Close` or shutdown path
- Registration and unregistration relationship

**Transition:**

That session interacts with shared realtime state, so the next question is who
owns that shared state and who is allowed to mutate it.

---

## 6. System Boundaries And State Ownership

**On screen:** Single-node component diagram showing HTTP layer, client
sessions, hub, room index, and application services.

**Present:**

The first implementation runs in one Go process.

The HTTP layer owns routing, health checks, the upgrade request, and initial
validation. A client session owns one socket and its read, write, heartbeat, rate
limit, and shutdown behavior. The realtime hub owns the registry of connected
clients, room membership, registration, unregistration, and message routing.

Application services remain responsible for business rules, persistent state,
and domain authorization.

This boundary matters because the hub can easily become a dumping ground. If
payment rules, trading rules, blockchain logic, and socket routing all enter the
same component, the realtime layer becomes impossible to reuse and risky to
modify.

The hub coordinates delivery. It does not decide whether a payment is valid or
whether an order should execute.

The in-memory state includes connected sessions, user-to-connection mappings,
room membership, bounded outbound queues, heartbeat timestamps, rate-limit
counters, and connection metadata.

Shared mutable state needs a clear owner and a controlled access path. I should
be able to answer who can register a client, who can remove a room membership,
who replaces a duplicate connection, and who closes a session whose queue is
full.

**Codebase stop:** `internal/realtime/hub.go`

**Present while showing the file:**

This is the coordination boundary. The maps show the state model, but the more
important detail is that mutations pass through explicit operations:
registration, unregistration, join, leave, broadcast, and private delivery.

I do not want unrelated parts of the program reaching into these maps directly.
That would make invariants invisible.

**Point out in code:**

- Connected-client registry
- Room-to-members mapping
- Register and unregister operations
- Join and leave operations
- Broadcast and direct-send operations
- Synchronization or single-owner event-loop strategy

**Transition:**

The hub owns routing state, but it should not own socket writes. That separation
is the core of the concurrency model.

---

## 7. Concurrency: One Reader And One Writer

**On screen:** Per-client read loop and write loop separated by a bounded send
queue. The hub feeds the queue but never writes to the socket directly.

**Present:**

Each connection has two independent flows.

The read path receives frames from the client, applies size limits, decodes the
message envelope, validates the command, enforces rate limits and authorization,
and then asks the hub or an application service to act.

The write path receives already-routed outbound messages from a bounded queue
and serializes writes to the socket.

Only the write loop writes to the socket.

That rule gives us one owner for write ordering, write deadlines, serialization
errors, and connection closure caused by failed writes. It also prevents several
goroutines from competing to write to the same connection.

The hub therefore does not broadcast by writing directly to every socket. It
broadcasts by attempting to enqueue a message for each recipient. Each client
then progresses independently.

This distinction is what allows one slow client to be isolated rather than
blocking the entire room.

**Codebase stop:** `internal/realtime/client.go`, then
`internal/realtime/hub.go`

**Present while switching files:**

Here is the boundary in code. The hub calls a queue-oriented operation on the
client. The client write loop is the only place that touches the outbound socket
write. That is the concurrency invariant I care about.

**Point out in code:**

- The bounded `send` channel or queue
- The hub's non-blocking enqueue
- The single socket write location
- Write deadline handling
- Queue-full policy

**Transition:**

Once the transport paths are separated, the next requirement is a protocol. A
WebSocket connection without a defined message contract becomes a collection of
unrelated JSON handlers.

---

## 8. The Message Contract

**On screen:** Structured message envelope with fields highlighted.

```json
{
  "version": "1",
  "type": "room.broadcast",
  "request_id": "req_123",
  "room_id": "payments",
  "recipient_id": null,
  "payload": {},
  "sent_at": "2026-06-18T10:00:00Z"
}
```

**Present:**

The message envelope is the application protocol carried over the WebSocket.

The transport gives us frames. It does not define what those frames mean.

The `type` field identifies the command or event. The `request_id` gives us
correlation across client logs, server logs, acknowledgements, and errors.
Routing fields such as `room_id` and `recipient_id` state the destination.
The payload contains message-specific data. The version gives us a path to evolve
the contract without silently changing semantics for existing clients.

Client commands include joining a room, leaving a room, broadcasting to a room,
sending a private message, and responding to heartbeat behavior where the
library does not handle that automatically.

Server events include connection readiness, membership confirmation, delivered
events, structured errors, and system notices.

Unknown message types are rejected explicitly. Malformed payloads produce
structured errors. Oversized frames are rejected before they consume
uncontrolled memory. The protocol should make failures machine-readable instead
of sending arbitrary strings.

**Codebase stop:** `internal/realtime/message.go`

**Present while showing the file:**

This file is the protocol boundary. I am looking for a small, explicit set of
message types, the shared envelope, validation rules, and structured server
responses. This is also one of the first places I inspect when the client and
server disagree about behavior.

**Point out in code:**

- Message type constants
- Inbound envelope
- Outbound envelope
- Validation function
- Structured error payload
- Protocol version

**Transition:**

With a defined contract, we can follow the first state-changing operation:
joining a room.

---

## 9. Room Membership Flow

**On screen:** Sequence diagram from client to read loop, authorization, hub,
room index, acknowledgement.

**Present:**

The client sends a `room.join` command. The read path validates the envelope and
confirms that the room identifier is present and well formed.

Authentication has already established who owns the connection, but the system
still checks whether that identity is allowed to join this room. Knowing a room
name is not authorization.

Once the policy succeeds, the hub adds the connection to the room membership
index. The operation should be idempotent: joining the same room twice should
not create duplicate membership.

The server then sends a structured acknowledgement and updates the relevant
metrics and logs.

On disconnect, the reverse operation must happen automatically. The client
should not have to send `room.leave` for cleanup to be correct.

**Codebase stop:** Join branch in `internal/realtime/client.go`, then `Join` in
`internal/realtime/hub.go`.

**Present while showing the code:**

The client session interprets the protocol command. The hub performs the state
mutation. That split keeps protocol handling separate from ownership of the room
index.

**Point out in code:**

- Join message validation
- Authorization seam
- Hub join method
- Idempotent membership representation
- Join acknowledgement
- Disconnect cleanup

**Transition:**

Membership gives the hub a routing set. Broadcasting is the operation that puts
that set under pressure.

---

## 10. Broadcast Flow

**On screen:** Sender -> validation -> room lookup -> fan-out -> bounded
recipient queues -> independent write loops.

**Present:**

A broadcast begins as an inbound command from one authenticated connection.

The server validates the message, confirms that the room exists, and applies the
publishing policy. For this design, a connection must be an authorized member of
the room before it can publish into that room.

The hub takes a snapshot or safely iterates over the current members and attempts
to enqueue the event for each recipient.

That is where the semantics need to be explicit.

Does the sender receive its own event? Is ordering guaranteed within one
connection, within one room, or across several server nodes? What happens when a
recipient queue is full? Is this event persisted? Does an empty room count as a
successful publish?

For version one, room delivery is best effort. Messages are not persisted.
Ordering is preserved only within the writes performed by an individual
connection's write loop. A slow recipient does not block the room. If its
bounded queue remains full, the server treats that client as unhealthy and
disconnects it.

Broadcasting is therefore not just looping over sockets. It is a fan-out
operation with authorization, capacity, ordering, and failure semantics.

**Codebase stop:** Broadcast handling in `internal/realtime/client.go`, then
`Broadcast` and `Send` in the hub/client files.

**Present while showing the code:**

I am tracing one event across boundaries: decode, validate, authorize, locate
the room, enqueue for each member, and let each write loop deliver
independently. I am also checking that no slow socket write occurs while shared
hub state is locked.

**Point out in code:**

- Membership check before publish
- Recipient iteration
- Sender-included or sender-excluded policy
- Non-blocking queue operation
- Queue-full handling
- Absence of persistence in version one

**Transition:**

Private delivery uses the same outbound path, but its routing and acknowledgement
semantics are different.

---

## 11. Private Message Flow

**On screen:** Sender -> hub lookup by recipient identity -> recipient queue;
offline path returns a structured response.

**Present:**

A private message targets one authenticated identity rather than a room.

The server validates the recipient, applies any relationship or permission
policy, looks up the active connection, and attempts to enqueue the message.

If the recipient is not connected, version one returns an explicit offline or
unavailable result. It does not store the message for later delivery.

This is where delivery language matters.

Accepted by the server, placed in an outbound queue, written to the network,
acknowledged by the client, and persisted durably are five different states.
The first version only promises best-effort realtime delivery. A successful
enqueue does not prove that the user saw the message.

If the product later requires durable messaging, we will need message IDs,
persistence, acknowledgements, retry rules, deduplication, and replay. Those
features change the system substantially, so I am not pretending they exist in
the first version.

**Codebase stop:** Private-message branch in `client.go`, recipient lookup in
`hub.go`, and acknowledgement/error constructors in `message.go`.

**Point out in code:**

- Recipient lookup
- Offline recipient behavior
- Queue acceptance versus delivery acknowledgement
- Structured sender response
- Authorization extension point

**Transition:**

So far, the happy path works. The next four controls determine whether the
server remains stable when connections and traffic behave badly.

---

## 12. Heartbeats And Dead Connections

**On screen:** Ping/pong timeline with read deadline refresh and timeout cleanup.

**Present:**

A TCP connection can appear open after the remote client is no longer reachable.
That happens with mobile network changes, broken Wi-Fi, proxy timeouts, machine
sleep, or abrupt application termination.

The server uses heartbeat behavior to turn that uncertainty into a bounded
decision.

It periodically sends a ping. A valid pong refreshes the read deadline. If the
deadline expires, the connection is considered dead and enters the same cleanup
path as every other failure.

Heartbeat intervals cannot be chosen in isolation. They must account for load
balancer idle timeouts, reverse proxy settings, mobile behavior, expected
network latency, and the number of active connections. Sending heartbeats too
frequently wastes resources; sending them too slowly leaves stale state around
for too long.

**Codebase stop:** Heartbeat constants and deadline logic in
`internal/realtime/client.go`.

**Point out in code:**

- Pong handler
- Read deadline
- Ping ticker
- Write deadline for control frames
- Timeout-triggered cleanup

**Transition:**

Heartbeats detect clients that disappear. Backpressure protects the system from
clients that remain connected but cannot keep up.

---

## 13. Backpressure And Slow Consumers

**On screen:** Fast producer filling a bounded queue in front of a slow client.

**Present:**

A producer can create events faster than a client can receive them.

If the server allows an unbounded queue, memory usage grows until the process
becomes unstable. If the hub waits synchronously for every socket write, one
slow connection can delay an entire room.

The bounded outbound queue makes capacity explicit.

When that queue is full, the system must choose a policy. It can drop the newest
event, drop the oldest event, degrade low-priority data, persist the event for
later delivery, or disconnect the slow client.

The correct choice depends on the product. Dropping an old market-price update
may be acceptable because a newer price supersedes it. Dropping a payment-state
transition may be unacceptable.

For this version, a full queue causes the connection to close. The server
protects overall availability instead of allowing one client to consume
unbounded resources.

That is not a universal rule. It is an explicit version-one policy.

**Codebase stop:** Send queue creation and non-blocking enqueue in `client.go`;
queue-full response in `hub.go` or the client send method.

**Point out in code:**

- Queue capacity
- Non-blocking `select`
- Full-queue branch
- Connection close path
- Metric/log extension point

**Transition:**

Backpressure limits outbound pressure. Rate limiting and input limits control
what clients can push into the server.

---

## 14. Rate Limiting And Resource Governance

**On screen:** Defense layers: connection limit, frame size, message rate,
bounded queue, deadlines.

**Present:**

A realtime server has to govern every resource that a client can consume.

That includes connection count, frame size, payload size, inbound message rate,
room joins, broadcast frequency, bytes per second, outbound queue capacity, and
the amount of time allowed for reads and writes.

Rate limiting can operate per connection, per authenticated user, per room, or
across the entire service.

The first implementation uses an in-memory per-connection limiter. That is
enough to validate behavior inside one process. It is not a distributed quota.
Once a user can connect through several nodes, enforcement needs a shared
counter, consistent routing, or a clearly defined local-plus-global policy.

The fixed-window algorithm is simple but permits bursts around window
boundaries. A token bucket or sliding-window strategy gives smoother control.
The algorithm matters less than being honest about what it protects.

These controls are not optional polish. They are the server's resource budget.

**Codebase stop:** `internal/realtime/rate_limit.go`, then read limits in
`client.go`.

**Point out in code:**

- Limiter state
- Allow/deny operation
- Where the limiter is called
- Maximum inbound frame size
- Read and write deadlines
- Structured rate-limit error

**Transition:**

With these protections in place, we can state the failure model instead of
hoping every connection follows the happy path.

---

## 15. Failure Model And Delivery Guarantees

**On screen:** Failure matrix with client, server, infrastructure, and
application categories.

**Present:**

I divide failures into four groups.

Client failures include abrupt disconnects, malformed messages, missed
heartbeats, abusive traffic, and slow reads.

Server failures include serialization errors, failed socket writes, memory
pressure, blocked routing, process crashes, and deployment restarts.

Infrastructure failures include proxy timeouts, network partitions, DNS issues,
and eventually broker outages.

Application failures include unauthorized room access, duplicate events,
out-of-order events, stale presence, and incorrect assumptions about delivery.

For every failure, I want six answers: how it is detected, which component owns
the response, whether retry is safe, what is logged, which metric changes, and
what the client experiences.

WebSockets do not automatically provide durable application delivery.

Our version-one guarantee is best-effort transient delivery. The server attempts
to route an event to currently connected clients. It does not persist the event,
replay it after reconnection, or claim that the user processed it.

If we later require at-least-once delivery, we introduce persistence,
acknowledgements, retry, and deduplication. If we require ordered replay, we add
sequence information and a durable event log. Each stronger promise adds state
and failure modes.

**Codebase stop:** Structured errors in `message.go`, cleanup paths in
`client.go`, and routing return values in `hub.go`.

**Point out in code:**

- Errors are classified, not only logged as strings
- Failed writes terminate the session
- Routing reports recipient unavailable or queue full
- No false claim of durable delivery

**Transition:**

Everything so far works because one process can see every connection. The first
distributed boundary appears when we add another WebSocket node.

---

## 16. Why The Single-Node Model Stops Working

**On screen:** User A on node 1 and User B on node 2, with isolated in-memory
registries.

**Present:**

In one process, the hub can see all connected users and all room memberships.

When we add a second node, that assumption breaks.

User A may connect to node one while User B connects to node two. The room map
inside node one contains only its local members. A direct lookup on node one
cannot find a user connected to node two.

This is the central horizontal-scaling problem. The WebSocket nodes own local
connections, but routing decisions may need service-wide information.

Sticky sessions can keep one client returning to the same node, but stickiness
does not solve cross-node broadcast or direct delivery. We still need a
coordination mechanism.

**Point out:**

- Local connection ownership remains with each node.
- In-memory presence becomes partial, not global.
- Cross-node delivery needs a shared routing or messaging layer.

**Transition:**

The scaled architecture introduces a broker, but the broker has a specific job.
It does not replace every responsibility in the system.

---

## 17. Multi-Node Architecture With Redis Or NATS

**On screen:** Load balancer -> WebSocket nodes -> Redis/NATS -> application
services, with local connection sets shown inside each node.

**Present:**

Each WebSocket node continues to own its local sockets, local outbound queues,
heartbeat processing, and local cleanup.

The broker carries events between processes.

When an application service produces a notification, it publishes an event.
When a user on node one sends a room broadcast, node one can publish that event
to a room subject or channel. Every node subscribed to that room can fan the
event out to its local members.

For private delivery, the system can publish by user identity, maintain a
presence-to-node index, or combine both strategies depending on scale and
operational requirements.

Redis Pub/Sub is simple and useful for transient fan-out, but messages disappear
when subscribers are unavailable. Redis Streams adds persistence, consumer
groups, and replay at the cost of more delivery-state complexity.

NATS is designed around subject-based messaging, and JetStream adds durable
streams and replay when those guarantees are required.

The choice is not based on which tool is fashionable. It follows from the
delivery guarantee, replay requirement, expected throughput, failure behavior,
and what the team can operate reliably.

The broker does not own client socket lifecycle. It does not automatically
authorize users. It does not automatically become permanent message history.
Those responsibilities remain explicit.

**Codebase stop:** Show the future interface boundary, not a broker
implementation:

```text
internal/realtime/broker.go
internal/broker/redis.go
internal/broker/nats.go
```

**Present while showing the proposed structure:**

The hub should depend on a small publishing and subscription contract, not on
Redis-specific calls scattered through connection code. That lets us test the
routing model without external infrastructure and choose an implementation
deliberately.

**Transition:**

Adding nodes also changes deployment behavior because these connections live
far longer than normal HTTP requests.

---

## 18. Load Balancing, Readiness, And Connection Draining

**On screen:** Rolling deployment sequence with one node becoming unready,
draining, notifying clients, and shutting down.

**Present:**

WebSocket connections are long-lived, so deployment cannot assume that all
requests finish in a few milliseconds.

The reverse proxy must support protocol upgrades and its idle timeout must align
with heartbeat behavior. A node entering shutdown should first stop accepting
new connections, report itself as unready, allow existing work to drain for a
bounded period, and then close remaining sockets in a controlled way.

Clients must be designed to reconnect with backoff and jitter. Otherwise, a
rolling restart can create a reconnection spike that overloads the remaining
nodes.

Health also needs more than a process-alive check. Liveness answers whether the
process should be restarted. Readiness answers whether the node should receive
new connections. A process can be alive while unable to publish to its broker,
accept new sessions, or maintain its resource budget.

**Codebase stop:** `cmd/server/main.go`, `internal/httpapi/router.go`, and future
health/readiness package.

**Point out in code:**

- Signal handling
- Graceful shutdown context
- Hub shutdown or connection drain hook
- `/healthz` versus `/readyz`
- Configuration for drain timeout

**Transition:**

The architecture also needs a security model that continues beyond the initial
handshake.

---

## 19. Security Model

**On screen:** Security checks at upgrade, message validation, room
authorization, and outbound data filtering.

**Present:**

The connection is authenticated before upgrade, but every meaningful action
still passes through authorization.

The server validates origins for browser clients, terminates TLS, limits message
size, rejects unknown protocol versions, enforces per-user connection policy,
and avoids placing credentials or sensitive payloads in logs.

Room membership and publishing permissions are separate decisions. A user may
be allowed to receive a stream without being allowed to publish into it.

Token expiry also needs a policy. The connection can be closed when the token
expires, periodically revalidated, or renewed through a controlled mechanism.
Leaving this undefined creates permanently authorized sockets.

A persistent authenticated connection is not permanent authorization for every
future action.

**Codebase stop:** Authentication seam in `httpapi/router.go`; authorization
interfaces around join, broadcast, and private-message handling.

**Point out in code:**

- Origin policy is configurable and restrictive in production
- Identity is derived from verified credentials
- Authorization is called per operation
- Sensitive values are excluded from logs

**Transition:**

Security tells us who may act. Observability tells us what the system is
actually doing after deployment.

---

## 20. Observability And Operational Evidence

**On screen:** Metrics dashboard, structured log event, and trace path.

**Present:**

For a stateful service, observability has to describe both traffic and
connection health.

I want metrics for active connections, attempted and rejected upgrades,
messages received and sent by type, room membership, outbound queue depth,
dropped or rejected messages, slow-client disconnections, heartbeat timeouts,
message-processing latency, and broker publish failures.

Structured logs should carry a connection ID, safe user identifier, message
type, room ID where relevant, request ID, server node, and a classified failure
reason.

Tracing becomes useful when an event crosses application services, a broker, a
WebSocket node, and finally the client delivery boundary.

The goal is not to log everything. The goal is to make the important state
transitions and failure decisions observable without leaking sensitive data or
creating excessive cardinality.

**Codebase stop:** Proposed `internal/observability` package and call sites in
the upgrade handler, hub, and client lifecycle.

**Point out in code:**

- Connection-open and connection-close events
- Classified close reason
- Queue-full counter
- Heartbeat timeout counter
- Message latency measurement
- Correlation using request and connection IDs

**Transition:**

At this point, the architecture is broad enough that scope discipline becomes
important. Version one should prove the core lifecycle without pretending to
solve every distributed problem.

---

## 21. Deliberate Version-One Scope

**On screen:** Two columns: Included Now and Deferred Deliberately.

**Present:**

Version one runs in one Go process.

It includes the HTTP-to-WebSocket upgrade, verified or development identity,
client registration, one-reader and one-writer lifecycle, room membership,
broadcasts, private delivery, heartbeat handling, bounded outbound queues, rate
limiting, structured messages, and graceful cleanup.

It deliberately excludes offline message persistence, guaranteed delivery,
message history, distributed presence, multi-region routing, end-to-end
encryption, and production broker integration.

That does not make version one incomplete. It makes its promise precise.

The purpose of the first implementation is to prove connection lifecycle,
state ownership, routing, resource protection, and failure cleanup. Once those
invariants are correct, distributed coordination becomes a meaningful next
step instead of a layer hiding basic lifecycle defects.

**Point out:**

- The scope is bounded by a delivery promise, not by a random feature list.
- Deferred features are documented, not forgotten.
- The architecture leaves extension points without implementing them early.

**Transition:**

Now I can map the entire mental model into the Go repository without walking
through every line.

---

## 22. Codebase Map

**On screen:** Repository tree.

```text
go-server/
  cmd/server/main.go
  internal/config/config.go
  internal/httpapi/router.go
  internal/realtime/client.go
  internal/realtime/hub.go
  internal/realtime/message.go
  internal/realtime/rate_limit.go
  internal/observability/
  tests/
```

**Present:**

The repository structure follows runtime responsibilities.

`cmd/server` owns process startup, dependency construction, signal handling, and
shutdown.

`internal/config` parses and validates runtime configuration.

`internal/httpapi` owns HTTP routes, health behavior, authentication at the
connection boundary, and the WebSocket upgrade.

`internal/realtime/client` owns one connection session, including its read loop,
write loop, heartbeat, deadlines, send queue, and closure.

`internal/realtime/hub` owns shared realtime state: connected clients, room
membership, registration, unregistration, broadcast routing, and private
routing.

`internal/realtime/message` owns the application protocol carried over the
socket.

`internal/realtime/rate_limit` owns the first local traffic-control policy.

`internal/observability` will provide logging and metrics without placing vendor
details inside the domain of the realtime components.

The package boundaries are not there to make the repository look complicated.
They exist so that every important responsibility has one clear home.

**Point out:**

- Main wires dependencies; it does not contain routing logic.
- The HTTP handler creates sessions; it does not own them.
- The client owns socket I/O.
- The hub owns shared routing state.
- The protocol is defined independently of the transport loops.

**Transition:**

The repository will be built through milestones, and every milestone has a
behavioral definition of done rather than merely a list of files.

---

## 23. Milestone Roadmap Summary

**On screen:** Milestone timeline from foundation to distributed scale.

**Present:**

The implementation roadmap begins with the process boundary and moves inward.

First, the repository, configuration, server lifecycle, and health behavior are
established. Then the upgrade path and session identity are added. After that,
the read and write lifecycles are separated, the message protocol is defined,
and the hub becomes the owner of registration and room state.

Broadcast and private delivery come only after those ownership rules are clear.
Heartbeat, backpressure, rate limiting, and input limits then harden the
connection path.

Testing proves lifecycle invariants and failure behavior. Observability makes
those behaviors visible. Graceful draining makes deployment safe. Broker
integration finally extends the routing model across nodes.

Every milestone ends with something demonstrable, testable, and documented.
The repository should tell us not only what was implemented, but what guarantee
was added and what remains deliberately unsupported.

**Codebase stop:** Open `04-milestone-roadmap.md` from this pack or the matching
repository documentation.

**Transition:**

That roadmap gives us a controlled path from one working connection to a
distributed realtime service.

---

## 24. Closing

**On screen:** Full architecture, returning to the opening diagram.

**Present:**

A WebSocket server is not simply an HTTP endpoint that remains open.

It is a stateful delivery system. It owns long-lived connections, concurrent
read and write paths, transient routing state, bounded queues, heartbeat-based
failure detection, cleanup, and eventually cross-node coordination.

The design becomes manageable when responsibilities are explicit.

The HTTP layer establishes the connection. The client session owns socket I/O.
The hub owns shared routing state. Application services own business rules. The
broker coordinates events between processes. Observability provides evidence
that these boundaries behave as intended.

The first implementation deliberately promises best-effort realtime delivery
inside one process. It does not pretend to provide persistence or guaranteed
delivery. Those capabilities will be introduced only when the product requires
them and the architecture can state their cost clearly.

The code is available in the repository, but the code is not the central lesson.
The central lesson is how to reason about responsibilities, state ownership,
contracts, capacity, failure semantics, and operational behavior before trusting
an implementation.

In the next episode, I will walk through the Go codebase and trace one
connection, one broadcast, one private message, and one failure from entry to
cleanup. I will not explain every line. I will show how the architecture appears
in code and where the important invariants are enforced.

Think in systems. Design with intention. Build and review like an engineer.


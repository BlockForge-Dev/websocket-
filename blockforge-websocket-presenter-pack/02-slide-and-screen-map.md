# Slide And Screen Map

This document is the recording sequence. It identifies when to remain on a
diagram, when to open the repository, what to highlight, and when to return to a
full-system view.

## Editing convention

Use a simple visual rhythm:

1. Show the full slide.
2. Zoom into the component currently being discussed.
3. Highlight one flow or boundary.
4. Switch to the relevant code file for 30-90 seconds.
5. Return to the diagram and summarize the invariant.

Avoid leaving a large code file on screen while discussing architecture that is
not visible in that file.

## Sequence

| Segment | Primary visual | Codebase cutaway | Key visual focus |
|---|---|---|---|
| Opening thesis | Full system context | None | WebSocket layer is one part of a larger system |
| Realtime meaning | Polling vs push timeline | None | Server-initiated delivery |
| Upgrade | HTTP upgrade sequence | `internal/httpapi/router.go` | HTTP establishes; realtime owns |
| Identity | User, devices, connection IDs | Upgrade/auth seam | Authentication vs authorization |
| Lifecycle | Connection state machine | `internal/realtime/client.go` | All exits converge on cleanup |
| Boundaries | Single-node architecture | `internal/realtime/hub.go` | State ownership |
| Concurrency | Read loop, queue, write loop | `client.go`, `hub.go` | One writer per socket |
| Protocol | Message envelope | `internal/realtime/message.go` | Explicit contract and errors |
| Room join | Join sequence | `client.go`, `hub.go` | Validate, authorize, mutate, acknowledge |
| Broadcast | Fan-out sequence | Broadcast and send methods | Queue fan-out, not direct writes |
| Private delivery | Direct route sequence | Hub lookup and response | Enqueue is not confirmed delivery |
| Heartbeat | Ping/pong timeline | Client deadline logic | Dead connection detection |
| Backpressure | Slow-client queue | Send queue and full branch | Capacity is explicit |
| Rate limiting | Resource-defense layers | `rate_limit.go` | Per-connection v1 limitation |
| Failure model | Failure matrix | Error and cleanup paths | Detect, own, retry, log, measure, expose |
| Single-node limit | Two isolated nodes | None | Local state is partial at scale |
| Distributed model | Broker architecture | Future broker interface | Local socket ownership remains local |
| Deployment | Connection draining | `cmd/server/main.go` | Readiness and graceful shutdown |
| Security | Layered checks | Auth and authorization seams | Persistent auth is not permanent permission |
| Observability | Metrics/logs/traces | Observability call sites | Operational evidence |
| V1 scope | Included/deferred columns | README/architecture decision | Precise promise |
| Repository map | File tree | Repository explorer | Responsibilities map to packages |
| Milestones | Roadmap timeline | Milestone document | Behavioral definitions of done |
| Closing | Full system context | None | Restate the ownership model |

## Code highlighting rules

- Increase editor font size before recording.
- Hide minimap and unrelated sidebars.
- Highlight no more than 10-20 lines at a time.
- Start from the function signature, then point to the invariant.
- Do not scroll rapidly through files.
- Use repository search to jump to exact symbols.
- Keep code cutaways short enough that the architecture remains the main story.

## Repeated verbal pattern for code cutaways

Use this three-part structure:

1. **Responsibility:** "This file owns one client session."
2. **Invariant:** "Only this write loop writes to the socket."
3. **Consequence:** "That prevents concurrent writes and isolates slow clients."

This sounds more deliberate than narrating syntax.


# Client Session Lifecycle

Milestone 3 moves ownership of an upgraded socket from the HTTP bootstrap into
`internal/realtime.Client`.

## Ownership

Each accepted connection has one client session containing:

- authenticated user identity
- generated connection ID
- the WebSocket connection
- one bounded outbound queue
- one read loop
- one write loop
- read and write deadlines
- an idempotent close path
- a cleanup callback seam

The HTTP handler validates and upgrades the request, then hands ownership to
the realtime session. It does not continue reading from or writing to the
socket.

## Concurrency invariant

```text
peer -> read loop -> outbound queue -> write loop -> peer
```

The read loop is the only normal socket reader. The write loop is the only
normal socket writer. Other components call `Send`, which copies and enqueues
the frame without touching socket I/O.

## Bounded queue

`BLOCKFORGE_OUTBOUND_QUEUE_CAPACITY` sets the fixed number of pending frames
owned by one connection. Enqueue is non-blocking. A full queue closes the
session through the standard cleanup path.

Milestone 10 expands this into the complete observable slow-client policy.

## Deadlines

- `BLOCKFORGE_WS_READ_TIMEOUT` bounds a stalled read and is refreshed after
  each successfully received frame.
- `BLOCKFORGE_WS_WRITE_TIMEOUT` bounds each socket write.

Milestone 9 replaces simple idle read timeout behavior with heartbeat-aware
deadline refresh.

## Cleanup

Read failure, write failure, queue overflow, deadline failure, peer closure, and
explicit closure all converge on `Client.Close`.

`Close` is guarded by `sync.Once`, so concurrent failure discovery:

- closes the done signal once;
- closes the socket once;
- runs the cleanup callback once;
- emits one classified session-close log.

The cleanup callback unregisters the exact client from the hub. Identity-safe
unregistration prevents a replaced session from deleting its successor.

## Protocol handoff

The read loop passes text-frame payloads to the versioned protocol decoder. Valid
commands and structured errors both return through the same bounded outbound
queue and single write loop.
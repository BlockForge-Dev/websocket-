# Authenticated WebSocket Upgrade

## Endpoint

```text
GET /ws
```

A request becomes a persistent connection only after every pre-upgrade check
succeeds.

## Validation order

1. The server must be ready.
2. The request must contain valid WebSocket upgrade headers.
3. A browser `Origin` must exactly match a configured origin.
4. Authentication must establish a non-empty identity.
5. Connection capacity must be available.
6. The HTTP connection is upgraded.
7. A cryptographically random connection ID is generated.
8. Ownership is handed to the realtime client session.

Rejected requests do not consume a persistent WebSocket session.

## Development identity

Development and test environments accept:

```text
ws://localhost:8080/ws?user_id=user_123
```

The `user_id` query parameter is a local-development convenience. It is not a
production authentication mechanism.

## Production authentication seam

The HTTP boundary depends on the `httpapi.Authenticator` interface:

```go
type Authenticator interface {
    Authenticate(*http.Request) (Principal, error)
}
```

Production defaults to a rejecting implementation. A deployment must inject a
verifier backed by signed access tokens, secure sessions, or short-lived
connection tickets. Query-string identity is never enabled automatically in
production.

## Origin policy

`BLOCKFORGE_ALLOWED_ORIGINS` is a comma-separated exact allowlist. Wildcards are
rejected. Production refuses to start without an explicit allowlist.

Requests without an `Origin` header are accepted because non-browser clients do
not always send one. Browser requests with an origin must match the allowlist.

## Admission policy

`BLOCKFORGE_MAX_CONNECTIONS` bounds simultaneous accepted sessions in one
process. When capacity is exhausted, the server rejects the request before
upgrade with `503 Service Unavailable`.

## Ready event

An accepted connection receives:

```json
{
  "version": "1",
  "type": "connection.ready",
  "event_id": "evt_conn_...",
  "payload": {
    "connection_id": "conn_...",
    "user_id": "user_123"
  },
  "sent_at": "2026-06-19T10:00:00Z"
}
```

The realtime client session sends this event through its bounded outbound queue.
The session then owns the read loop, write loop, deadlines, and cleanup.

## Upgrade log events

- `websocket_upgrade_accepted`
- `websocket_upgrade_rejected`
- `websocket_connection_id_failed`
- `websocket_session_closed`

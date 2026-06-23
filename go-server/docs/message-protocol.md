# Versioned Message Protocol

Status: implemented for Milestone 4.

## Transport

- WebSocket text frames containing UTF-8 JSON
- one complete protocol message per frame
- protocol version `1`
- RFC 3339 UTC timestamps
- binary frames are rejected with a structured protocol error

## Client command envelope

```json
{
  "version": "1",
  "type": "room.join",
  "request_id": "req_123",
  "room_id": "payments",
  "recipient_id": null,
  "payload": {},
  "sent_at": "2026-06-19T10:00:00Z"
}
```

Fields:

- `version`: required and currently must equal `1`.
- `type`: required and must be a supported command.
- `request_id`: required, client-generated, and returned on the response.
- `room_id`: required for room membership and broadcast commands.
- `recipient_id`: required for private delivery.
- `payload`: required as a JSON object for broadcast and private delivery.
- `sent_at`: optional client metadata; it is not trusted as server time.

Identity comes from the authenticated connection. No payload field can override
its sender identity.

## Supported commands

| Type | Required fields | Milestone 4 response |
|---|---|---|
| `room.join` | `request_id`, `room_id` | validated acknowledgement |
| `room.leave` | `request_id`, `room_id` | validated acknowledgement |
| `room.broadcast` | `request_id`, `room_id`, object `payload` | validated acknowledgement |
| `private.send` | `request_id`, `recipient_id`, object `payload` | validated acknowledgement |

The acknowledgement proves protocol acceptance only. Room mutation and message
routing are introduced in later milestones and will extend command handling
without changing the envelope.

## Acknowledgement

```json
{
  "version": "1",
  "type": "command.ack",
  "request_id": "req_123",
  "event_id": "evt_...",
  "payload": {
    "status": "accepted",
    "command_type": "room.join"
  },
  "sent_at": "2026-06-19T10:00:00Z"
}
```

`accepted` means the command passed protocol validation and was accepted by the
current command layer. It does not mean durable storage, recipient processing,
or human visibility.

## Structured error

```json
{
  "version": "1",
  "type": "error",
  "request_id": "req_123",
  "event_id": "evt_...",
  "payload": {
    "code": "invalid_message",
    "message": "room_id is required for room.join.",
    "retryable": false
  },
  "sent_at": "2026-06-19T10:00:00Z"
}
```

Implemented error codes:

- `invalid_json`: the text frame is not exactly one valid JSON object.
- `invalid_message`: required fields or frame type are invalid.
- `unsupported_version`: the protocol version is not supported.
- `unknown_message_type`: the command type is not supported.

Protocol errors do not expose stack traces, credentials, topology, or private
authorization details. They do not close an otherwise healthy session; the
client may correct the command and continue.

## Server event envelope

All server messages share:

- `version`
- `type`
- optional `request_id`
- server-generated `event_id`
- typed `payload`
- authoritative server `sent_at`

Currently emitted event types:

- `connection.ready`
- `command.ack`
- `error`

Room, private-delivery, and draining events are introduced by their owning
milestones.

## Evolution rules

- Unknown command types are rejected explicitly.
- Unsupported protocol versions are rejected explicitly.
- Unknown JSON fields are ignored for forward-compatible additive changes.
- Existing field meaning cannot change silently within version `1`.
- A breaking contract change requires a new protocol version.
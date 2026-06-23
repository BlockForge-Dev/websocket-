# Room Membership

This document details the design, state representation, authorization model, and lifecycle policies of rooms.

## Overview

A **room** is a process-local logical grouping of active WebSocket client sessions. The room membership model allows clients to join and leave rooms dynamically.

```text
       Hub (Room Owner)
      /       \
 [Room A]   [Room B]
   /   \       |
Conn1 Conn2  Conn3
```

## State Ownership and Model

Room membership state is exclusively owned by the `internal/realtime.Hub`. It is represented in memory using two tracking registries to allow fast lookups and disconnect cleanup:

1. **`rooms`**: `map[string]map[string]*Client`  
   Maps a `roomID` to a set of active connections (represented by connection ID and client pointer). This is used for routing events to all room members (used in Milestone 7: Room Broadcast).
   
2. **`clientRooms`**: `map[string]map[string]struct{}`  
   Maps a `connectionID` to the set of rooms that the connection is currently joined to. This allows $O(1)$ lookup of a client's memberships when they disconnect, avoiding scanning all rooms.

## Core Policies

### Idempotency
- **Join**: If a client is already a member of a room, subsequent join commands for that room return success immediately without duplicating membership or corrupting internal states.
- **Leave**: If a client attempts to leave a room they are not currently a member of, the command succeeds silently.

### Authorization
Room mutations check authorization using the `realtime.Authorizer` interface before any state changes:
```go
type Authorizer interface {
    AuthorizeJoin(userID, roomID string) error
}
```
- If `AuthorizeJoin` returns an error, the state is unchanged, and the client receives a structured error message with the code `"unauthorized"`.

### Disconnect Cleanup
When a WebSocket session terminates (whether due to a network error, client closure, or duplicate replacement):
1. The client's connection is unregistered from the `Hub`.
2. All memberships held by that connection ID are immediately removed.
3. If any room becomes empty as a result, it is cleaned up.

### Empty Rooms Policy
- Empty rooms (rooms with `0` active members) are **removed immediately** from the `Hub` memory map.
- This prevents memory leaks from short-lived or abandoned rooms.

## Message Protocols

### 1. Join Room (`room.join`)

#### Command
```json
{
  "version": "1",
  "type": "room.join",
  "request_id": "req_101",
  "room_id": "payments"
}
```

#### Successful Response
```json
{
  "version": "1",
  "type": "command.ack",
  "request_id": "req_101",
  "event_id": "evt_...",
  "payload": {
    "status": "accepted",
    "command_type": "room.join"
  },
  "sent_at": "2026-06-22T10:00:00Z"
}
```

#### Unauthorized Response
```json
{
  "version": "1",
  "type": "error",
  "request_id": "req_101",
  "event_id": "evt_...",
  "payload": {
    "code": "unauthorized",
    "message": "forbidden",
    "retryable": false
  },
  "sent_at": "2026-06-22T10:00:00Z"
}
```

### 2. Leave Room (`room.leave`)

#### Command
```json
{
  "version": "1",
  "type": "room.leave",
  "request_id": "req_102",
  "room_id": "payments"
}
```

#### Response
```json
{
  "version": "1",
  "type": "command.ack",
  "request_id": "req_102",
  "event_id": "evt_...",
  "payload": {
    "status": "accepted",
    "command_type": "room.leave"
  },
  "sent_at": "2026-06-22T10:00:00Z"
}
```

# Hub Registration and State Ownership

Milestone 5 introduces `internal/realtime.Hub` as the only owner of the active
client registry.

## Registry model

Version one stores:

```text
authenticated user ID -> active client session
```

One user has at most one active session in one process. Connection identity
remains separate from user identity.

## Registration

Registration performs this sequence:

1. Lock the registry.
2. Read the currently active session for the user.
3. Install the new session.
4. Unlock the registry.
5. Close the replaced session, if one existed.

The new session is visible before the old session is closed. Closing happens
outside the registry lock so socket cleanup cannot block registry progress.

## Duplicate-session policy

A new connection replaces the previous connection for the same user. The old
session closes with reason `duplicate_session_replaced`.

This is the explicit version-one policy. Supporting multiple devices later
requires changing the user registry value from one session to a set of
sessions.

## Stale-cleanup protection

Unregistration receives the specific client session that is closing. It
removes the registry entry only if:

- the user still exists in the registry;
- the registered client is the same client instance;
- the registered connection ID still matches.

Therefore, cleanup from an old replaced connection cannot remove the newer
replacement.

## Safe reads

The hub exposes:

- `Lookup(userID)` for the current active session;
- `Count()` for active users;
- `Snapshot()` for safe enumeration without exposing the registry map.

These operations use read locking and never allow callers to mutate registry
state directly.

## Shutdown

Process shutdown:

1. withdraws readiness;
2. snapshots and closes all active sessions;
3. lets each session run normal unregister cleanup;
4. performs bounded HTTP shutdown.

This prevents long-lived handlers from keeping the HTTP shutdown path blocked.
Milestone 14 adds a graceful WebSocket drain notice and deadline policy.

## Concurrency evidence

Tests cover:

- duplicate replacement;
- stale-session cleanup;
- active-client counts;
- lookup behavior;
- concurrent registration and unregistration;
- closing every session during shutdown;
- a real two-connection duplicate-user flow.

The CI pipeline also runs the complete suite with Go's race detector.

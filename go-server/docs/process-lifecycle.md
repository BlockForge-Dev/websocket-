# Process Lifecycle and Health

## Startup

The process performs startup in this order:

1. Load environment configuration.
2. Validate every address and duration.
3. Open the TCP listener.
4. install `SIGINT` and `SIGTERM` handling.
5. Construct the HTTP server with explicit timeouts.
6. Mark readiness true and begin serving.

Invalid configuration or a listener error prevents the process from serving
traffic and produces a structured error log.

## Health endpoints

### `GET /healthz`

Returns `200 OK` while the process is running:

```json
{"status":"ok"}
```

This is the liveness endpoint. A failed liveness check means the process should
be restarted.

### `GET /readyz`

Returns `200 OK` only while the process is accepting work:

```json
{"status":"ready"}
```

Before serving and during shutdown it returns `503 Service Unavailable`:

```json
{"status":"not_ready"}
```

Readiness is withdrawn before graceful shutdown begins.

## Shutdown

`SIGINT` and `SIGTERM` cancel the process context. The server then:

1. marks itself unready;
2. starts graceful HTTP shutdown;
3. waits up to `BLOCKFORGE_SHUTDOWN_TIMEOUT`;
4. force-closes the HTTP server if graceful shutdown exceeds that deadline;
5. emits a structured stopped event and exits.

Milestone 14 extends this process with WebSocket connection draining. Milestone
1 establishes the bounded process-level lifecycle that later sessions will
join.

## Structured lifecycle events

- `configuration_loaded`
- `server_started`
- `server_shutdown_started`
- `server_shutdown_failed`
- `server_stopped`
- `server_exit_failed`

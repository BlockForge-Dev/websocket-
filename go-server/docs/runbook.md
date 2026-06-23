# Operational Runbook

This runbook describes operational instructions, recovery steps, and health verification for the BlockForge Labs realtime service.

---

## 1. Broker (NATS) Outages

### Symptoms
- **HTTP Readiness Endpoint (`/readyz`)**: Returns `503 Service Unavailable` with body `{"status":"not_ready"}`.
- **Log Activity**: Repeated warnings of type `broker_nats_disconnected` with the connection failure message.
- **Metric spikes**: Increment of `/metrics` `broker_publish_errors` counter.

### Fallback Behavior
- Active client sessions continue to operate.
- Local broadcasts within a single node continue to route immediately to local room members.
- Cross-node routing for broadcasts and private delivery is degraded/unavailable.
- NATS client will attempt to automatically reconnect in the background (infinite retry with 2s wait).

### Recovery Validation
- On successful reconnection to NATS JetStream:
  - The node logs `broker_nats_reconnected`.
  - The `/readyz` endpoint recovers immediately and returns `200 OK` (`{"status":"ready"}`).
  - Cross-node event delivery automatically resumes.

---

## 2. Connection Spikes

### Scaling Out
- Increase replication count in your orchestrator (Kubernetes deployment replicas).
- Load balancers (Nginx / HAProxy) should use round-robin or least-connections distribution. Since all nodes share state via the NATS Broker, clients can connect to any replica.

### Proxy and OS Tuning

#### Nginx Configuration Limits
Ensure Nginx supports high concurrent connections. Set `/etc/nginx/nginx.conf`:
```nginx
events {
    worker_connections 65535; # Set high enough for expected connection spikes
}
```

#### OS Limits (ulimit)
WebSocket server nodes and proxies require high file descriptor limits. Set the system-wide limits in `/etc/security/limits.conf`:
```text
* soft nofile 65535
* hard nofile 65535
```
Apply running settings:
```bash
ulimit -n 65535
```

---

## 3. High Outbound Queue Pressure and Slow Clients

### How it is Handled
- Outbound queues are bounded (default: 64 messages, configurable via `BLOCKFORGE_OUTBOUND_QUEUE_CAPACITY`).
- If a client consumes messages slower than they are produced:
  1. The client's queue fills up.
  2. The server detects the overflow, logs a warning `websocket_private_recipient_overloaded` or `websocket_broadcast_recipient_unavailable`.
  3. The connection is terminated with standard close reason `slow_client_disconnect` to prevent memory exhaustion on the server.

### Troubleshooting
- Monitor the metric `queue_full_disconnects` under `/metrics`. A high rate of increases indicates many slow clients or network saturation.
- Look up logs for `websocket_private_recipient_overloaded`. Identify if specific User IDs are consistently falling behind.
- **Remediation**:
  - Increase queue capacity via `BLOCKFORGE_OUTBOUND_QUEUE_CAPACITY` if clients are legitimately slow but need more buffer.
  - Optimize client-side message parsing (move heavy processing off the main WebSocket reader thread).

---

## 4. Key Alerting Rules

Alert on the following metrics:
- **Liveness/Readiness**: Alert if `/readyz` returns anything other than `200 OK` for > 30 seconds (indicates server shutdown or broker disconnect).
- **Queue Disconnects**: Alert if `queue_full_disconnects` rate exceeds 5 per minute (indicates client network bottlenecks or server frame saturation).
- **Broker Errors**: Alert if the rate of increase of `broker_publish_errors` is > 0 (indicates NATS write issues).

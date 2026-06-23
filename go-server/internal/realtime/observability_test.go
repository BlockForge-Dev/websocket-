package realtime

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/blockforgelabs/go-websocket/internal/observability"
)

type mockAuthorizer struct{}

func (mockAuthorizer) AuthorizeJoin(userID, roomID string) error {
	if roomID == "forbidden" {
		return errors.New("forbidden room join")
	}
	return nil
}

func (mockAuthorizer) AuthorizePublish(userID, roomID string) error {
	if roomID == "forbidden_pub" {
		return errors.New("forbidden publish")
	}
	return nil
}

func (mockAuthorizer) AuthorizePrivateSend(senderID, recipientID string) error {
	if recipientID == "forbidden_recipient" {
		return errors.New("forbidden private send")
	}
	return nil
}

func TestObservabilityMetricsAndLogPropagation(t *testing.T) {
	hub := NewHub(discardLogger(), mockAuthorizer{}, 5)
	sock := newMockReadSocket()

	var logsBuffer bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logsBuffer, nil))

	client := newClient(sock, ClientOptions{
		ConnectionID:  "conn_obs",
		UserID:        "user_obs",
		QueueCapacity: 5,
		Logger:        logger,
		Hub:           hub,
	})
	hub.Register(client)

	go client.Run()
	defer client.Close("test_complete")

	// Drain connection.ready event
	<-sock.writes

	// Fetch current snapshot before we do actions
	metricsBefore := observability.DefaultMetrics.Snapshot()

	// 1. Send successful room.join
	joinCmd := `{"version":"1","type":"room.join","request_id":"req_join_ok","room_id":"room_1"}`
	sock.reads <- readResult{msgType: websocket.TextMessage, payload: []byte(joinCmd)}
	select {
	case <-sock.writes:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for join ack")
	}

	// 2. Send rejected room.join (fails auth)
	joinUnauth := `{"version":"1","type":"room.join","request_id":"req_join_fail","room_id":"forbidden"}`
	sock.reads <- readResult{msgType: websocket.TextMessage, payload: []byte(joinUnauth)}
	select {
	case <-sock.writes:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for join error")
	}

	// 3. Send successful room.leave
	leaveCmd := `{"version":"1","type":"room.leave","request_id":"req_leave_ok","room_id":"room_1"}`
	sock.reads <- readResult{msgType: websocket.TextMessage, payload: []byte(leaveCmd)}
	select {
	case <-sock.writes:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for leave ack")
	}

	// 4. Send rejected room.broadcast (fails auth)
	broadcastUnauth := `{"version":"1","type":"room.broadcast","request_id":"req_broad_fail","room_id":"forbidden_pub","payload":{"text":"hello"}}`
	sock.reads <- readResult{msgType: websocket.TextMessage, payload: []byte(broadcastUnauth)}
	select {
	case <-sock.writes:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for broadcast error")
	}

	// 5. Send rejected private.send (fails auth)
	privateUnauth := `{"version":"1","type":"private.send","request_id":"req_priv_fail","recipient_id":"forbidden_recipient","payload":{"text":"hello"}}`
	sock.reads <- readResult{msgType: websocket.TextMessage, payload: []byte(privateUnauth)}
	select {
	case <-sock.writes:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for private send error")
	}

	// Wait briefly to ensure metrics are recorded in background closure callbacks
	time.Sleep(100 * time.Millisecond)

	metricsAfter := observability.DefaultMetrics.Snapshot()

	// Check type increments
	joinCountDiff := metricsAfter.InboundMessageCounts.RoomJoin - metricsBefore.InboundMessageCounts.RoomJoin
	if joinCountDiff != 2 {
		t.Fatalf("expected 2 joins, got %d", joinCountDiff)
	}

	leaveCountDiff := metricsAfter.InboundMessageCounts.RoomLeave - metricsBefore.InboundMessageCounts.RoomLeave
	if leaveCountDiff != 1 {
		t.Fatalf("expected 1 leave, got %d", leaveCountDiff)
	}

	broadcastCountDiff := metricsAfter.InboundMessageCounts.RoomBroadcast - metricsBefore.InboundMessageCounts.RoomBroadcast
	if broadcastCountDiff != 1 {
		t.Fatalf("expected 1 broadcast, got %d", broadcastCountDiff)
	}

	privateCountDiff := metricsAfter.InboundMessageCounts.PrivateSend - metricsBefore.InboundMessageCounts.PrivateSend
	if privateCountDiff != 1 {
		t.Fatalf("expected 1 private send, got %d", privateCountDiff)
	}

	// Check outcome results
	successDiff := metricsAfter.InboundMessageResults.Success - metricsBefore.InboundMessageResults.Success
	if successDiff != 2 { // Only join_ok and leave_ok were successful
		t.Fatalf("expected 2 successes, got %d", successDiff)
	}

	errorDiff := metricsAfter.InboundMessageResults.Error - metricsBefore.InboundMessageResults.Error
	if errorDiff != 3 { // join_fail, broad_fail, and priv_fail failed
		t.Fatalf("expected 3 errors, got %d", errorDiff)
	}

	// Check queue pressure and latency updates
	if metricsAfter.ProcessingLatency.Count <= metricsBefore.ProcessingLatency.Count {
		t.Fatalf("expected processing latency count to increase")
	}
	if metricsAfter.ProcessingLatency.TotalNs <= metricsBefore.ProcessingLatency.TotalNs {
		t.Fatalf("expected processing latency total ns to increase")
	}

	// Verify structured warning logs for command rejections contain correct metadata
	logOutput := logsBuffer.String()
	if !strings.Contains(logOutput, "websocket_command_rejected") {
		t.Fatalf("expected log output to contain 'websocket_command_rejected'")
	}
	if !strings.Contains(logOutput, "req_join_fail") {
		t.Fatalf("expected log output to contain request ID 'req_join_fail'")
	}
	if !strings.Contains(logOutput, "req_broad_fail") {
		t.Fatalf("expected log output to contain request ID 'req_broad_fail'")
	}
	if !strings.Contains(logOutput, "req_priv_fail") {
		t.Fatalf("expected log output to contain request ID 'req_priv_fail'")
	}
	if !strings.Contains(logOutput, "forbidden room join") {
		t.Fatalf("expected log output to contain reason 'forbidden room join'")
	}
}

func TestQueuePressureTracking(t *testing.T) {
	// Capture initial max queue depth
	metricsBefore := observability.DefaultMetrics.Snapshot()

	sock := newMockReadSocket()
	client := newClient(sock, ClientOptions{
		ConnectionID:  "conn_pressure",
		UserID:        "user_pressure",
		QueueCapacity: 10,
		Logger:        discardLogger(),
	})

	// Fill queue to depth of 3
	client.Send(websocket.TextMessage, []byte(`{"text":"hello"}`))
	client.Send(websocket.TextMessage, []byte(`{"text":"hello"}`))
	client.Send(websocket.TextMessage, []byte(`{"text":"hello"}`))

	metricsAfter := observability.DefaultMetrics.Snapshot()
	if metricsAfter.MaxQueueDepth < metricsBefore.MaxQueueDepth || metricsAfter.MaxQueueDepth < 3 {
		t.Fatalf("expected MaxQueueDepth to be at least 3, got %d", metricsAfter.MaxQueueDepth)
	}
}

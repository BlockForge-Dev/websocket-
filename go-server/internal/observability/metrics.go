package observability

import (
	"sync/atomic"
	"time"
)

// Metrics holds the count of various events in the system.
type Metrics struct {
	ActiveConnections    atomic.Int64
	UpgradesAttempted    atomic.Int64
	UpgradesRejected     atomic.Int64
	HeartbeatTimeouts    atomic.Int64
	QueueFullDisconnects atomic.Int64
	MessagesReceived     atomic.Int64
	MessagesSent         atomic.Int64

	InboundJoinCount      atomic.Int64
	InboundLeaveCount     atomic.Int64
	InboundBroadcastCount atomic.Int64
	InboundPrivateCount   atomic.Int64
	InboundUnknownCount   atomic.Int64

	InboundSuccessCount atomic.Int64
	InboundErrorCount   atomic.Int64

	MaxQueueDepth atomic.Int64

	ProcessingLatencyTotalNs atomic.Int64
	ProcessingLatencyCount   atomic.Int64

	BrokerPublishCount  atomic.Int64
	BrokerReceiveCount  atomic.Int64
	BrokerPublishErrors atomic.Int64
}

// DefaultMetrics is the global metrics registry.
var DefaultMetrics = &Metrics{}

// MetricsSnapshot represents a point-in-time snapshot of system metrics.
type MetricsSnapshot struct {
	ActiveConnections    int64 `json:"active_connections"`
	UpgradesAttempted    int64 `json:"upgrades_attempted"`
	UpgradesRejected     int64 `json:"upgrades_rejected"`
	HeartbeatTimeouts    int64 `json:"heartbeat_timeouts"`
	QueueFullDisconnects int64 `json:"queue_full_disconnects"`
	MessagesReceived     int64 `json:"messages_received"`
	MessagesSent         int64 `json:"messages_sent"`

	InboundMessageCounts struct {
		RoomJoin      int64 `json:"room.join"`
		RoomLeave     int64 `json:"room.leave"`
		RoomBroadcast int64 `json:"room.broadcast"`
		PrivateSend   int64 `json:"private.send"`
		Unknown       int64 `json:"unknown"`
	} `json:"inbound_message_counts"`

	InboundMessageResults struct {
		Success int64 `json:"success"`
		Error   int64 `json:"error"`
	} `json:"inbound_message_results"`

	MaxQueueDepth int64 `json:"max_queue_depth"`

	ProcessingLatency struct {
		TotalNs   int64 `json:"total_ns"`
		Count     int64 `json:"count"`
		AverageNs int64 `json:"average_ns"`
	} `json:"processing_latency"`

	BrokerPublishCount  int64 `json:"broker_publish_count"`
	BrokerReceiveCount  int64 `json:"broker_receive_count"`
	BrokerPublishErrors int64 `json:"broker_publish_errors"`
}

// Snapshot returns a thread-safe MetricsSnapshot.
func (m *Metrics) Snapshot() MetricsSnapshot {
	var s MetricsSnapshot
	s.ActiveConnections = m.ActiveConnections.Load()
	s.UpgradesAttempted = m.UpgradesAttempted.Load()
	s.UpgradesRejected = m.UpgradesRejected.Load()
	s.HeartbeatTimeouts = m.HeartbeatTimeouts.Load()
	s.QueueFullDisconnects = m.QueueFullDisconnects.Load()
	s.MessagesReceived = m.MessagesReceived.Load()
	s.MessagesSent = m.MessagesSent.Load()

	s.InboundMessageCounts.RoomJoin = m.InboundJoinCount.Load()
	s.InboundMessageCounts.RoomLeave = m.InboundLeaveCount.Load()
	s.InboundMessageCounts.RoomBroadcast = m.InboundBroadcastCount.Load()
	s.InboundMessageCounts.PrivateSend = m.InboundPrivateCount.Load()
	s.InboundMessageCounts.Unknown = m.InboundUnknownCount.Load()

	s.InboundMessageResults.Success = m.InboundSuccessCount.Load()
	s.InboundMessageResults.Error = m.InboundErrorCount.Load()

	s.MaxQueueDepth = m.MaxQueueDepth.Load()

	s.ProcessingLatency.TotalNs = m.ProcessingLatencyTotalNs.Load()
	s.ProcessingLatency.Count = m.ProcessingLatencyCount.Load()
	if s.ProcessingLatency.Count > 0 {
		s.ProcessingLatency.AverageNs = s.ProcessingLatency.TotalNs / s.ProcessingLatency.Count
	}

	s.BrokerPublishCount = m.BrokerPublishCount.Load()
	s.BrokerReceiveCount = m.BrokerReceiveCount.Load()
	s.BrokerPublishErrors = m.BrokerPublishErrors.Load()

	return s
}

// IncrementActiveConnections increments the count of active sessions.
func IncrementActiveConnections() {
	DefaultMetrics.ActiveConnections.Add(1)
}

// DecrementActiveConnections decrements the count of active sessions.
func DecrementActiveConnections() {
	DefaultMetrics.ActiveConnections.Add(-1)
}

// IncrementUpgradesAttempted increments connection upgrade attempts.
func IncrementUpgradesAttempted() {
	DefaultMetrics.UpgradesAttempted.Add(1)
}

// IncrementUpgradesRejected increments rejected connection upgrades.
func IncrementUpgradesRejected() {
	DefaultMetrics.UpgradesRejected.Add(1)
}

// IncrementHeartbeatTimeouts increments heartbeat timeout count.
func IncrementHeartbeatTimeouts() {
	DefaultMetrics.HeartbeatTimeouts.Add(1)
}

// IncrementQueueFullDisconnects increments queue-full disconnections.
func IncrementQueueFullDisconnects() {
	DefaultMetrics.QueueFullDisconnects.Add(1)
}

// IncrementMessagesReceived increments inbound message count.
func IncrementMessagesReceived() {
	DefaultMetrics.MessagesReceived.Add(1)
}

// IncrementMessagesSent increments outbound message count.
func IncrementMessagesSent() {
	DefaultMetrics.MessagesSent.Add(1)
}

// IncrementInboundType increments counter for specific command types.
func IncrementInboundType(msgType string) {
	switch msgType {
	case "room.join":
		DefaultMetrics.InboundJoinCount.Add(1)
	case "room.leave":
		DefaultMetrics.InboundLeaveCount.Add(1)
	case "room.broadcast":
		DefaultMetrics.InboundBroadcastCount.Add(1)
	case "private.send":
		DefaultMetrics.InboundPrivateCount.Add(1)
	default:
		DefaultMetrics.InboundUnknownCount.Add(1)
	}
}

// IncrementInboundResult increments success or error outcome counter.
func IncrementInboundResult(success bool) {
	if success {
		DefaultMetrics.InboundSuccessCount.Add(1)
	} else {
		DefaultMetrics.InboundErrorCount.Add(1)
	}
}

// UpdateMaxQueueDepth sets the maximum queue depth if it is higher than the current value.
func UpdateMaxQueueDepth(depth int64) {
	for {
		current := DefaultMetrics.MaxQueueDepth.Load()
		if depth <= current {
			break
		}
		if DefaultMetrics.MaxQueueDepth.CompareAndSwap(current, depth) {
			break
		}
	}
}

// RecordProcessingLatency adds duration to total processing latency metric.
func RecordProcessingLatency(d time.Duration) {
	ns := d.Nanoseconds()
	if ns <= 0 {
		ns = 1
	}
	DefaultMetrics.ProcessingLatencyTotalNs.Add(ns)
	DefaultMetrics.ProcessingLatencyCount.Add(1)
}

// IncrementBrokerPublishCount increments the count of published broker events.
func IncrementBrokerPublishCount() {
	DefaultMetrics.BrokerPublishCount.Add(1)
}

// IncrementBrokerReceiveCount increments the count of received broker events.
func IncrementBrokerReceiveCount() {
	DefaultMetrics.BrokerReceiveCount.Add(1)
}

// IncrementBrokerPublishErrors increments the count of broker publish errors.
func IncrementBrokerPublishErrors() {
	DefaultMetrics.BrokerPublishErrors.Add(1)
}

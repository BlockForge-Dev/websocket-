package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type Config struct {
	ServerURL   string
	NumClients  int
	Duration    time.Duration
	MsgRate     float64 // messages per second per client
	Concurrency int     // connection throttle concurrency
}

type Stats struct {
	ConnectionsAttempted   int64
	ConnectionsEstablished int64
	ConnectionErrors       int64
	MessagesSent           int64
	MessagesAcked          int64
	BroadcastsReceived     int64
	ErrorsReceived         int64
	MinLatency             int64 // microseconds
	MaxLatency             int64 // microseconds
	TotalLatency           int64 // sum of microsecond latencies (for average)
	LatencyCount           int64
}

func main() {
	serverFlag := flag.String("server", "ws://localhost:8080/ws", "WebSocket server URL")
	clientsFlag := flag.Int("clients", 100, "Number of concurrent clients")
	durationFlag := flag.Duration("duration", 10*time.Second, "Soak duration")
	rateFlag := flag.Float64("rate", 1.0, "Broadcast rate per client per second")
	concurrencyFlag := flag.Int("concurrency", 20, "Connection establishment throttle")
	flag.Parse()

	cfg := Config{
		ServerURL:   *serverFlag,
		NumClients:  *clientsFlag,
		Duration:    *durationFlag,
		MsgRate:     *rateFlag,
		Concurrency: *concurrencyFlag,
	}

	fmt.Printf("Starting load test with config:\n")
	fmt.Printf("  Server:      %s\n", cfg.ServerURL)
	fmt.Printf("  Clients:     %d\n", cfg.NumClients)
	fmt.Printf("  Duration:    %s\n", cfg.Duration)
	fmt.Printf("  Msg Rate:    %.2f msg/sec/client\n", cfg.MsgRate)
	fmt.Printf("  Concurrency: %d concurrent connections\n\n", cfg.Concurrency)

	stats := &Stats{
		MinLatency: 9999999999,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	throttle := make(chan struct{}, cfg.Concurrency)

	// Create clients
	for i := 0; i < cfg.NumClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			throttle <- struct{}{}
			defer func() { <-throttle }()

			runClient(ctx, id, cfg, stats)
		}(i)
	}

	// Wait for duration then cancel context
	time.Sleep(cfg.Duration)
	fmt.Println("Duration reached. Stopping clients...")
	cancel()

	wg.Wait()

	// Output report
	printReport(stats, cfg)
}

func runClient(ctx context.Context, id int, cfg Config, stats *Stats) {
	atomic.AddInt64(&stats.ConnectionsAttempted, 1)

	u, err := url.Parse(cfg.ServerURL)
	if err != nil {
		atomic.AddInt64(&stats.ConnectionErrors, 1)
		return
	}

	q := u.Query()
	q.Set("user_id", fmt.Sprintf("loadtest_%d", id))
	u.RawQuery = q.Encode()

	dialer := websocket.Dialer{
		HandshakeTimeout: 5 * time.Second,
	}

	header := http.Header{}
	header.Set("Origin", "http://localhost:3000")

	conn, _, err := dialer.Dial(u.String(), header)
	if err != nil {
		atomic.AddInt64(&stats.ConnectionErrors, 1)
		return
	}
	defer conn.Close()

	atomic.AddInt64(&stats.ConnectionsEstablished, 1)

	// Read connection.ready
	var ready struct {
		Type string `json:"type"`
	}
	if err := conn.ReadJSON(&ready); err != nil {
		return
	}
	if ready.Type != "connection.ready" {
		return
	}

	// Join room
	roomJoinMsg := map[string]any{
		"version":    "1",
		"type":       "room.join",
		"request_id": fmt.Sprintf("join_%d", id),
		"room_id":    "loadtest_room",
	}
	if err := conn.WriteJSON(roomJoinMsg); err != nil {
		return
	}

	// Read join ACK
	var joinAck struct {
		Type string `json:"type"`
	}
	if err := conn.ReadJSON(&joinAck); err != nil {
		return
	}

	// Start reader loop
	sentTimes := make(map[string]time.Time)
	var mu sync.Mutex

	readCtx, cancelRead := context.WithCancel(ctx)
	defer cancelRead()

	go func() {
		defer cancelRead()
		for {
			var msg map[string]any
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}

			typ, _ := msg["type"].(string)
			switch typ {
			case "command.ack":
				reqID, _ := msg["request_id"].(string)
				mu.Lock()
				tSend, ok := sentTimes[reqID]
				if ok {
					delete(sentTimes, reqID)
				}
				mu.Unlock()

				if ok {
					latency := time.Since(tSend).Microseconds()
					atomic.AddInt64(&stats.MessagesAcked, 1)
					recordLatency(stats, latency)
				}
			case "room.broadcast":
				atomic.AddInt64(&stats.BroadcastsReceived, 1)
			case "error":
				atomic.AddInt64(&stats.ErrorsReceived, 1)
			}
		}
	}()

	// Start writer loop
	if cfg.MsgRate <= 0 {
		<-readCtx.Done()
		return
	}

	ticker := time.NewTicker(time.Duration(float64(time.Second) / cfg.MsgRate))
	defer ticker.Stop()

	msgSeq := 0
	for {
		select {
		case <-readCtx.Done():
			return
		case <-ticker.C:
			msgSeq++
			reqID := fmt.Sprintf("msg_%d_%d", id, msgSeq)
			mu.Lock()
			sentTimes[reqID] = time.Now()
			mu.Unlock()

			payload, _ := json.Marshal(map[string]any{
				"seq":  msgSeq,
				"uuid": fmt.Sprintf("client_%d", id),
			})

			broadCmd := map[string]any{
				"version":    "1",
				"type":       "room.broadcast",
				"request_id": reqID,
				"room_id":    "loadtest_room",
				"payload":    json.RawMessage(payload),
			}

			atomic.AddInt64(&stats.MessagesSent, 1)
			if err := conn.WriteJSON(broadCmd); err != nil {
				return
			}
		}
	}
}

func recordLatency(stats *Stats, lat int64) {
	atomic.AddInt64(&stats.LatencyCount, 1)
	atomic.AddInt64(&stats.TotalLatency, lat)

	for {
		currentMin := atomic.LoadInt64(&stats.MinLatency)
		if lat >= currentMin {
			break
		}
		if atomic.CompareAndSwapInt64(&stats.MinLatency, currentMin, lat) {
			break
		}
	}

	for {
		currentMax := atomic.LoadInt64(&stats.MaxLatency)
		if lat <= currentMax {
			break
		}
		if atomic.CompareAndSwapInt64(&stats.MaxLatency, currentMax, lat) {
			break
		}
	}
}

func printReport(stats *Stats, cfg Config) {
	fmt.Printf("\n==================================================\n")
	fmt.Printf("                   LOAD TEST REPORT               \n")
	fmt.Printf("==================================================\n")
	fmt.Printf("Connections Attempted:      %d\n", stats.ConnectionsAttempted)
	fmt.Printf("Connections Established:    %d\n", stats.ConnectionsEstablished)
	fmt.Printf("Connection Failures:       %d\n", stats.ConnectionErrors)
	fmt.Printf("Messages Sent:             %d\n", stats.MessagesSent)
	fmt.Printf("Messages Acknowledged:     %d\n", stats.MessagesAcked)
	fmt.Printf("Broadcast Events Received: %d\n", stats.BroadcastsReceived)
	fmt.Printf("Errors Received:           %d\n", stats.ErrorsReceived)

	if stats.LatencyCount > 0 {
		avg := float64(stats.TotalLatency) / float64(stats.LatencyCount)
		fmt.Printf("\nLatency Statistics:\n")
		fmt.Printf("  Average Latency:         %.2f ms\n", avg/1000.0)
		fmt.Printf("  Min Latency:             %.2f ms\n", float64(stats.MinLatency)/1000.0)
		fmt.Printf("  Max Latency:             %.2f ms\n", float64(stats.MaxLatency)/1000.0)
	} else {
		fmt.Printf("\nNo latency statistics recorded (no messages acknowledged).\n")
	}
	fmt.Printf("==================================================\n")
}

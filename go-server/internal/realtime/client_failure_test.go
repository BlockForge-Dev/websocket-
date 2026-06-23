package realtime

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type failingWriteSocket struct {
	closed    chan struct{}
	closeOnce sync.Once
}

func newFailingWriteSocket() *failingWriteSocket {
	return &failingWriteSocket{closed: make(chan struct{})}
}

func (*failingWriteSocket) SetReadDeadline(time.Time) error   { return nil }
func (*failingWriteSocket) SetWriteDeadline(time.Time) error  { return nil }
func (*failingWriteSocket) SetPongHandler(func(string) error) {}
func (*failingWriteSocket) SetReadLimit(int64)                {}

func (s *failingWriteSocket) ReadMessage() (int, []byte, error) {
	<-s.closed
	return 0, nil, errors.New("closed")
}

func (*failingWriteSocket) WriteMessage(int, []byte) error {
	return errors.New("forced write failure")
}

func (s *failingWriteSocket) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func TestClientWriteFailureEntersCleanupPath(t *testing.T) {
	t.Parallel()

	closed := make(chan string, 1)
	client := newClient(newFailingWriteSocket(), ClientOptions{
		ConnectionID:  "conn_write_failure",
		UserID:        "user_write_failure",
		QueueCapacity: 2,
		ReadTimeout:   time.Second,
		WriteTimeout:  time.Second,
		Logger:        discardLogger(),
		OnClose: func(reason string) {
			closed <- reason
		},
	})

	done := make(chan struct{})
	go func() {
		client.Run()
		close(done)
	}()

	select {
	case reason := <-closed:
		if reason != "write_failed" {
			t.Fatalf("close reason = %q, want write_failed", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("write failure did not enter cleanup")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("client Run did not return after write failure")
	}
}

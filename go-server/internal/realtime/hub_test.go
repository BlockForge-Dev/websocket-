package realtime

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestHubReplacesDuplicateSessionAndIgnoresStaleCleanup(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	oldClient := hubTestClient(hub, "conn_old", "user_1", nil)
	newClient := hubTestClient(hub, "conn_new", "user_1", nil)

	hub.Register(oldClient)
	hub.Register(newClient)

	current, ok := hub.Lookup("user_1")
	if !ok || current != newClient {
		t.Fatalf("lookup = %p, %v; want new client %p", current, ok, newClient)
	}
	if hub.Count() != 1 {
		t.Fatalf("count = %d, want 1", hub.Count())
	}
	if hub.Unregister(oldClient) {
		t.Fatal("stale client removed its replacement")
	}
	current, ok = hub.Lookup("user_1")
	if !ok || current != newClient {
		t.Fatal("new client disappeared after stale cleanup")
	}

	newClient.Close("test_complete")
	if hub.Count() != 0 {
		t.Fatalf("count after active close = %d, want 0", hub.Count())
	}
}

func TestHubLookupAndActiveCount(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	first := hubTestClient(hub, "conn_1", "user_1", nil)
	second := hubTestClient(hub, "conn_2", "user_2", nil)

	hub.Register(first)
	hub.Register(second)
	if hub.Count() != 2 {
		t.Fatalf("count = %d, want 2", hub.Count())
	}
	if client, ok := hub.Lookup("user_2"); !ok || client != second {
		t.Fatalf("lookup user_2 = %p, %v; want %p", client, ok, second)
	}

	first.Close("test_complete")
	if hub.Count() != 1 {
		t.Fatalf("count after close = %d, want 1", hub.Count())
	}
	second.Close("test_complete")
}

func TestHubCloseAllEnumeratesAndClosesActiveSessions(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	var closeCalls atomic.Int32
	for index := 0; index < 5; index++ {
		client := hubTestClient(
			hub,
			fmt.Sprintf("conn_%d", index),
			fmt.Sprintf("user_%d", index),
			func() { closeCalls.Add(1) },
		)
		hub.Register(client)
	}

	if len(hub.Snapshot()) != 5 {
		t.Fatalf("snapshot length = %d, want 5", len(hub.Snapshot()))
	}
	hub.CloseAll("server_shutdown")
	if hub.Count() != 0 {
		t.Fatalf("count after CloseAll = %d, want 0", hub.Count())
	}
	if calls := closeCalls.Load(); calls != 5 {
		t.Fatalf("close callbacks = %d, want 5", calls)
	}
}

func TestHubConcurrentRegistrationAndUnregistration(t *testing.T) {
	t.Parallel()

	hub := NewHub(discardLogger(), AllowAllAuthorizer{}, 10)
	var wait sync.WaitGroup
	for index := 0; index < 100; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			userID := fmt.Sprintf("user_%d", index%10)
			client := hubTestClient(hub, fmt.Sprintf("conn_%d", index), userID, nil)
			hub.Register(client)
			client.Close("test_complete")
		}()
	}
	wait.Wait()
	hub.CloseAll("test_cleanup")
	if hub.Count() != 0 {
		t.Fatalf("count after concurrent cleanup = %d, want 0", hub.Count())
	}
}

func hubTestClient(hub *Hub, connectionID, userID string, afterClose func()) *Client {
	var client *Client
	client = NewClient(nil, ClientOptions{
		ConnectionID:  connectionID,
		UserID:        userID,
		QueueCapacity: 1,
		Logger:        discardLogger(),
		OnClose: func(string) {
			hub.Unregister(client)
			if afterClose != nil {
				afterClose()
			}
		},
	})
	return client
}

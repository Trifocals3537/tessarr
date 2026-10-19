package manager

import (
	"errors"
	"os"
	"testing"

	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

func TestClientRetryCannotBypassPendingAdmin(t *testing.T) {
	for _, method := range []string{"entry", "queue"} {
		for _, flag := range []bool{false, true} {
			t.Run(method+map[bool]string{false: "-false", true: "-true"}[flag], func(t *testing.T) {
				s := newLifecycleTestStorage(t)
				q := newLifecycleTestQueue(s, newEntryLifecycle())
				e, path := addLifecycleTestEntry(t, q, "pending-admin")
				e.ActiveProvider = "fake"
				e.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "active"}}
				if err := q.Update(e); err != nil {
					t.Fatal(err)
				}
				main := *e
				main.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "library"}}
				if err := s.AddOrUpdate(&main); err != nil {
					t.Fatal(err)
				}
				// A preserving benign intent must stay preserving even when retried true.
				if _, err := s.PrepareQBitQueuedDeletion(e.InfoHash, false); err != nil {
					t.Fatal(err)
				}
				offline := errors.New("offline")
				calls := 0
				clients := xsync.NewMap[string, debrid.Client]()
				clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(string) error { calls++; return offline }})
				m := &Manager{storage: s, queue: q, clients: clients, logger: zerolog.Nop()}
				var err error
				if method == "entry" {
					err = m.DeleteEntry(e.InfoHash, true)
				} else {
					err = m.DeleteQueueEntry(e.InfoHash, true)
				}
				if !errors.Is(err, offline) {
					t.Fatal(err)
				}
				before := calls
				if err = m.DeleteQBitQueueEntry(e.InfoHash, flag); err == nil {
					t.Error("client bypassed pending admin cleanup")
				}
				if calls != before {
					t.Error("client executed admin provider cleanup")
				}
				if _, err = os.Stat(path); err != nil {
					t.Error(err)
				}
				intents, err := s.QueuedDeletionIntents()
				if err != nil || len(intents) != 1 {
					t.Fatalf("lost intent: %v %v", intents, err)
				}
			})
		}
	}
}

func TestQueueOnlyAdminCannotAdoptBenignClientIntent(t *testing.T) {
	s := newLifecycleTestStorage(t)
	q := newLifecycleTestQueue(s, newEntryLifecycle())
	e, path := addLifecycleTestEntry(t, q, "queue-only-admin")
	e.ActiveProvider = "fake"
	e.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "active"}}
	if err := q.Update(e); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PrepareQBitQueuedDeletion(e.InfoHash, true); err != nil {
		t.Fatal(err)
	}
	calls := 0
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(string) error { calls++; return errors.New("offline") }})
	m := &Manager{storage: s, queue: q, clients: clients, logger: zerolog.Nop()}
	if err := m.DeleteQueueEntry(e.InfoHash, true); err == nil {
		t.Error("admin falsely reported successful provider deletion")
	}
	if calls != 0 {
		t.Error("immutable scope widened")
	}
	if _, err := os.Stat(path); err != nil {
		t.Error(err)
	}
	intents, err := s.QueuedDeletionIntents()
	if err != nil || len(intents) != 1 {
		t.Fatalf("lost intent: %v %v", intents, err)
	}
}

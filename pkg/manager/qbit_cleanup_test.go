package manager

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

func TestQBitRecoveryReopenPreservesMainAndInactivePlacements(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db")
	store, err := storage.NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	queue := newLifecycleTestQueue(store, newEntryLifecycle())
	e, path := addLifecycleTestEntry(t, queue, "reopen-qbit")
	e.State = storage.EntryStateStalledDL
	e.DownloadUncached = true
	e.HandoffReason = "terminal"
	e.Category = "sonarr"
	e.ClientEndpoint = "host:8282"
	e.ActiveProvider = "fake"
	e.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "active"}, "other": {Provider: "other", ID: "inactive"}}
	if err = queue.Update(e); err != nil {
		t.Fatal(err)
	}
	if err = store.AuthorizeUncachedHandoff(e); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PrepareQBitQueuedDeletion(e.InfoHash, true); err != nil {
		t.Fatal(err)
	}
	main := *e
	main.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "library"}}
	if err = store.AddOrUpdate(&main); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = storage.NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	clients := xsync.NewMap[string, debrid.Client]()
	var ids []string
	clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(id string) error { ids = append(ids, id); return nil }})
	clients.Store("other", &placementDeleteTestProvider{name: "other", delete: func(id string) error { t.Errorf("inactive placement deleted %s", id); return nil }})
	m := &Manager{storage: store, queue: newLifecycleTestQueue(store, newEntryLifecycle()), clients: clients, logger: zerolog.Nop()}
	residual, fatal := m.recoverQueuedDeletions()
	if residual != nil || fatal != nil {
		t.Fatalf("reopen recovery: %v %v", residual, fatal)
	}
	if len(ids) != 1 || ids[0] != "active" {
		t.Fatal(ids)
	}
	if _, err = store.Get(e.InfoHash); err != nil {
		t.Fatalf("main lost: %v", err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("staging retained: %v", err)
	}
	if err = m.DeleteQBitQueueEntry(e.InfoHash, true); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatal("duplicate deleted provider")
	}
}

func TestQBitCleanupScope(t *testing.T) {
	for _, mode := range []string{"cached", "uncached-success", "false", "missing-proof", "failed", "collision", "retry", "recovery"} {
		t.Run(mode, func(t *testing.T) {
			store := newLifecycleTestStorage(t)
			queue := newLifecycleTestQueue(store, newEntryLifecycle())
			entry, path := addLifecycleTestEntry(t, queue, "qbit-"+mode)
			entry.ActiveProvider = "fake"
			entry.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "active"}, "other": {Provider: "other", ID: "inactive"}}
			entry.DownloadUncached = mode != "cached"
			entry.Category = "sonarr"
			entry.ClientEndpoint = "tessarr.example:8282"
			entry.State = storage.EntryStateStalledDL
			entry.HandoffReason = "terminal"
			if mode == "cached" || mode == "uncached-success" {
				entry.State = storage.EntryStatePausedUP
				entry.IsComplete = true
				entry.Progress = 1
				entry.HandoffReason = ""
			}
			if err := queue.Update(entry); err != nil {
				t.Fatal(err)
			}
			main := *entry
			main.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "library"}}
			if mode == "collision" {
				main.Providers["fake"].ID = "active"
			}
			if err := store.AddOrUpdate(&main); err != nil {
				t.Fatal(err)
			}
			clients := xsync.NewMap[string, debrid.Client]()
			var ids []string
			fail := mode == "retry" || mode == "recovery"
			providerErr := errors.New("provider not found in account listing")
			clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(id string) error {
				ids = append(ids, id)
				if fail {
					return providerErr
				}
				return nil
			}})
			clients.Store("other", &placementDeleteTestProvider{name: "other", delete: func(id string) error { t.Fatalf("inactive placement deleted: %s", id); return nil }})
			m := &Manager{storage: store, queue: queue, clients: clients, logger: zerolog.Nop()}
			authorized := mode == "failed" || mode == "collision" || mode == "retry" || mode == "recovery"
			if authorized {
				if err := store.AuthorizeUncachedHandoff(entry); err != nil {
					t.Fatal(err)
				}
			}
			err := m.DeleteQBitQueueEntry(entry.InfoHash, mode != "false")
			if mode == "collision" {
				if err == nil {
					t.Fatal("shared library placement must require manual review")
				}
				if len(ids) != 0 {
					t.Fatal(ids)
				}
				return
			}
			if fail {
				if !errors.Is(err, providerErr) {
					t.Fatalf("error=%v", err)
				}
				intents, e := store.QueuedDeletionIntents()
				if e != nil || len(intents) != 1 {
					t.Fatalf("intents=%v %v", intents, e)
				}
				if intents[0].PlacementScope != storage.QueuePlacementScopeTransferOnly || len(intents[0].PlacementEntries) != 1 {
					t.Fatalf("unsafe intent: %+v", intents[0])
				}
				fail = false
				if mode == "recovery" {
					residual, fatal := m.recoverQueuedDeletions()
					if residual != nil || fatal != nil {
						t.Fatalf("recovery: %v %v", residual, fatal)
					}
				} else {
					if err = m.DeleteQBitQueueEntry(entry.InfoHash, true); err != nil {
						t.Fatal(err)
					}
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Get(entry.InfoHash); err != nil {
				t.Fatalf("main entry lost: %v", err)
			}
			if authorized {
				for _, id := range ids {
					if id != "active" {
						t.Fatalf("deleted %s", id)
					}
				}
				if len(ids) == 0 {
					t.Fatal("failed transfer was not cancelled")
				}
			} else if len(ids) != 0 {
				t.Fatalf("routine cleanup deleted placements: %v", ids)
			}
			_, err = os.Stat(path)
			if mode == "false" {
				if err != nil {
					t.Fatalf("preserved staging: %v", err)
				}
			} else if !os.IsNotExist(err) {
				t.Fatalf("staging remains: %v", err)
			}
			if err = m.DeleteQBitQueueEntry(entry.InfoHash, true); err != nil {
				t.Fatalf("duplicate: %v", err)
			}
		})
	}
}

func TestQBitCleanupRejectsChangedAuthorization(t *testing.T) {
	for _, change := range []string{"progress", "placement", "reason"} {
		t.Run(change, func(t *testing.T) {
			store := newLifecycleTestStorage(t)
			queue := newLifecycleTestQueue(store, newEntryLifecycle())
			e, _ := addLifecycleTestEntry(t, queue, "changed-"+change)
			e.DownloadUncached = true
			e.State = storage.EntryStateStalledDL
			e.HandoffReason = "terminal"
			e.Category = "sonarr"
			e.ClientEndpoint = "host:8282"
			e.ActiveProvider = "fake"
			e.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "active"}}
			if err := queue.Update(e); err != nil {
				t.Fatal(err)
			}
			if err := store.AuthorizeUncachedHandoff(e); err != nil {
				t.Fatal(err)
			}
			if change == "progress" {
				e.Progress = .5
			}
			if change == "placement" {
				e.Providers["fake"].ID = "new"
			}
			if change == "reason" {
				e.HandoffReason = "unknown"
			}
			if err := queue.Update(e); err != nil {
				t.Fatal(err)
			}
			clients := xsync.NewMap[string, debrid.Client]()
			clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(string) error { t.Fatal("stale proof deleted provider"); return nil }})
			m := &Manager{storage: store, queue: queue, clients: clients}
			if err := m.DeleteQBitQueueEntry(e.InfoHash, true); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Explicit administrative deletion remains destructive.
func TestAdministrativeQueueDeletionStillRemovesMain(t *testing.T) {
	store := newLifecycleTestStorage(t)
	queue := newLifecycleTestQueue(store, newEntryLifecycle())
	e, _ := addLifecycleTestEntry(t, queue, "admin")
	if err := store.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}
	m := &Manager{storage: store, queue: queue, clients: xsync.NewMap[string, debrid.Client](), logger: zerolog.Nop()}
	if err := m.DeleteQueueEntry(e.InfoHash, true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(e.InfoHash); !storage.IsEntryNotFound(err) {
		t.Fatalf("admin main remained: %v", err)
	}
}

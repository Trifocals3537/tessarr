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

func TestAdminRecoveryFailurePreservesAssociatedQueueCleanup(t *testing.T) {
	for _, method := range []string{"entry", "queue"} {
		t.Run(method, func(t *testing.T) {
			db := filepath.Join(t.TempDir(), "db")
			store, err := storage.NewStorage(db)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			queue := newLifecycleTestQueue(store, newEntryLifecycle())
			entry, staging := addLifecycleTestEntry(t, queue, "admin-recovery-order")
			entry.ActiveProvider = "fake"
			entry.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "library"}}
			if err := queue.Update(entry); err != nil {
				t.Fatal(err)
			}
			if err := store.AddOrUpdate(entry); err != nil {
				t.Fatal(err)
			}
			if _, err := store.PrepareQBitQueuedDeletion(entry.InfoHash, true); err != nil {
				t.Fatal(err)
			}
			unrelated := &storage.Entry{InfoHash: "unrelated-cleanup", Name: "unrelated.mkv", Protocol: entry.Protocol, SavePath: entry.SavePath}
			unrelatedPath := unrelated.DownloadPath()
			if _, _, err := claimTorrentEntryDirectory(entry.SavePath, unrelated, torrentLegacyProof{}); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(unrelatedPath, "payload"), []byte("data"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := queue.Add(unrelated); err != nil {
				t.Fatal(err)
			}
			if _, err := store.PrepareQBitQueuedDeletion(unrelated.InfoHash, true); err != nil {
				t.Fatal(err)
			}
			offline := errors.New("provider unavailable")
			clients := xsync.NewMap[string, debrid.Client]()
			clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(string) error { return offline }})
			manager := &Manager{storage: store, queue: queue, clients: clients, logger: zerolog.Nop()}
			if method == "entry" {
				err = manager.DeleteEntry(entry.InfoHash, true)
			} else {
				err = manager.DeleteQueueEntry(entry.InfoHash, true)
			}
			if !errors.Is(err, offline) {
				t.Fatalf("admin failure = %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = storage.NewStorage(db)
			if err != nil {
				t.Fatal(err)
			}
			manager.storage = store
			manager.queue = newLifecycleTestQueue(store, newEntryLifecycle())
			// Repeated failure must not remove files or forget either obligation.
			for attempt := 0; attempt < 2; attempt++ {
				residual, fatal := manager.recoverQueuedDeletions()
				if fatal != nil || !errors.Is(residual, offline) {
					t.Fatalf("recovery = %v, %v", residual, fatal)
				}
				if _, err := os.Stat(staging); err != nil {
					t.Fatalf("staging removed while main obligation failed: %v", err)
				}
				intents, err := store.QueuedDeletionIntents()
				if err != nil || len(intents) != 1 || intents[0].InfoHash != entry.InfoHash {
					t.Fatalf("queue obligations = %v, %v", intents, err)
				}
				admins, err := store.MainDeletionPlacementIntents()
				if err != nil || len(admins) != 1 {
					t.Fatalf("admin obligations = %v, %v", admins, err)
				}
				if _, err := store.GetQueued(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
					t.Fatalf("failed cleanup queue became restorable: %v", err)
				}
				if _, err := os.Stat(unrelatedPath); !os.IsNotExist(err) {
					t.Fatalf("unrelated cleanup was blocked: %v", err)
				}
			}
			clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(string) error { return nil }})
			requestCleanupRetry(t, manager, "retry")
			residual, fatal := manager.recoverQueuedDeletions()
			if residual != nil || fatal != nil {
				t.Fatalf("successful retry = %v, %v", residual, fatal)
			}
			if _, err := os.Stat(staging); !os.IsNotExist(err) {
				t.Fatalf("staging retained after success: %v", err)
			}
			intents, err := store.QueuedDeletionIntents()
			if err != nil || len(intents) != 0 {
				t.Fatalf("queue cleanup incomplete: %v, %v", intents, err)
			}
			admins, err := store.MainDeletionPlacementIntents()
			if err != nil || len(admins) != 0 {
				t.Fatalf("admin cleanup incomplete: %v, %v", admins, err)
			}
		})
	}
}

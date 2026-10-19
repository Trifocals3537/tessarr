package manager

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

func TestCleanupRetryMatrix(t *testing.T) {
	for _, mode := range []string{"authorized", "admin-entry", "admin-queue", "queue-only-existing", "queue-only-fresh"} {
		for _, reopen := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reopen=%t", mode, reopen), func(t *testing.T) {
				root := t.TempDir()
				config.SetConfigPath(root)
				db := filepath.Join(root, "db")
				s, err := storage.NewStorage(db)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := s.Close(); err != nil {
						t.Error(err)
					}
				}()
				q := newLifecycleTestQueue(s, newEntryLifecycle())
				e, path := addLifecycleTestEntry(t, q, "retry-matrix")
				e.ActiveProvider = "fake"
				e.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "active"}}
				if mode == "authorized" {
					e.DownloadUncached = true
					e.State = storage.EntryStateStalledDL
					e.HandoffReason = "terminal"
					e.Category = "sonarr"
					e.ClientEndpoint = "host:8282"
				}
				if err = q.Update(e); err != nil {
					t.Fatal(err)
				}
				if mode == "authorized" {
					if err = s.AuthorizeUncachedHandoff(e); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "admin-entry" || mode == "admin-queue" {
					main := *e
					main.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "library"}}
					if err = s.AddOrUpdate(&main); err != nil {
						t.Fatal(err)
					}
				}
				if mode != "authorized" && mode != "queue-only-fresh" {
					if _, err = s.PrepareQBitQueuedDeletion(e.InfoHash, true); err != nil {
						t.Fatal(err)
					}
				}
				offline := errors.New("provider offline")
				var providerErr error = offline
				calls := 0
				clients := xsync.NewMap[string, debrid.Client]()
				clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(string) error { calls++; return providerErr }})
				m := &Manager{storage: s, queue: q, clients: clients, logger: zerolog.Nop()}
				admin := func() error {
					if mode == "admin-entry" {
						return m.DeleteEntry(e.InfoHash, true)
					}
					return m.DeleteQueueEntry(e.InfoHash, true)
				}
				if mode == "authorized" {
					err = m.DeleteQBitQueueEntry(e.InfoHash, true)
				} else {
					err = admin()
				}
				if err == nil {
					t.Fatal("first operation unexpectedly succeeded")
				}
				if mode != "queue-only-existing" && !errors.Is(err, offline) {
					t.Fatal(err)
				}
				if _, err = os.Stat(path); err != nil {
					t.Fatal(err)
				}
				if reopen {
					if err = s.Close(); err != nil {
						t.Fatal(err)
					}
					s, err = storage.NewStorage(db)
					if err != nil {
						t.Fatal(err)
					}
					q = newLifecycleTestQueue(s, newEntryLifecycle())
					m.storage = s
					m.queue = q
				}
				before := calls
				if mode == "queue-only-existing" {
					for i := 0; i < 2; i++ {
						if err = admin(); err == nil {
							t.Fatal("incompatible admin retry reported success")
						}
					}
					if calls != before {
						t.Fatal("immutable benign provider scope widened")
					}
					intents, err := s.QueuedDeletionIntents()
					if err != nil || len(intents) != 1 || intents[0].PlacementCleanupPending || intents[0].PlacementScope != storage.QueuePlacementScopeTransferOnly {
						t.Fatalf("intent changed: %+v %v", intents, err)
					}
					if _, err = os.Stat(path); err != nil {
						t.Fatal(err)
					}
					return
				}
				if mode != "queue-only-fresh" {
					for _, flag := range []bool{false, true} {
						if mode == "authorized" && flag {
							continue
						}
						if err = m.DeleteQBitQueueEntry(e.InfoHash, flag); err == nil {
							t.Fatal("client retry bypassed unfinished obligation")
						}
						if calls != before {
							t.Fatal("preserving/client retry cancelled provider")
						}
						if _, err = os.Stat(path); err != nil {
							t.Fatal(err)
						}
					}
				}
				// A different hash is not gated by the failed administrative snapshot.
				other := &storage.Entry{InfoHash: "unrelated", Name: "unrelated", Protocol: config.ProtocolTorrent, SavePath: e.SavePath}
				if err = q.Add(other); err != nil {
					t.Fatal(err)
				}
				if err = m.DeleteQBitQueueEntry(other.InfoHash, false); err != nil {
					t.Fatalf("unrelated hash blocked: %v", err)
				}
				providerErr = nil
				if mode == "authorized" {
					err = m.DeleteQBitQueueEntry(e.InfoHash, true)
				} else if reopen && (mode == "admin-entry" || mode == "admin-queue") {
					residual, fatal := m.recoverQueuedDeletions()
					err = errors.Join(residual, fatal)
				} else {
					err = admin()
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err = os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("original file obligation not completed: %v", err)
				}
				intents, err := s.QueuedDeletionIntents()
				if err != nil || len(intents) != 0 {
					t.Fatalf("unfinished intents: %v %v", intents, err)
				}
				if calls <= before {
					t.Fatal("provider obligation never resumed")
				}
			})
		}
	}
}

func TestAuthorizedFilesPendingRejectsPreservingManagerRetry(t *testing.T) {
	s := newLifecycleTestStorage(t)
	q := newLifecycleTestQueue(s, newEntryLifecycle())
	e, path := addLifecycleTestEntry(t, q, "files-pending")
	e.DownloadUncached = true
	e.State = storage.EntryStateStalledDL
	e.HandoffReason = "terminal"
	e.Category = "sonarr"
	e.ClientEndpoint = "host:8282"
	e.ActiveProvider = "fake"
	e.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "active"}}
	if err := q.Update(e); err != nil {
		t.Fatal(err)
	}
	if err := s.AuthorizeUncachedHandoff(e); err != nil {
		t.Fatal(err)
	}
	intent, err := s.PrepareQBitQueuedDeletion(e.InfoHash, true)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.StartQueuedDeletionCleanup(intent.InfoHash, intent.QueueIncarnation); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkQueuedDeletionPlacementsClean(intent.InfoHash, intent.QueueIncarnation); err != nil {
		t.Fatal(err)
	}
	m := &Manager{storage: s, queue: q, logger: zerolog.Nop()}
	if err = m.DeleteQBitQueueEntry(e.InfoHash, false); err == nil {
		t.Fatal("preserving retry discarded pending files")
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if err = m.DeleteQBitQueueEntry(e.InfoHash, true); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("files remained: %v", err)
	}
}

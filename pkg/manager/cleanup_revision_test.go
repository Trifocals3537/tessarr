package manager

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

// The provider barrier is the final validation/delete interval, not a scan hook.
// A publication attempted here must fail promptly (including from a synchronous
// provider callback), rather than wait on a mutex held over network work.
func TestTransferCleanupFinalIntervalRejectsMainPublication(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		for _, sameHash := range []bool{false, true} {
			t.Run(fmtRevisionCase(recovery, sameHash), func(t *testing.T) {
				store := newLifecycleTestStorage(t)
				q := newLifecycleTestQueue(store, newEntryLifecycle())
				e, _ := addLifecycleTestEntry(t, q, "reserved-transfer")
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
				if err := store.AuthorizeUncachedHandoff(e); err != nil {
					t.Fatal(err)
				}
				if recovery {
					if _, err := store.PrepareQBitQueuedDeletion(e.InfoHash, true); err != nil {
						t.Fatal(err)
					}
				}
				entered := make(chan struct{})
				release := make(chan struct{})
				done := make(chan error, 1)
				clients := xsync.NewMap[string, debrid.Client]()
				clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(string) error { close(entered); <-release; return nil }})
				m := &Manager{storage: store, queue: q, clients: clients, logger: zerolog.Nop()}
				go func() {
					if recovery {
						r, f := m.recoverQueuedDeletions()
						done <- errors.Join(r, f)
					} else {
						done <- m.DeleteQBitQueueEntry(e.InfoHash, true)
					}
				}()
				<-entered
				main := *e
				main.MainGeneration = 0
				if !sameHash {
					main.InfoHash = "other-library"
				}
				published := make(chan error, 1)
				go func() { published <- store.AddOrUpdate(&main) }()
				var err error
				select {
				case err = <-published:
					close(release)
				case <-time.After(5 * time.Second):
					close(release)
					err = <-published
					t.Error("publication blocked behind provider network call")
				}
				if !errors.Is(err, storage.ErrPlacementCleanupReserved) {
					t.Errorf("main publication in destructive provider interval: %v", err)
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if _, err := store.Get(main.InfoHash); !storage.IsEntryNotFound(err) {
					t.Fatalf("reserved placement published: %v", err)
				}
			})
		}
	}
}
func fmtRevisionCase(recovery, same bool) string {
	if recovery {
		if same {
			return "recovery-same"
		}
		return "recovery-other"
	}
	if same {
		return "live-same"
	}
	return "live-other"
}

func TestAdminResumesImmutableClientIntent(t *testing.T) {
	for _, method := range []string{"entry", "queue"} {
		for _, authorized := range []bool{false, true} {
			for _, remove := range []bool{false, true} {
				name := method + "-benign"
				if authorized {
					name = method + "-authorized"
				}
				if remove {
					name += "-true"
				} else {
					name += "-false"
				}
				t.Run(name, func(t *testing.T) {
					store := newLifecycleTestStorage(t)
					q := newLifecycleTestQueue(store, newEntryLifecycle())
					e, path := addLifecycleTestEntry(t, q, "admin-resume")
					e.ActiveProvider = "fake"
					e.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "active"}}
					e.DownloadUncached = true
					e.State = storage.EntryStateStalledDL
					e.HandoffReason = "terminal"
					e.Category = "sonarr"
					e.ClientEndpoint = "host:8282"
					if err := q.Update(e); err != nil {
						t.Fatal(err)
					}
					if authorized {
						if err := store.AuthorizeUncachedHandoff(e); err != nil {
							t.Fatal(err)
						}
					}
					if _, err := store.PrepareQBitQueuedDeletion(e.InfoHash, authorized); err != nil {
						t.Fatal(err)
					}
					main := *e
					main.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "library"}}
					if err := store.AddOrUpdate(&main); err != nil {
						t.Fatal(err)
					}
					clients := xsync.NewMap[string, debrid.Client]()
					deleted := map[string]int{}
					clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(id string) error {
						intents, err := store.QueuedDeletionIntents()
						if err != nil {
							t.Fatal(err)
						}
						for _, intent := range intents {
							if intent.PlacementScope != storage.QueuePlacementScopeTransferOnly || len(intent.PlacementEntries) > 1 {
								t.Error("admin widened client intent")
							}
						}
						admin, err := store.MainDeletionPlacementIntents()
						if err != nil || len(admin) != 1 || admin[0].Providers["fake"].ID != "library" {
							t.Fatalf("admin callback lacks independent durable obligation: %v %v", admin, err)
						}
						deleted[id]++
						return nil
					}})
					m := &Manager{storage: store, queue: q, clients: clients, logger: zerolog.Nop()}
					var err error
					if method == "entry" {
						err = m.DeleteEntry(e.InfoHash, remove)
					} else {
						err = m.DeleteQueueEntry(e.InfoHash, remove)
					}
					// A false admin request cannot fulfill a pre-existing authorized obligation;
					// it must fail closed rather than revoke it.
					if authorized && !remove {
						if err == nil {
							t.Fatal("authorized pending cleanup silently dropped")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if remove && deleted["library"] != 1 {
						t.Fatalf("main provider deletion count=%d", deleted["library"])
					}
					if !remove && len(deleted) != 0 {
						t.Fatal(deleted)
					}
					if !authorized {
						if _, err := os.Stat(path); err != nil {
							t.Fatalf("sticky preserve lost: %v", err)
						}
					}
				})
			}
		}
	}
}

func TestAdminProviderObligationSurvivesRestart(t *testing.T) {
	db := filepath.Join(t.TempDir(), "db")
	store, err := storage.NewStorage(db)
	if err != nil {
		t.Fatal(err)
	}
	q := newLifecycleTestQueue(store, newEntryLifecycle())
	e, _ := addLifecycleTestEntry(t, q, "admin-restart")
	e.ActiveProvider = "fake"
	e.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "library"}}
	if err := q.Update(e); err != nil {
		t.Fatal(err)
	}
	if err := store.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PrepareQBitQueuedDeletion(e.InfoHash, false); err != nil {
		t.Fatal(err)
	}
	clients := xsync.NewMap[string, debrid.Client]()
	fail := errors.New("offline")
	clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(string) error { return fail }})
	m := &Manager{storage: store, queue: q, clients: clients, logger: zerolog.Nop()}
	if err := m.DeleteQueueEntry(e.InfoHash, true); !errors.Is(err, fail) {
		t.Errorf("admin error=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = storage.NewStorage(db)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	calls := 0
	clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(string) error { calls++; return nil }})
	m.storage = store
	m.queue = newLifecycleTestQueue(store, newEntryLifecycle())
	r, f := m.recoverQueuedDeletions()
	if r != nil || f != nil {
		t.Fatalf("recovery %v %v", r, f)
	}
	if calls != 1 {
		t.Fatalf("durable admin cleanup calls=%d", calls)
	}
}

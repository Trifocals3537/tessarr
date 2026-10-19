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

func TestAdminMainProviderFailurePrecedesQueueFiles(t *testing.T) {
	for _, method := range []string{"entry", "queue"} {
		t.Run(method, func(t *testing.T) {
			s := newLifecycleTestStorage(t)
			q := newLifecycleTestQueue(s, newEntryLifecycle())
			e, path := addLifecycleTestEntry(t, q, "admin-order")
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
			fail := errors.New("main provider unavailable")
			clients := xsync.NewMap[string, debrid.Client]()
			clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(id string) error {
				if id == "library" {
					return fail
				}
				return nil
			}})
			m := &Manager{storage: s, queue: q, clients: clients, logger: zerolog.Nop()}
			var err error
			if method == "entry" {
				err = m.DeleteEntry(e.InfoHash, true)
			} else {
				err = m.DeleteQueueEntry(e.InfoHash, true)
			}
			if !errors.Is(err, fail) {
				t.Fatal(err)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("queue files removed before main provider cleanup succeeded: %v", err)
			}
			if _, err := s.Get(e.InfoHash); err != nil {
				t.Fatalf("main removed after provider failure: %v", err)
			}
		})
	}
}

package manager

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

// rediscoveryTestClient records UpdateTorrent calls — the expensive provider
// work the awaiting-absence skip must prevent — and then aborts processing so
// the test only measures whether the call happened.
type rediscoveryTestClient struct {
	debrid.Client
	cfg         config.Debrid
	updateCalls *atomic.Int64
}

func (c *rediscoveryTestClient) Config() config.Debrid  { return c.cfg }
func (c *rediscoveryTestClient) Logger() zerolog.Logger { return zerolog.Nop() }

func (c *rediscoveryTestClient) UpdateTorrent(*debridTypes.Torrent) error {
	c.updateCalls.Add(1)
	return errors.New("stop after recording the expensive provider call")
}

func newRediscoveryManager(t *testing.T, store *storage.Storage) (*Manager, *atomic.Int64) {
	t.Helper()
	var updateCalls atomic.Int64
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("torbox", &rediscoveryTestClient{
		cfg:         config.Debrid{Name: "torbox", Provider: "torbox"},
		updateCalls: &updateCalls,
	})
	return &Manager{
		clients: clients,
		storage: store,
		config:  &config.Config{Debrids: []config.Debrid{{Name: "torbox"}}},
		logger:  zerolog.Nop(),
	}, &updateCalls
}

func rediscoveryCandidate(infoHash string) *debridTypes.Torrent {
	return &debridTypes.Torrent{
		Debrid:   "torbox",
		InfoHash: infoHash,
		Name:     "Candidate",
		Files:    map[string]debridTypes.File{}, // incomplete: forces an UpdateTorrent call unless skipped
	}
}

func seedRetiredEntry(t *testing.T, store *storage.Storage, key string) {
	t.Helper()
	entry := &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       key,
		Name:           "entry-" + key,
		ActiveProvider: "torbox",
		Providers: map[string]*storage.ProviderEntry{
			"torbox": {Provider: "torbox", ID: "provider-id"},
		},
		Files: map[string]*storage.File{},
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("seed entry: %v", err)
	}
	if err := store.Delete(key); err != nil {
		t.Fatalf("delete entry: %v", err)
	}
}

func openStorage(t *testing.T, dbPath string) *storage.Storage {
	t.Helper()
	store, err := storage.NewStorage(dbPath)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// TestProcessSyncTorrentSkipsEntriesAwaitingProviderAbsence proves the sync
// path performs no provider work for tombstoned entries that the rediscovery
// guard would reject anyway.
func TestProcessSyncTorrentSkipsEntriesAwaitingProviderAbsence(t *testing.T) {
	store := openStorage(t, t.TempDir())
	seedRetiredEntry(t, store, "awaiting-key")

	snapshot := store.BeginProviderSnapshot()
	m, updateCalls := newRediscoveryManager(t, store)

	mt, err := m.processSyncTorrent(rediscoveryCandidate("awaiting-key"), snapshot)
	if err != nil {
		t.Fatalf("skip must be silent, got error %v", err)
	}
	if mt != nil {
		t.Fatal("skipped entry must not produce a batch candidate")
	}
	if got := updateCalls.Load(); got != 0 {
		t.Fatalf("UpdateTorrent calls = %d, want 0 for entry awaiting provider absence", got)
	}
}

// TestProcessSyncTorrentStillProcessesAuthorizedRediscovery proves the skip
// never delays a legitimately authorized reappearance: once an authoritative
// absence was observed, a strictly later snapshot is processed normally.
func TestProcessSyncTorrentStillProcessesAuthorizedRediscovery(t *testing.T) {
	store := openStorage(t, t.TempDir())
	seedRetiredEntry(t, store, "authorized-key")

	absence := store.BeginProviderSnapshot()
	if err := store.ObserveProviderSnapshot("torbox", absence, nil); err != nil {
		t.Fatalf("observe absence: %v", err)
	}
	reappearance := store.BeginProviderSnapshot()

	m, updateCalls := newRediscoveryManager(t, store)
	if !m.shouldLogRediscoveryPending("torbox/authorized-key", time.Now()) {
		t.Fatal("failed to seed pending notice state")
	}
	_, err := m.processSyncTorrent(rediscoveryCandidate("authorized-key"), reappearance)
	if err == nil {
		t.Fatal("expected the mock client's stop error after the expensive call")
	}
	if got := updateCalls.Load(); got != 1 {
		t.Fatalf("UpdateTorrent calls = %d, want 1 for authorized rediscovery", got)
	}
	if _, retained := m.rediscoveryPendingLast["torbox/authorized-key"]; retained {
		t.Fatal("authorized rediscovery retained stale coalescer state")
	}
}

// TestProcessSyncTorrentStillProcessesLiveEntries guards against the skip
// ever firing for entries that were never deleted.
func TestProcessSyncTorrentStillProcessesLiveEntries(t *testing.T) {
	store := openStorage(t, t.TempDir())
	entry := &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       "live-key",
		Name:           "live",
		ActiveProvider: "torbox",
		Providers:      map[string]*storage.ProviderEntry{"torbox": {Provider: "torbox", ID: "id"}},
		Files:          map[string]*storage.File{},
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("seed entry: %v", err)
	}

	snapshot := store.BeginProviderSnapshot()
	m, updateCalls := newRediscoveryManager(t, store)
	_, err := m.processSyncTorrent(rediscoveryCandidate("live-key"), snapshot)
	if err == nil {
		t.Fatal("expected the mock client's stop error after the expensive call")
	}
	if got := updateCalls.Load(); got != 1 {
		t.Fatalf("UpdateTorrent calls = %d, want 1 for live entry", got)
	}
}

// TestProcessSyncTorrentNilStorageKeepsLegacyBehavior proves a Manager built
// without storage (unit-test construction) still processes normally.
func TestProcessSyncTorrentNilStorageKeepsLegacyBehavior(t *testing.T) {
	m, updateCalls := newRediscoveryManager(t, nil)
	m.storage = nil
	_, err := m.processSyncTorrent(rediscoveryCandidate("no-storage"))
	if err == nil {
		t.Fatal("expected the mock client's stop error after the expensive call")
	}
	if got := updateCalls.Load(); got != 1 {
		t.Fatalf("UpdateTorrent calls = %d, want 1 without storage", got)
	}
}

// TestRediscoveryPendingLogRateLimit proves the coalescer emits one notice per
// entry per interval: suppressed inside the window, refired after it.
func TestRediscoveryPendingLogRateLimit(t *testing.T) {
	base := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	now := base
	m := &Manager{logger: zerolog.Nop(), rediscoveryPendingNowFunc: func() time.Time { return now }}

	if !m.shouldLogRediscoveryPending("torbox/key", now) {
		t.Fatal("first notice must fire")
	}
	now = base.Add(30 * time.Minute)
	if m.shouldLogRediscoveryPending("torbox/key", now) {
		t.Fatal("repeat inside the interval must be suppressed")
	}
	now = base.Add(time.Hour + time.Minute)
	if !m.shouldLogRediscoveryPending("torbox/key", now) {
		t.Fatal("notice must refire after the interval elapsed")
	}
	if !m.shouldLogRediscoveryPending("realdebrid/other-key", now) {
		t.Fatal("independent entries must not suppress each other")
	}
}

// TestRediscoveryPendingLogRateUnderConcurrency proves exactly one notice
// fires no matter how many sync workers race on the same entry.
func TestRediscoveryPendingLogRateUnderConcurrency(t *testing.T) {
	m := &Manager{logger: zerolog.Nop()}
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if m.shouldLogRediscoveryPending("torbox/key", time.Now()) {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 1 {
		t.Fatalf("concurrent first notices = %d, want exactly 1", got)
	}
}

// TestRediscoveryPendingLogRefiresAfterRestart models a process restart: the
// in-memory coalescer is gone, so the fresh process emits its own first
// notice, retaining visibility across restarts.
func TestRediscoveryPendingLogRefiresAfterRestart(t *testing.T) {
	first := &Manager{logger: zerolog.Nop()}
	if !first.shouldLogRediscoveryPending("torbox/key", time.Now()) {
		t.Fatal("first process must log its first occurrence")
	}
	if first.shouldLogRediscoveryPending("torbox/key", time.Now()) {
		t.Fatal("second occurrence in the same process must be suppressed")
	}
	restarted := &Manager{logger: zerolog.Nop()}
	if !restarted.shouldLogRediscoveryPending("torbox/key", time.Now()) {
		t.Fatal("restarted process must log its own first occurrence")
	}
}

func TestRediscoveryPendingLogStateIsStrictlyBounded(t *testing.T) {
	m := &Manager{logger: zerolog.Nop()}
	base := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	for i := range rediscoveryPendingMaxEntries + 32 {
		key := fmt.Sprintf("torbox/key-%d", i)
		if !m.shouldLogRediscoveryPending(key, base.Add(time.Duration(i)*time.Nanosecond)) {
			t.Fatalf("first notice for %q was suppressed", key)
		}
	}
	if got := len(m.rediscoveryPendingLast); got != rediscoveryPendingMaxEntries {
		t.Fatalf("coalescer entries = %d, want hard cap %d", got, rediscoveryPendingMaxEntries)
	}
	if _, retained := m.rediscoveryPendingLast["torbox/key-0"]; retained {
		t.Fatal("oldest coalescer entry was not evicted at the hard cap")
	}
}

func TestClearRediscoveryPendingReleasesEntry(t *testing.T) {
	m := &Manager{logger: zerolog.Nop()}
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	if !m.shouldLogRediscoveryPending("torbox/key", now) {
		t.Fatal("first notice must fire")
	}

	m.clearRediscoveryPending("torbox", "key")
	if got := len(m.rediscoveryPendingLast); got != 0 {
		t.Fatalf("coalescer entries after clear = %d, want 0", got)
	}
	if !m.shouldLogRediscoveryPending("torbox/key", now.Add(time.Minute)) {
		t.Fatal("cleared entry must be treated as new if it becomes blocked again")
	}
}

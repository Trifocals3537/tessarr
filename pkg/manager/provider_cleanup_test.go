package manager

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

type placementDeleteTestProvider struct {
	debrid.Client
	name   string
	delete func(string) error
}

func (p *placementDeleteTestProvider) DeleteTorrent(id string) error {
	return p.delete(id)
}

func (p *placementDeleteTestProvider) Config() config.Debrid {
	return config.Debrid{Name: p.name}
}

func (p *placementDeleteTestProvider) Logger() zerolog.Logger {
	return zerolog.Nop()
}

func TestQueueProviderCleanupIsSynchronousBeforeFilesAndRetainsStateOnFailure(t *testing.T) {
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	queue := newLifecycleTestQueue(store, lifecycle)
	entry, outputPath := addLifecycleTestEntry(t, queue, "provider-cleanup")
	entry.ActiveProvider = "fake"
	entry.Providers = map[string]*storage.ProviderEntry{
		"fake": {Provider: "fake", ID: "placement-1"},
	}
	if err := queue.Update(entry); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	cleanupErr := errors.New("provider unavailable")
	var resultMu sync.RWMutex
	result := cleanupErr
	provider := &placementDeleteTestProvider{
		name: "fake",
		delete: func(string) error {
			close(started)
			<-release
			resultMu.RLock()
			defer resultMu.RUnlock()
			return result
		},
	}
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("fake", provider)
	manager := &Manager{
		storage: store,
		queue:   queue,
		clients: clients,
		logger:  zerolog.Nop(),
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- queue.Delete(entry.InfoHash, manager.DeleteEntryForQueueCleanup)
	}()
	<-started
	select {
	case err := <-deleteDone:
		t.Fatalf("Delete returned while provider cleanup was still running: %v", err)
	default:
	}
	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("files removed before provider cleanup finished: %v", err)
	}
	if _, err := store.GetQueued(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("deleting queue row remained visible during provider cleanup: %v", err)
	}
	close(release)
	if err := <-deleteDone; !errors.Is(err, cleanupErr) {
		t.Fatalf("Delete error = %v, want provider failure", err)
	}
	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("files removed after provider cleanup failure: %v", err)
	}
	if _, err := store.GetQueued(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("failed provider cleanup made queue row visible again: %v", err)
	}
	intents, err := store.QueuedDeletionIntents()
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 1 || !intents[0].PlacementCleanupPending {
		t.Fatalf("provider cleanup intent = %#v", intents)
	}

	resultMu.Lock()
	result = customerror.TorrentNotFoundError
	resultMu.Unlock()
	provider.delete = func(string) error {
		resultMu.RLock()
		defer resultMu.RUnlock()
		return result
	}
	if err := queue.Delete(entry.InfoHash, manager.DeleteEntryForQueueCleanup); err != nil {
		t.Fatalf("idempotent provider retry failed: %v", err)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("files remain after successful cleanup retry: %v", err)
	}
	if _, err := store.GetQueued(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("queue row after cleanup retry = %v", err)
	}
}

func TestRemoveTorrentPlacementsUsesDeterministicOrderAndJoinsErrors(t *testing.T) {
	var mu sync.Mutex
	var order []string
	firstErr := errors.New("first failure")
	secondErr := errors.New("second failure")
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("alpha", &placementDeleteTestProvider{
		name: "alpha",
		delete: func(id string) error {
			mu.Lock()
			order = append(order, "alpha/"+id)
			mu.Unlock()
			return firstErr
		},
	})
	clients.Store("zeta", &placementDeleteTestProvider{
		name: "zeta",
		delete: func(id string) error {
			mu.Lock()
			order = append(order, "zeta/"+id)
			mu.Unlock()
			return secondErr
		},
	})
	manager := &Manager{clients: clients}
	entry := &storage.Entry{Providers: map[string]*storage.ProviderEntry{
		"zeta":  {Provider: "zeta", ID: "2"},
		"alpha": {Provider: "alpha", ID: "1"},
	}}

	err := manager.RemoveTorrentPlacements(entry)
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("RemoveTorrentPlacements() error = %v, want both failures", err)
	}
	if got, want := order, []string{"alpha/1", "zeta/2"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("deletion order = %v, want %v", got, want)
	}
}

func TestRecoverQueuedDeletionRetiresRowAndRetriesPlacementCleanup(t *testing.T) {
	root := t.TempDir()
	config.SetConfigPath(root)
	downloadRoot := filepath.Join(root, "downloads")
	if err := os.MkdirAll(downloadRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	config.Get().DownloadFolder = downloadRoot
	dbPath := filepath.Join(root, "db")

	store, err := storage.NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	queue := newLifecycleTestQueue(store, newEntryLifecycle())
	entry := &storage.Entry{
		InfoHash:       "recover-placement-cleanup",
		Name:           "release.mkv",
		Protocol:       config.ProtocolTorrent,
		SavePath:       downloadRoot,
		ActiveProvider: "fake",
		Providers: map[string]*storage.ProviderEntry{
			"fake": {Provider: "fake", ID: "placement-1"},
		},
	}
	outputPath := entry.DownloadPath()
	if _, _, err := claimTorrentEntryDirectory(
		downloadRoot,
		entry,
		torrentLegacyProof{},
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(outputPath, "payload"),
		[]byte("data"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PrepareQueuedDeletion(entry.InfoHash, true); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = storage.NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close reopened storage: %v", err)
		}
	})

	providerErr := errors.New("provider unavailable")
	var calls int
	fail := true
	provider := &placementDeleteTestProvider{
		name: "fake",
		delete: func(string) error {
			calls++
			if fail {
				return providerErr
			}
			return nil
		},
	}
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("fake", provider)
	manager := &Manager{
		storage: store,
		queue:   newLifecycleTestQueue(store, newEntryLifecycle()),
		clients: clients,
		logger:  zerolog.Nop(),
	}

	residual, fatal := manager.recoverQueuedDeletions()
	if fatal != nil {
		t.Fatalf("first recovery fatal error = %v", fatal)
	}
	if !errors.Is(residual, providerErr) {
		t.Fatalf("first recovery residual = %v, want provider error", residual)
	}
	if _, err := store.GetQueued(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("recovered queue row = %v, want not found", err)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("local files remain after recovery retirement: %v", err)
	}
	intents, err := store.QueuedDeletionIntents()
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 1 || !intents[0].PlacementCleanupPending {
		t.Fatalf("retained recovery intent = %#v", intents)
	}

	fail = false
	residual, fatal = manager.recoverQueuedDeletions()
	if fatal != nil || residual != nil {
		t.Fatalf("second recovery = residual %v, fatal %v", residual, fatal)
	}
	if calls != 2 {
		t.Fatalf("provider cleanup calls = %d, want 2", calls)
	}
	intents, err = store.QueuedDeletionIntents()
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 0 {
		t.Fatalf("completed recovery retained intents: %#v", intents)
	}
}

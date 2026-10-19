package manager

import (
	"context"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

func TestProviderSnapshotStartedBeforeDeleteCannotRecreateMainEntry(t *testing.T) {
	testRoot := t.TempDir()
	config.SetConfigPath(testRoot)
	store, err := storage.NewStorage(testRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const (
		key      = "provider-race"
		provider = "provider-a"
	)
	entry := &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       key,
		Name:           "provider-race",
		ActiveProvider: provider,
		Providers: map[string]*storage.ProviderEntry{
			provider: {
				Provider: provider,
				ID:       "old-id",
				Status:   debridTypes.TorrentStatusDownloaded,
				Files: map[string]*storage.ProviderFile{
					"video.mkv": {Id: "file-id", Link: "https://example.invalid/file"},
				},
			},
		},
		Files: map[string]*storage.File{
			"video.mkv": {Name: "video.mkv", InfoHash: key, Size: 1},
		},
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("seed entry: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	remote := &debridTypes.Torrent{
		Id:       "new-id",
		InfoHash: key,
		Name:     "provider-race",
		Size:     1,
		Bytes:    1,
		Debrid:   provider,
		Status:   debridTypes.TorrentStatusDownloaded,
		Added:    time.Now(),
		Files: map[string]debridTypes.File{
			"video.mkv": {
				Id:   "file-id",
				Name: "video.mkv",
				Size: 1,
				Link: "https://example.invalid/file",
			},
		},
	}
	client := &mainGateProvider{
		blockingLifecycleProvider: newBlockingLifecycleProvider(make(chan struct{})),
		started:                   started,
		release:                   release,
		remote:                    []*debridTypes.Torrent{remote},
		name:                      provider,
	}
	manager := &Manager{
		storage: store,
		clients: xsync.NewMap[string, debrid.Client](),
		logger:  zerolog.Nop(),
	}
	manager.clients.Store(provider, client)

	syncDone := make(chan error, 1)
	go func() {
		syncDone <- manager.doRefreshTorrents(context.Background(), provider, client)
	}()
	<-started
	if err := store.Delete(key); err != nil {
		t.Fatalf("delete during provider request: %v", err)
	}
	close(release)
	if err := <-syncDone; err != nil {
		t.Fatalf("provider refresh: %v", err)
	}
	if _, err := store.Get(key); !storage.IsEntryNotFound(err) {
		t.Fatalf("stale provider response recreated row: Get error = %v", err)
	}

	laterPresence := store.BeginProviderSnapshot()
	candidate := &storage.Entry{
		Protocol: config.ProtocolTorrent,
		InfoHash: key,
		Name:     "still-stale",
		Providers: map[string]*storage.ProviderEntry{
			provider: {Provider: provider, ID: "new-id"},
		},
		Files: map[string]*storage.File{},
	}
	if err := store.PrepareProviderEntry(candidate, provider, laterPresence); err == nil {
		t.Fatal("provider presence without an intervening absence was authorized")
	}
}

type mainGateProvider struct {
	*blockingLifecycleProvider
	started chan struct{}
	release <-chan struct{}
	remote  []*debridTypes.Torrent
	name    string
}

func (p *mainGateProvider) GetTorrents() ([]*debridTypes.Torrent, error) {
	close(p.started)
	<-p.release
	return p.remote, nil
}

func (p *mainGateProvider) UpdateTorrent(*debridTypes.Torrent) error {
	return nil
}

func (p *mainGateProvider) Config() config.Debrid {
	return config.Debrid{Name: p.name}
}

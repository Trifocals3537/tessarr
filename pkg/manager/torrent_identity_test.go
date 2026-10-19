package manager

import (
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

func TestDetectTorrentChangesPreservesPlacementByProviderIDWhenHashIsOmitted(t *testing.T) {
	store := newLifecycleTestStorage(t)
	const infoHash = "abcdef0123456789"
	entry := managedProviderEntry(infoHash, "premiumize", "transfer-1")
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{storage: store, logger: zerolog.Nop()}
	remote := indexRemoteTorrents([]*debridTypes.Torrent{{
		Id:       "transfer-1",
		InfoHash: "",
		Debrid:   "premiumize",
		Status:   debridTypes.TorrentStatusDownloaded,
	}})

	added, updated, deleted, present, err := manager.detectTorrentChanges("premiumize", remote)
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != 0 || len(updated) != 0 || len(deleted) != 0 {
		t.Fatalf("changes = added %d, updated %d, deleted %d; want none", len(added), len(updated), len(deleted))
	}
	if _, ok := present[infoHash]; !ok {
		t.Fatalf("present = %v, want stored hash %q", present, infoHash)
	}
}

func TestDetectTorrentChangesRebindsHashlessUpdateWithoutMutatingRemote(t *testing.T) {
	store := newLifecycleTestStorage(t)
	const infoHash = "abcdef0123456789"
	entry := managedProviderEntry(infoHash, "premiumize", "transfer-1")
	entry.Providers["premiumize"].Status = debridTypes.TorrentStatusDownloading
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{storage: store, logger: zerolog.Nop()}
	remoteTorrent := &debridTypes.Torrent{
		Id:       "transfer-1",
		InfoHash: "",
		Debrid:   "premiumize",
		Status:   debridTypes.TorrentStatusDownloaded,
	}

	added, updated, deleted, present, err := manager.detectTorrentChanges(
		"premiumize",
		indexRemoteTorrents([]*debridTypes.Torrent{remoteTorrent}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != 1 || added[0] == remoteTorrent || added[0].InfoHash != infoHash {
		t.Fatalf("added = %#v, want a rebound copy with stored hash", added)
	}
	if remoteTorrent.InfoHash != "" {
		t.Fatalf("shared remote record was mutated: %#v", remoteTorrent)
	}
	if len(updated) != 0 || len(deleted) != 0 {
		t.Fatalf("changes = updated %d, deleted %d; want none", len(updated), len(deleted))
	}
	if _, ok := present[infoHash]; !ok {
		t.Fatalf("present = %v, want stored hash %q", present, infoHash)
	}
}

func TestDetectTorrentChangesIgnoresUnmatchedHashlessTransfers(t *testing.T) {
	store := newLifecycleTestStorage(t)
	manager := &Manager{storage: store, logger: zerolog.Nop()}
	remote := indexRemoteTorrents([]*debridTypes.Torrent{{
		Id:       "unmanaged-cloud-item",
		InfoHash: "",
		Debrid:   "premiumize",
	}})

	added, updated, deleted, present, err := manager.detectTorrentChanges("premiumize", remote)
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != 0 || len(updated) != 0 || len(deleted) != 0 || len(present) != 0 {
		t.Fatalf("hashless unmanaged record produced changes: added=%d updated=%d deleted=%d present=%v", len(added), len(updated), len(deleted), present)
	}
}

func TestDetectTorrentChangesDoesNotRebindExplicitDifferentHash(t *testing.T) {
	store := newLifecycleTestStorage(t)
	const oldHash = "oldhash"
	const newHash = "newhash"
	if err := store.AddOrUpdate(managedProviderEntry(oldHash, "premiumize", "transfer-1")); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{storage: store, logger: zerolog.Nop()}
	remoteTorrent := &debridTypes.Torrent{
		Id:       "transfer-1",
		InfoHash: newHash,
		Debrid:   "premiumize",
		Status:   debridTypes.TorrentStatusDownloaded,
		Added:    time.Now(),
	}

	added, updated, deleted, present, err := manager.detectTorrentChanges(
		"premiumize",
		indexRemoteTorrents([]*debridTypes.Torrent{remoteTorrent}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != 1 || added[0] != remoteTorrent {
		t.Fatalf("added = %#v, want explicit new-hash torrent", added)
	}
	if len(updated) != 0 || len(deleted) != 1 || deleted[0].InfoHash != oldHash {
		t.Fatalf("updated=%#v deleted=%#v, want old placement deletion", updated, deleted)
	}
	if _, ok := present[newHash]; !ok {
		t.Fatalf("present = %v, want new hash", present)
	}
	if _, ok := present[oldHash]; ok {
		t.Fatalf("present = %v, old hash must not be rebound", present)
	}
}

func TestDetectTorrentChangesMatchesHashCaseInsensitively(t *testing.T) {
	store := newLifecycleTestStorage(t)
	const storedHash = "abcdef"
	if err := store.AddOrUpdate(managedProviderEntry(storedHash, "realdebrid", "transfer-1")); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{storage: store, logger: zerolog.Nop()}
	remote := indexRemoteTorrents([]*debridTypes.Torrent{{
		Id:       "transfer-1",
		InfoHash: "ABCDEF",
		Debrid:   "realdebrid",
		Status:   debridTypes.TorrentStatusDownloaded,
	}})

	added, updated, deleted, present, err := manager.detectTorrentChanges("realdebrid", remote)
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != 0 || len(updated) != 0 || len(deleted) != 0 {
		t.Fatalf("case-only hash produced changes: added=%d updated=%d deleted=%d", len(added), len(updated), len(deleted))
	}
	if _, ok := present[storedHash]; !ok {
		t.Fatalf("present = %v, want normalized hash", present)
	}
}

func managedProviderEntry(infoHash, provider, providerID string) *storage.Entry {
	return &storage.Entry{
		InfoHash: infoHash,
		Name:     infoHash,
		Protocol: config.ProtocolTorrent,
		Providers: map[string]*storage.ProviderEntry{
			provider: {
				Provider: provider,
				ID:       providerID,
				Status:   debridTypes.TorrentStatusDownloaded,
				Files: map[string]*storage.ProviderFile{
					"video.mkv": {Id: "file-1", Link: "https://download.invalid/file-1"},
				},
			},
		},
	}
}

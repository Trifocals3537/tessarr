package storage

import (
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestReconcileCompletedTorrentEntryBeforeMergePreservesCanonicalIdentity(t *testing.T) {
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "one", Path: "Season 01/Episode.mkv", Size: 11, Link: "provider://one"},
		{Id: "two", Path: "Season 02/Episode.mkv", Size: 12, Link: "provider://two"},
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Now()
	const legacyName = "Season 01/Episode.mkv"
	existing := &Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       "same-hash",
		CompletedAt:    &completed,
		ActiveProvider: "primary",
		Files: map[string]*File{
			legacyName: {ID: "stable-canonical", Name: legacyName, Path: legacyName, Size: 11},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				legacyName: {Id: "one", Path: legacyName, Link: "provider://old"},
			}},
		},
	}
	incoming := &Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       existing.InfoHash,
		ActiveProvider: "primary",
		Files:          make(map[string]*File),
		Providers:      make(map[string]*ProviderEntry),
	}
	if _, err := incoming.AddTorrentProvider(&debridTypes.Torrent{
		Id: "transfer", Debrid: "primary", Status: debridTypes.TorrentStatusDownloaded, Files: remoteFiles,
	}); err != nil {
		t.Fatal(err)
	}
	queuedAt := completed.Add(-time.Hour)
	incomingPlacement := incoming.GetActiveProvider()
	incomingPlacement.AddedAt = queuedAt
	incomingPlacement.Progress = 1.0
	incomingPlacement.DownloadedAt = &completed
	for name, remote := range remoteFiles {
		incoming.Files[name] = newCanonicalTorrentFile(incoming, remote)
	}
	if err := ReconcileCompletedTorrentEntry(existing, incoming); err != nil {
		t.Fatal(err)
	}
	merged := HandleExistingEntryMerge(existing, incoming)
	if len(merged.Files) != 2 {
		t.Fatalf("queue merge created duplicate identities: %#v", merged.Files)
	}
	if merged.Files[legacyName] == nil || merged.Files[legacyName].ID != "stable-canonical" {
		t.Fatalf("legacy canonical identity was not preserved: %#v", merged.Files)
	}
	if merged.Providers["primary"].Files[legacyName] == nil {
		t.Fatalf("completed placement did not reconcile to legacy key: %#v", merged.Providers["primary"].Files)
	}
	placement := merged.Providers["primary"]
	if !placement.AddedAt.Equal(queuedAt) || placement.Progress != 1.0 ||
		placement.DownloadedAt == nil || !placement.DownloadedAt.Equal(completed) {
		t.Fatalf("completed queue transfer metadata was lost: %#v", placement)
	}
}

func TestReconcileCompletedTorrentEntryPreservesMaterializedTitleAndPath(t *testing.T) {
	completed := time.Now()
	existing := &Entry{
		Protocol: config.ProtocolTorrent, InfoHash: "same-hash", Name: "Old",
		OriginalFilename: "Old Original",
		OutputName:       NewTorrentOutputName("Old", "same-hash"),
		CompletedAt:      &completed,
		ActiveProvider:   "primary",
		Files: map[string]*File{
			"movie.mkv": {Name: "movie.mkv", Path: "Release/movie.mkv", Size: 1},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				"movie.mkv": {Id: "existing", Path: "Release/movie.mkv"},
			}},
		},
	}
	beforePath := materializedTorrentRelativePath(existing, existing.Files["movie.mkv"].Path)
	beforeFolder := GetTorrentFolder(config.WebDavUseFileName, existing)
	incoming := &Entry{
		Protocol: config.ProtocolTorrent, InfoHash: existing.InfoHash, Name: "Release",
		OriginalFilename: "Release Original", ActiveProvider: "primary",
		Files: map[string]*File{
			"movie.mkv": {Name: "movie.mkv", Path: "Release/movie.mkv", Size: 1},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", ID: "transfer", Status: debridTypes.TorrentStatusDownloaded,
				Progress: 1.0, DownloadedAt: &completed, Files: map[string]*ProviderFile{
					"movie.mkv": {Id: "existing", Path: "Release/movie.mkv"},
				}},
		},
	}
	if err := ReconcileCompletedTorrentEntry(existing, incoming); err != nil {
		t.Fatal(err)
	}
	merged := HandleExistingEntryMerge(existing, incoming)
	if merged.Name != "Old" || merged.OriginalFilename != "Old Original" {
		t.Fatalf("completed queue title changed materialized identity: name=%q original=%q", merged.Name, merged.OriginalFilename)
	}
	if got := materializedTorrentRelativePath(merged, merged.Files["movie.mkv"].Path); got != beforePath {
		t.Fatalf("materialized relative path changed from %q to %q", beforePath, got)
	}
	if got := GetTorrentFolder(config.WebDavUseFileName, merged); got != beforeFolder {
		t.Fatalf("WebDAV folder changed from %q to %q", beforeFolder, got)
	}
}

func TestReconcileCompletedTorrentEntryMatchesDuplicateFilesAcrossReleaseRoots(t *testing.T) {
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "fallback-one", Path: "Season 01/Episode.mkv", Size: 11, Link: "provider://one"},
		{Id: "fallback-two", Path: "Season 02/Episode.mkv", Size: 12, Link: "provider://two"},
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Now()
	const (
		legacyOne = "Old Release/Season 01/Episode.mkv"
		legacyTwo = "Old Release/Season 02/Episode.mkv"
	)
	existing := &Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       "same-hash",
		CompletedAt:    &completed,
		ActiveProvider: "primary",
		Files: map[string]*File{
			legacyOne: {ID: "canonical-one", Name: legacyOne, Path: legacyOne, Size: 11},
			legacyTwo: {ID: "canonical-two", Name: legacyTwo, Path: legacyTwo, Size: 12},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				legacyOne: {Id: "primary-one", Path: "Old Release/Season 01/Episode.mkv"},
				legacyTwo: {Id: "primary-two", Path: "Old Release/Season 02/Episode.mkv"},
			}},
		},
	}
	incoming := &Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       existing.InfoHash,
		ActiveProvider: "fallback",
		Files:          make(map[string]*File),
		Providers:      make(map[string]*ProviderEntry),
	}
	if _, err := incoming.AddTorrentProvider(&debridTypes.Torrent{
		Id: "transfer", Debrid: "fallback", Status: debridTypes.TorrentStatusDownloaded, Files: remoteFiles,
	}); err != nil {
		t.Fatal(err)
	}
	for name, remote := range remoteFiles {
		incoming.Files[name] = newCanonicalTorrentFile(incoming, remote)
	}
	if err := ReconcileCompletedTorrentEntry(existing, incoming); err != nil {
		t.Fatal(err)
	}
	merged := HandleExistingEntryMerge(existing, incoming)
	if len(merged.Files) != 2 {
		t.Fatalf("queue merge created duplicate identities: %#v", merged.Files)
	}
	for name, id := range map[string]string{legacyOne: "fallback-one", legacyTwo: "fallback-two"} {
		if merged.Files[name] == nil {
			t.Fatalf("canonical file %q was not preserved: %#v", name, merged.Files)
		}
		providerFile := merged.Providers["fallback"].Files[name]
		if providerFile == nil || providerFile.Id != id {
			t.Fatalf("fallback placement did not map %q to %q: %#v", name, id, merged.Providers["fallback"].Files)
		}
	}
}

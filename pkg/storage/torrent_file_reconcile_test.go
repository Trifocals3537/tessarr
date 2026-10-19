package storage

import (
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestAddTorrentProviderReconcilesPersistedLogicalNames(t *testing.T) {
	const oldName = "Release/Season 01/Episode.mkv"
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "stable-1", Path: "Release/Season 01/Episode.mkv", Size: 11},
		{Id: "stable-2", Path: "Release/Season 02/Episode.mkv", Size: 12},
	})
	if err != nil {
		t.Fatal(err)
	}
	var newName string
	for name, file := range remoteFiles {
		if file.Id == "stable-1" {
			newName = name
		}
	}
	if newName == "" || newName == oldName {
		t.Fatalf("new logical name = %q", newName)
	}

	persisted := &File{ID: "file-id", Name: oldName, Path: "Release/Season 01/Episode.mkv", Size: 10}
	entry := &Entry{
		SavePath:   t.TempDir(),
		OutputName: "nonmaterialized",
		Files:      map[string]*File{oldName: persisted},
		Providers: map[string]*ProviderEntry{
			"primary": {
				Provider: "primary",
				Files: map[string]*ProviderFile{
					oldName: {Id: "stable-1", Path: "Release/Season 01/Episode.mkv"},
				},
			},
			"fallback": {
				Provider: "fallback",
				Files: map[string]*ProviderFile{
					oldName: {Id: "other-id", Path: "Release/Season 01/Episode.mkv"},
				},
			},
		},
	}
	remote := &debridTypes.Torrent{Id: "placement", Debrid: "primary", Files: remoteFiles}
	if _, err := entry.AddTorrentProvider(remote); err != nil {
		t.Fatal(err)
	}
	if entry.Files[oldName] != nil || entry.Files[newName] != persisted {
		t.Fatalf("canonical files = %#v", entry.Files)
	}
	if persisted.ID != "file-id" || persisted.Name != newName || persisted.Path != remoteFiles[newName].LocalPath() {
		t.Fatalf("reconciled canonical file = %#v", persisted)
	}
	if entry.Providers["primary"].Files[newName] == nil || entry.Providers["primary"].Files[oldName] != nil {
		t.Fatalf("primary placement files = %#v", entry.Providers["primary"].Files)
	}
	if entry.Providers["fallback"].Files[newName] == nil || entry.Providers["fallback"].Files[oldName] != nil {
		t.Fatalf("fallback placement files = %#v", entry.Providers["fallback"].Files)
	}
}

func TestAddTorrentProviderReconcilesByPathWithoutProviderID(t *testing.T) {
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Path: "Release/Why?.mkv",
		Size: 42,
	}})
	if err != nil {
		t.Fatal(err)
	}
	var newName string
	for name := range remoteFiles {
		newName = name
	}
	entry := &Entry{
		SavePath:   t.TempDir(),
		OutputName: "nonmaterialized",
		Files:      map[string]*File{"Why_.mkv": {Name: "Why_.mkv", Path: "Release/Why?.mkv"}},
		Providers: map[string]*ProviderEntry{
			"primary": {
				Provider: "primary",
				Files: map[string]*ProviderFile{
					"Why_.mkv": {Path: `release\why?.mkv`},
				},
			},
		},
	}
	if _, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "primary", Files: remoteFiles}); err != nil {
		t.Fatal(err)
	}
	if entry.Files[newName] == nil || entry.Files["Why_.mkv"] != nil {
		t.Fatalf("canonical files = %#v", entry.Files)
	}
}

func TestAddTorrentProviderDoesNotTransferUnrelatedFallbackLinkAcrossRename(t *testing.T) {
	const oldName = "Episode.mkv"
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "primary-1", Path: "Season 01/Episode.mkv", Link: "primary://one"},
		{Id: "primary-2", Path: "Season 02/Episode.mkv", Link: "primary://two"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var primaryName string
	for name, file := range remoteFiles {
		if file.Id == "primary-1" {
			primaryName = name
		}
	}
	entry := &Entry{
		SavePath:   t.TempDir(),
		OutputName: "nonmaterialized",
		Files: map[string]*File{
			oldName: {Name: oldName, Path: "Season 01/Episode.mkv"},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {
				Provider: "primary",
				Files: map[string]*ProviderFile{
					oldName: {Id: "primary-1", Path: "Season 01/Episode.mkv", Link: "primary://one"},
				},
			},
			"fallback": {
				Provider: "fallback",
				Files: map[string]*ProviderFile{
					oldName: {Id: "fallback-local-id", Path: "Different Release/Episode.mkv", Link: "fallback://wrong"},
				},
			},
		},
	}
	if _, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "primary", Files: remoteFiles}); err != nil {
		t.Fatal(err)
	}
	if entry.Files[primaryName] == nil || entry.Files[oldName] != nil {
		t.Fatalf("primary canonical rename failed: %#v", entry.Files)
	}
	if fallback := entry.Providers["fallback"].Files; len(fallback) != 0 {
		t.Fatalf("unrelated fallback mapping survived canonical rename: %#v", fallback)
	}
}

func TestAddTorrentProviderRejectsCanonicalRenameCollision(t *testing.T) {
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id:   "stable",
		Path: "Release/Why?.mkv",
	}})
	if err != nil {
		t.Fatal(err)
	}
	var newName string
	for name := range remoteFiles {
		newName = name
	}
	entry := &Entry{
		SavePath:   t.TempDir(),
		OutputName: "nonmaterialized",
		Files: map[string]*File{
			"Why_.mkv": {Name: "Why_.mkv"},
			newName:    {Name: newName},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {
				Provider: "primary",
				Files: map[string]*ProviderFile{
					"Why_.mkv": {Id: "stable", Path: "Release/Why?.mkv"},
				},
			},
		},
	}
	if _, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "primary", Files: remoteFiles}); err == nil {
		t.Fatal("canonical rename collision was accepted")
	}
	if entry.Files["Why_.mkv"] == nil || entry.Providers["primary"].Files["Why_.mkv"] == nil {
		t.Fatal("failed reconciliation mutated the persisted entry")
	}
}

func TestAddTorrentProviderAppliesRenameChainsAtomically(t *testing.T) {
	first := &File{ID: "first", Name: "old-a.mkv"}
	second := &File{ID: "second", Name: "new-a.mkv"}
	entry := &Entry{
		SavePath:   t.TempDir(),
		OutputName: "nonmaterialized",
		Files: map[string]*File{
			"old-a.mkv": first,
			"new-a.mkv": second,
		},
		Providers: map[string]*ProviderEntry{
			"primary": {
				Provider: "primary",
				Files: map[string]*ProviderFile{
					"old-a.mkv": {Id: "provider-a"},
					"new-a.mkv": {Id: "provider-b"},
				},
			},
		},
	}
	remote := &debridTypes.Torrent{
		Debrid: "primary",
		Files: map[string]debridTypes.File{
			"new-a.mkv":   {Id: "provider-a", Name: "new-a.mkv", Path: "new-a.mkv"},
			"final-b.mkv": {Id: "provider-b", Name: "final-b.mkv", Path: "final-b.mkv"},
		},
	}
	if _, err := entry.AddTorrentProvider(remote); err != nil {
		t.Fatal(err)
	}
	if len(entry.Files) != 2 || entry.Files["new-a.mkv"] != first || entry.Files["final-b.mkv"] != second {
		t.Fatalf("atomic rename chain = %#v", entry.Files)
	}
}

func TestAddTorrentProviderUpdatesPortablePathWithoutLogicalRename(t *testing.T) {
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id:   "stable",
		Path: "Release?/Episode.mkv",
		Size: 42,
	}})
	if err != nil {
		t.Fatal(err)
	}
	remote := remoteFiles["Episode.mkv"]
	entry := &Entry{
		InfoHash:   "hash",
		SavePath:   t.TempDir(),
		OutputName: "nonmaterialized",
		Files: map[string]*File{
			"Episode.mkv": {Name: "Episode.mkv", Path: "Release?/Episode.mkv", Size: 1},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {
				Provider: "primary",
				Files: map[string]*ProviderFile{
					"Episode.mkv": {Id: "stable", Path: "Release?/Episode.mkv"},
				},
			},
		},
	}
	if _, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "primary", Files: remoteFiles}); err != nil {
		t.Fatal(err)
	}
	if got := entry.Files["Episode.mkv"]; got == nil || got.Path != remote.LocalPath() || got.Size != 42 {
		t.Fatalf("same-key portable path was not refreshed: %#v", got)
	}
}

func TestAddTorrentProviderPreservesMaterializedOutputIdentity(t *testing.T) {
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "stable-1", Path: "Season 01/Episode.mkv", Size: 11},
		{Id: "stable-2", Path: "Season 02/Episode.mkv", Size: 12},
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Now()
	oldNames := map[string]string{
		"stable-1": "Season 01/Episode.mkv",
		"stable-2": "Season 02/Episode.mkv",
	}
	entry := &Entry{
		InfoHash:    "hash",
		CompletedAt: &completed,
		Files:       make(map[string]*File),
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: make(map[string]*ProviderFile)},
		},
	}
	for id, oldName := range oldNames {
		entry.Files[oldName] = &File{ID: "canonical-" + id, Name: oldName, Path: oldName}
		entry.Providers["primary"].Files[oldName] = &ProviderFile{Id: id, Path: oldName}
	}
	placement, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "primary", Files: remoteFiles})
	if err != nil {
		t.Fatal(err)
	}
	for id, oldName := range oldNames {
		file := entry.Files[oldName]
		if file == nil || file.Name != oldName || file.Path != oldName || file.ID != "canonical-"+id {
			t.Fatalf("materialized file %q moved: %#v", oldName, file)
		}
		if placement.Files[oldName] == nil || placement.Files[oldName].Id != id {
			t.Fatalf("placement did not map to stable file %q: %#v", oldName, placement.Files)
		}
	}
}

func TestAddTorrentProviderDisambiguatesNewFileFromMaterializedDirectory(t *testing.T) {
	initial, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id: "nested", Path: "Release/movie.mkv/extra.srt", Size: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Now()
	entry := &Entry{
		InfoHash:    "hash",
		CompletedAt: &completed,
		Files:       make(map[string]*File),
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: make(map[string]*ProviderFile)},
		},
	}
	for name, file := range initial {
		entry.Files[name] = &File{Name: name, Path: file.LocalPath(), Size: file.Size}
		entry.Providers["primary"].Files[name] = &ProviderFile{Id: file.Id, Path: file.Path}
	}
	expanded, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "nested", Path: "Release/movie.mkv/extra.srt", Size: 1},
		{Id: "file", Path: "Release/Movie.mkv", Size: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	placement, err := entry.AddTorrentProvider(&debridTypes.Torrent{
		Debrid: "primary", Files: expanded,
	})
	if err != nil {
		t.Fatal(err)
	}
	if nested := entry.Files["extra.srt"]; nested == nil || nested.Path != "Release/movie.mkv/extra.srt" {
		t.Fatalf("materialized nested file moved: %#v", nested)
	}
	var generatedName string
	for name, providerFile := range placement.Files {
		if providerFile != nil && providerFile.Id == "file" {
			generatedName = name
			break
		}
	}
	if generatedName == "" || strings.EqualFold(generatedName, "Movie.mkv") || path.Ext(generatedName) != ".mkv" {
		t.Fatalf("new colliding file was not safely disambiguated: %q", generatedName)
	}
	newFile := entry.Files[generatedName]
	if newFile == nil || path.Base(filepath.ToSlash(newFile.Path)) != generatedName {
		t.Fatalf("new canonical file does not match its output path: %#v", newFile)
	}
	if strings.HasPrefix(
		portableTorrentStoragePathKey(entry.Files["extra.srt"].Path),
		portableTorrentStoragePathKey(newFile.Path)+"/",
	) {
		t.Fatalf("new file %q still owns the materialized directory prefix", newFile.Path)
	}
}

func TestMaterializedOutputRetriesLogicalNameCollision(t *testing.T) {
	firstGenerated, err := disambiguateMaterializedTorrentComponent("foo", "foo", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Now()
	entry := &Entry{
		Protocol: config.ProtocolTorrent, InfoHash: "logical-collision", Name: "Release",
		OutputName:  NewTorrentOutputName("Release", "logical-collision"),
		CompletedAt: &completed,
		Files: map[string]*File{
			"bar.mkv":      {Name: "bar.mkv", Path: "foo/bar.mkv", Size: 1},
			firstGenerated: {Name: firstGenerated, Path: "other/" + firstGenerated, Size: 2},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				"bar.mkv":      {Id: "old-bar", Path: "foo/bar.mkv"},
				firstGenerated: {Id: "literal", Path: "other/" + firstGenerated},
			}},
		},
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "old-bar", Path: "foo/bar.mkv", Size: 1},
		{Id: "literal", Path: "other/" + firstGenerated, Size: 2},
		{Id: "new", Path: "foo", Size: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	placement, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "primary", Files: remoteFiles})
	if err != nil {
		t.Fatal(err)
	}
	var generatedName string
	for name, file := range placement.Files {
		if file != nil && file.Id == "new" {
			generatedName = name
		}
	}
	if generatedName == "" || generatedName == firstGenerated || entry.Files[generatedName] == nil ||
		entry.Files[generatedName].Path != generatedName {
		t.Fatalf("new file did not retry the occupied logical name: generated=%q files=%#v", generatedName, entry.Files)
	}
}

func TestMaterializedTorrentParentDisambiguationUsesDirectoryIdentity(t *testing.T) {
	first, err := disambiguateMaterializedTorrentComponent(
		"Extras", "Release/Extras/one.mkv", 1, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := disambiguateMaterializedTorrentComponent(
		"Extras", "Release/Extras/two.mkv", 1, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("files from one provider directory received different parent names: %q and %q", first, second)
	}
	firstLeaf, err := disambiguateMaterializedTorrentComponent(
		"one.mkv", "Release/Extras/one.mkv", 2, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondLeaf, err := disambiguateMaterializedTorrentComponent(
		"two.mkv", "Release/Extras/two.mkv", 2, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if firstLeaf == secondLeaf || path.Ext(firstLeaf) != ".mkv" || path.Ext(secondLeaf) != ".mkv" {
		t.Fatalf("file identities were not distinct with extensions preserved: %q and %q", firstLeaf, secondLeaf)
	}
}

func TestAddTorrentProviderReconcilesNewPlacementThroughExistingPaths(t *testing.T) {
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "fallback-1", Path: "Season 01/Episode.mkv", Size: 11},
		{Id: "fallback-2", Path: "Season 02/Episode.mkv", Size: 12},
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Now()
	entry := &Entry{
		InfoHash:    "hash",
		CompletedAt: &completed,
		Files: map[string]*File{
			"Season 01/Episode.mkv": {ID: "one", Name: "Season 01/Episode.mkv", Path: "Season 01/Episode.mkv"},
			"Season 02/Episode.mkv": {ID: "two", Name: "Season 02/Episode.mkv", Path: "Season 02/Episode.mkv"},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {
				Provider: "primary",
				Files: map[string]*ProviderFile{
					"Season 01/Episode.mkv": {Id: "primary-1", Path: "Season 01/Episode.mkv"},
					"Season 02/Episode.mkv": {Id: "primary-2", Path: "Season 02/Episode.mkv"},
				},
			},
		},
	}
	placement, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "fallback", Files: remoteFiles})
	if err != nil {
		t.Fatal(err)
	}
	if len(entry.Files) != 2 || len(placement.Files) != 2 {
		t.Fatalf("new placement duplicated canonical files: files=%#v placement=%#v", entry.Files, placement.Files)
	}
	for oldName := range entry.Files {
		if placement.Files[oldName] == nil {
			t.Fatalf("new placement did not reuse canonical key %q: %#v", oldName, placement.Files)
		}
	}
}

func TestAddTorrentProviderRejectsUnprovenLogicalKeyAcrossReleaseRoots(t *testing.T) {
	entry := &Entry{
		InfoHash: "hash",
		Files: map[string]*File{
			"Movie.mkv": {ID: "canonical", Name: "Movie.mkv", Path: "Release/Movie.mkv", Size: 123},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {
				Provider: "primary",
				Files: map[string]*ProviderFile{
					"Movie.mkv": {Id: "primary-id", Path: "Release/Movie.mkv"},
				},
			},
		},
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id: "fallback-id", Path: "Movie.mkv", Size: 123, Link: "fallback://movie",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "fallback", Files: remoteFiles}); err == nil {
		t.Fatal("unproven basename-only logical key was accepted across a release-root change")
	}
}

func TestAddTorrentProviderDoesNotBypassPathProofWithLogicalKey(t *testing.T) {
	completed := time.Now()
	entry := &Entry{
		InfoHash:    "same-hash",
		CompletedAt: &completed,
		Files: map[string]*File{
			"Episode.mkv": {Name: "Episode.mkv", Path: "Old Show/Season 01/Episode.mkv", Size: 123},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				"Episode.mkv": {Path: "Old Show/Season 01/Episode.mkv"},
			}},
		},
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id: "unrelated", Path: "Another Show/Season 02/Episode.mkv", Size: 123,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "fallback", Files: remoteFiles}); err == nil {
		t.Fatal("logical-key fallback bypassed provider-path identity proof")
	}
}

func TestAddTorrentProviderReconcilesDuplicateBasenamesAcrossNestedReleaseRoots(t *testing.T) {
	sourceFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "primary-one", Path: "downloads/Old Release/Season 01/Episode.mkv", Size: 11},
		{Id: "primary-two", Path: "downloads/Old Release/Season 02/Episode.mkv", Size: 12},
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Now()
	entry := &Entry{
		InfoHash:       "same-hash",
		CompletedAt:    &completed,
		ActiveProvider: "primary",
		Files:          make(map[string]*File),
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: make(map[string]*ProviderFile)},
		},
	}
	for name, file := range sourceFiles {
		entry.Files[name] = &File{Name: name, Path: file.LocalPath(), Size: file.Size}
		entry.Providers["primary"].Files[name] = &ProviderFile{Id: file.Id, Path: file.Path}
	}
	targetFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "fallback-one", Path: "Season 01/Episode.mkv", Size: 11},
		{Id: "fallback-two", Path: "Season 02/Episode.mkv", Size: 12},
	})
	if err != nil {
		t.Fatal(err)
	}
	placement, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "fallback", Files: targetFiles})
	if err != nil {
		t.Fatal(err)
	}
	if len(entry.Files) != 2 || len(placement.Files) != 2 {
		t.Fatalf("release-root change duplicated files: canonical=%#v placement=%#v", entry.Files, placement.Files)
	}
	for canonicalName := range sourceFiles {
		if placement.Files[canonicalName] == nil {
			t.Fatalf("fallback placement did not preserve canonical key %q: %#v", canonicalName, placement.Files)
		}
	}
}

func TestAddTorrentProviderRejectsAmbiguousDuplicatePathAcrossReleaseRoots(t *testing.T) {
	entry := &Entry{
		Files: map[string]*File{
			"downloads/Old Release/Season 01/Episode.mkv": {
				Name: "downloads/Old Release/Season 01/Episode.mkv", Path: "downloads/Old Release/Season 01/Episode.mkv", Size: 123,
			},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				"downloads/Old Release/Season 01/Episode.mkv": {Path: "downloads/Old Release/Season 01/Episode.mkv"},
			}},
		},
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "one", Path: "Old Release/Season 01/Episode.mkv", Size: 123},
		{Id: "two", Path: "Season 01/Episode.mkv", Size: 123},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "fallback", Files: remoteFiles}); err == nil {
		t.Fatal("ambiguous root-independent provider paths were accepted")
	}
}

func TestAddTorrentProviderDoesNotMatchNestedPathsByBasenameAlone(t *testing.T) {
	entry := &Entry{
		InfoHash: "same-hash",
		Files: map[string]*File{
			"Old Release/Season 01/Episode.mkv": {
				Name: "Old Release/Season 01/Episode.mkv", Path: "Old Release/Season 01/Episode.mkv", Size: 123,
			},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				"Old Release/Season 01/Episode.mkv": {Path: "Old Release/Season 01/Episode.mkv"},
			}},
		},
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id: "unrelated", Path: "Another Show/Season 02/Episode.mkv", Size: 123,
	}})
	if err != nil {
		t.Fatal(err)
	}
	placement, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "fallback", Files: remoteFiles})
	if err != nil {
		t.Fatal(err)
	}
	if len(entry.Files) != 2 || len(placement.Files) != 1 {
		t.Fatalf("unrelated same-basename file was reconciled: canonical=%#v placement=%#v", entry.Files, placement.Files)
	}
	if placement.Files["Old Release/Season 01/Episode.mkv"] != nil {
		t.Fatalf("fallback placement attached to unrelated canonical media: %#v", placement.Files)
	}
}

func TestAddTorrentProviderDoesNotMatchNestedPathToFlatByBasenameAlone(t *testing.T) {
	completed := time.Now()
	entry := &Entry{
		InfoHash:    "same-hash",
		CompletedAt: &completed,
		Files: map[string]*File{
			"Old Release/Season 01/Episode.mkv": {
				Name: "Old Release/Season 01/Episode.mkv", Path: "Old Release/Season 01/Episode.mkv", Size: 123,
			},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				"Old Release/Season 01/Episode.mkv": {Path: "Old Release/Season 01/Episode.mkv"},
			}},
		},
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id: "unrelated-flat", Path: "Episode.mkv", Size: 123,
	}})
	if err != nil {
		t.Fatal(err)
	}
	placement, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "fallback", Files: remoteFiles})
	if err != nil {
		t.Fatal(err)
	}
	if len(entry.Files) != 2 || len(placement.Files) != 1 {
		t.Fatalf("flat same-basename file was reconciled to a nested canonical path: canonical=%#v placement=%#v", entry.Files, placement.Files)
	}
	if placement.Files["Old Release/Season 01/Episode.mkv"] != nil {
		t.Fatalf("flat fallback attached to unrelated nested canonical media: %#v", placement.Files)
	}
}

func TestAddTorrentProviderDoesNotMatchUnprovenQualifiedSuffix(t *testing.T) {
	completed := time.Now()
	const oldName = "Release/Show A/Season 01/Episode.mkv"
	entry := &Entry{
		InfoHash:    "same-hash",
		CompletedAt: &completed,
		Files: map[string]*File{
			oldName: {Name: oldName, Path: oldName, Size: 123},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				oldName: {Path: oldName},
			}},
		},
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id: "different-show", Path: "Other/Show B/Season 01/Episode.mkv", Size: 123,
	}})
	if err != nil {
		t.Fatal(err)
	}
	placement, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "fallback", Files: remoteFiles})
	if err != nil {
		t.Fatal(err)
	}
	if len(entry.Files) != 2 || len(placement.Files) != 1 || placement.Files[oldName] != nil {
		t.Fatalf("unproven common suffix reconciled unrelated media: canonical=%#v placement=%#v", entry.Files, placement.Files)
	}
}

func TestAddTorrentProviderLetsUniqueIDOverrideOutputAliasAmbiguity(t *testing.T) {
	unsafeOnly, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id: "unsafe", Path: "Why?.mkv", Size: 11,
	}})
	if err != nil {
		t.Fatal(err)
	}
	var generatedLiteral string
	for _, file := range unsafeOnly {
		generatedLiteral = file.LocalPath()
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "unsafe", Path: "Why?.mkv", Size: 11, Link: "provider://unsafe"},
		{Id: "literal", Path: generatedLiteral, Size: 12, Link: "provider://literal"},
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Now()
	entry := &Entry{
		InfoHash:    "same-hash",
		CompletedAt: &completed,
		Files:       make(map[string]*File),
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: make(map[string]*ProviderFile)},
		},
	}
	for name, file := range remoteFiles {
		entry.Files[name] = &File{Name: name, Path: file.LocalPath(), Size: file.Size}
		entry.Providers["primary"].Files[name] = &ProviderFile{Id: file.Id, Path: file.Path, Link: "old://" + file.Id}
	}
	placement, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "primary", Files: remoteFiles})
	if err != nil {
		t.Fatalf("unique stable provider IDs were blocked by output aliases: %v", err)
	}
	if len(placement.Files) != 2 {
		t.Fatalf("provider refresh lost files: %#v", placement.Files)
	}
	for _, providerFile := range placement.Files {
		if providerFile == nil || !strings.HasPrefix(providerFile.Link, "provider://") {
			t.Fatalf("provider link was not refreshed through stable ID: %#v", placement.Files)
		}
	}
}

func TestAddTorrentProviderPrefersExactNativePathOverOutputAlias(t *testing.T) {
	unsafeOnly, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id: "unsafe", Path: "Why?.mkv", Size: 11,
	}})
	if err != nil {
		t.Fatal(err)
	}
	var generatedLiteral string
	for _, file := range unsafeOnly {
		generatedLiteral = file.LocalPath()
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "unsafe", Path: "Why?.mkv", Size: 11, Link: "provider://unsafe"},
		{Id: "literal", Path: generatedLiteral, Size: 12, Link: "provider://literal"},
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Now()
	const canonicalName = "existing-literal.mkv"
	entry := &Entry{
		InfoHash:    "same-hash",
		CompletedAt: &completed,
		Files: map[string]*File{
			canonicalName: {Name: canonicalName, Path: generatedLiteral, Size: 12},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				canonicalName: {Id: "primary-literal", Path: generatedLiteral},
			}},
		},
	}
	placement, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "fallback", Files: remoteFiles})
	if err != nil {
		t.Fatalf("exact native path was blocked by an output alias: %v", err)
	}
	if got := placement.Files[canonicalName]; got == nil || got.Id != "literal" {
		t.Fatalf("canonical file matched the wrong provider identity: %#v", placement.Files)
	}
	if len(entry.Files) != 2 || len(placement.Files) != 2 {
		t.Fatalf("provider files were lost after native-path reconciliation: canonical=%#v placement=%#v", entry.Files, placement.Files)
	}
}

func TestAddTorrentProviderKeepsLegacyOutputPathSeparateFromNativePath(t *testing.T) {
	unsafeOnly, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id: "unsafe", Path: "Why?.mkv", Size: 11,
	}})
	if err != nil {
		t.Fatal(err)
	}
	var generatedLiteral string
	for _, file := range unsafeOnly {
		generatedLiteral = file.LocalPath()
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "unsafe", Path: "Why?.mkv", Size: 11, Link: "provider://unsafe"},
		{Id: "literal", Path: generatedLiteral, Size: 12, Link: "provider://literal"},
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Now()
	const canonicalName = "legacy-output.mkv"
	entry := &Entry{
		InfoHash:    "same-hash",
		CompletedAt: &completed,
		Files: map[string]*File{
			canonicalName: {Name: canonicalName, Path: generatedLiteral, Size: 11},
		},
		Providers: make(map[string]*ProviderEntry),
	}
	placement, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "fallback", Files: remoteFiles})
	if err != nil {
		t.Fatalf("legacy output path did not reconcile through the output index: %v", err)
	}
	if got := placement.Files[canonicalName]; got == nil || got.Id != "unsafe" {
		t.Fatalf("legacy output path matched a provider-native collision: %#v", placement.Files)
	}
	if len(entry.Files) != 2 || len(placement.Files) != 2 {
		t.Fatalf("provider files were lost after legacy output reconciliation: canonical=%#v placement=%#v", entry.Files, placement.Files)
	}
}

func TestAddTorrentProviderPrefersRootedNativePathOverOutputAlias(t *testing.T) {
	const sourcePath = "Season 01/Episode_.mkv"
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "alias", Path: "Season 01/Episode?.mkv", Size: 11, Link: "provider://alias"},
		{Id: "rooted", Path: "Release/Season 01/Episode_.mkv", Size: 12, Link: "provider://rooted"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var aliasOutput string
	for name, file := range remoteFiles {
		if file.Id == "alias" {
			// Model an output path persisted under the earlier portable naming
			// rules, before duplicate-basename disambiguation was introduced.
			file.OutputPath = sourcePath
			remoteFiles[name] = file
			aliasOutput = file.LocalPath()
		}
	}
	if aliasOutput != sourcePath {
		t.Fatalf("test fixture did not create the required output alias: got %q", aliasOutput)
	}
	completed := time.Now()
	const canonicalName = "existing-rooted.mkv"
	entry := &Entry{
		InfoHash:    "same-hash",
		CompletedAt: &completed,
		Files: map[string]*File{
			canonicalName: {Name: canonicalName, Path: sourcePath, Size: 12},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				canonicalName: {Id: "primary-rooted", Path: sourcePath},
			}},
		},
	}
	placement, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "fallback", Files: remoteFiles})
	if err != nil {
		t.Fatalf("rooted native path did not take precedence over output alias: %v", err)
	}
	if got := placement.Files[canonicalName]; got == nil || got.Id != "rooted" {
		t.Fatalf("canonical file matched the output alias instead of rooted native path: %#v", placement.Files)
	}
	if len(entry.Files) != 2 || len(placement.Files) != 2 {
		t.Fatalf("provider files were lost after rooted native reconciliation: canonical=%#v placement=%#v", entry.Files, placement.Files)
	}
}

func TestUpdateTorrentProviderPreservesCanonicalDeletionState(t *testing.T) {
	entry := &Entry{
		InfoHash: "same-hash",
		Files: map[string]*File{
			"Episode.mkv": {Name: "Episode.mkv", Path: "Episode.mkv", Size: 123, Deleted: true},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				"Episode.mkv": {Id: "stable", Path: "Episode.mkv"},
			}},
		},
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id: "stable", Path: "Episode.mkv", Size: 123,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.AddTorrentProvider(&debridTypes.Torrent{
		Debrid: "primary", Files: remoteFiles,
	}); err != nil {
		t.Fatal(err)
	}
	if file := entry.Files["Episode.mkv"]; file == nil || !file.Deleted {
		t.Fatalf("provider refresh revived locally deleted canonical file: %#v", file)
	}
}

func TestAddTorrentProviderRejectsLogicalKeyMatchWithContradictorySize(t *testing.T) {
	entry := &Entry{
		Files: map[string]*File{"Episode.mkv": {Name: "Episode.mkv", Path: "Season 01/Episode.mkv", Size: 123}},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				"Episode.mkv": {Path: "Season 01/Episode.mkv"},
			}},
		},
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{{Path: "Different/Episode.mkv", Size: 456}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "fallback", Files: remoteFiles}); err == nil {
		t.Fatal("contradictory same-name fallback was accepted")
	}
}

func TestUpdateTorrentProviderStatePreservesIdentityAcrossIncompleteSnapshots(t *testing.T) {
	canonical := &File{Name: "Movie.mkv", Path: "Movie.mkv", Size: 123}
	providerFile := &ProviderFile{Id: "stable", Path: "Movie.mkv", Link: "provider://movie"}
	entry := &Entry{
		Files: map[string]*File{"Movie.mkv": canonical},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", ID: "old-id", Files: map[string]*ProviderFile{"Movie.mkv": providerFile}},
		},
	}
	placement, err := entry.UpdateTorrentProviderState(&debridTypes.Torrent{
		Id: "new-id", Debrid: "primary", Status: debridTypes.TorrentStatusDownloaded,
		Files: map[string]debridTypes.File{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if entry.Files["Movie.mkv"] != canonical || placement.Files["Movie.mkv"] != providerFile {
		t.Fatalf("empty snapshot discarded identities: files=%#v placement=%#v", entry.Files, placement.Files)
	}
	partial := map[string]debridTypes.File{
		"Other.mkv": {Name: "Other.mkv", Path: "Other.mkv", Size: 1},
	}
	if _, err := entry.UpdateTorrentProviderState(&debridTypes.Torrent{Debrid: "primary", Files: partial}); err != nil {
		t.Fatal(err)
	}
	if len(entry.Files) != 1 || len(entry.Providers["primary"].Files) != 1 {
		t.Fatalf("partial snapshot leaked into canonical state: files=%#v placement=%#v", entry.Files, entry.Providers["primary"].Files)
	}
}

func TestAddTorrentProviderPreservesOutputAfterRestartWhenArtifactExists(t *testing.T) {
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "stable-1", Path: "Season 01/Episode.mkv", Size: 11},
		{Id: "stable-2", Path: "Season 02/Episode.mkv", Size: 12},
	})
	if err != nil {
		t.Fatal(err)
	}
	entry := &Entry{
		Protocol:   config.ProtocolTorrent,
		InfoHash:   "hash",
		SavePath:   t.TempDir(),
		OutputName: "owned-output",
		Files: map[string]*File{
			"Season 01/Episode.mkv": {Name: "Season 01/Episode.mkv", Path: "Season 01/Episode.mkv", Size: 11},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				"Season 01/Episode.mkv": {Id: "stable-1", Path: "Season 01/Episode.mkv"},
			}},
		},
	}
	if err := os.MkdirAll(filepath.Clean(entry.DownloadPath()), 0o755); err != nil {
		t.Fatal(err)
	}
	placement, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "primary", Files: remoteFiles})
	if err != nil {
		t.Fatal(err)
	}
	if entry.Files["Season 01/Episode.mkv"] == nil || placement.Files["Season 01/Episode.mkv"] == nil {
		t.Fatalf("restart-time artifact identity moved: files=%#v placement=%#v", entry.Files, placement.Files)
	}
}

func TestAddTorrentProviderTreatsRelativeSavePathAsPotentiallyMaterialized(t *testing.T) {
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "stable-1", Path: "Season 01/Episode.mkv", Size: 11},
		{Id: "stable-2", Path: "Season 02/Episode.mkv", Size: 12},
	})
	if err != nil {
		t.Fatal(err)
	}
	const legacyName = "Season 01/Episode.mkv"
	entry := &Entry{
		InfoHash: "hash",
		SavePath: "relative-downloads",
		Files: map[string]*File{
			legacyName: {Name: legacyName, Path: legacyName, Size: 11},
		},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				legacyName: {Id: "stable-1", Path: legacyName},
			}},
		},
	}
	placement, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "primary", Files: remoteFiles})
	if err != nil {
		t.Fatal(err)
	}
	if entry.Files[legacyName] == nil || placement.Files[legacyName] == nil {
		t.Fatalf("relative legacy output identity moved: files=%#v placement=%#v", entry.Files, placement.Files)
	}
}

func TestAddTorrentProviderPreservesKnownSizeAcrossIncompleteMetadata(t *testing.T) {
	entry := &Entry{
		Files: map[string]*File{"Movie.mkv": {Name: "Movie.mkv", Path: "Movie.mkv", Size: 123}},
		Providers: map[string]*ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*ProviderFile{
				"Movie.mkv": {Id: "stable", Path: "Movie.mkv"},
			}},
		},
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{{Id: "stable", Path: "Movie.mkv", Size: 0}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "primary", Files: remoteFiles}); err != nil {
		t.Fatal(err)
	}
	if got := entry.Files["Movie.mkv"].Size; got != 123 {
		t.Fatalf("known size overwritten with %d", got)
	}
}

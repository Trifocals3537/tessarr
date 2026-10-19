package storage

import (
	"testing"
	"time"

	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestMergeFilesPreservesAndEnrichesNestedPathProvenance(t *testing.T) {
	added := time.Unix(1_700_000_000, 0)
	existing := map[string]*File{
		"episode.mkv": {Name: "episode.mkv", AddedOn: added},
	}
	incoming := map[string]*File{
		"episode.mkv": {
			Name:    "episode.mkv",
			Path:    "Season 01/episode.mkv",
			AddedOn: added,
		},
	}
	merged := mergeFiles(existing, incoming)
	if got := merged["episode.mkv"].Path; got != "Season 01/episode.mkv" {
		t.Fatalf("tied merge path = %q", got)
	}

	newerWithoutPath := &File{Name: "episode.mkv", AddedOn: added.Add(time.Minute)}
	merged = mergeFiles(incoming, map[string]*File{"episode.mkv": newerWithoutPath})
	if got := merged["episode.mkv"].Path; got != "Season 01/episode.mkv" {
		t.Fatalf("newer pathless merge dropped provenance: %q", got)
	}
	if newerWithoutPath.Path != "" {
		t.Fatal("merge mutated caller-owned incoming file")
	}
}

func TestCachedTorrentConversionPreservesNestedFilePath(t *testing.T) {
	cached := &CachedTorrent{
		ID:       "provider-id",
		InfoHash: "hash",
		Name:     "Release",
		Debrid:   "provider",
		Status:   string(debridTypes.TorrentStatusDownloaded),
		Files: map[string]*debridTypes.File{
			"episode.mkv": {
				Name: "episode.mkv",
				Path: "Release/Season 01/episode.mkv",
			},
		},
	}
	entry := cached.ToManagedTorrent()
	if got := entry.Files["episode.mkv"].Path; got != "Release/Season 01/episode.mkv" {
		t.Fatalf("converted nested path = %q", got)
	}
}

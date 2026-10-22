package storage

import (
	"errors"
	"testing"

	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestRestrictedTorrentScopeRefreshUsesStableIdentity(t *testing.T) {
	for _, scenario := range []string{"renamed", "missing", "ambiguous", "outside incomplete"} {
		t.Run(scenario, func(t *testing.T) {
			entry := &Entry{Name: "Example S01", FileScopeRestricted: true, Files: map[string]*File{"s1.mkv": {Name: "s1.mkv", Path: "Season 1/s1.mkv", Size: 100}},
				Providers: map[string]*ProviderEntry{"torbox": {ID: "42", Files: map[string]*ProviderFile{"s1.mkv": {Id: "s1", Path: "Season 1/s1.mkv"}}}}}
			remote := &debridTypes.Torrent{Debrid: "torbox", Id: "42", Name: "Example Complete Series", Status: debridTypes.TorrentStatusDownloaded, Files: map[string]debridTypes.File{
				"renamed.mkv": {Id: "s1", Name: "renamed.mkv", Path: "Season 1/s1.mkv", Size: 100, Link: "https://example.invalid/s1"},
				"s2.mkv":      {Id: "s2", Name: "s2.mkv", Path: "Season 2/s2.mkv", Size: 200, Link: "https://example.invalid/s2"},
			}}
			switch scenario {
			case "missing":
				delete(remote.Files, "renamed.mkv")
			case "ambiguous":
				duplicate := remote.Files["renamed.mkv"]
				duplicate.Name = "duplicate.mkv"
				remote.Files[duplicate.Name] = duplicate
			case "outside incomplete":
				outside := remote.Files["s2.mkv"]
				outside.Link = ""
				remote.Files[outside.Name] = outside
			}
			scoped, err := entry.ScopeTorrentSnapshot(remote)
			if scenario == "missing" || scenario == "ambiguous" {
				if err == nil {
					t.Fatal("unproven scope accepted")
				}
				if scenario == "missing" && !errors.Is(err, ErrIncompleteProviderFileScope) {
					t.Fatal(err)
				}
				if len(entry.Files) != 1 || entry.Files["s1.mkv"] == nil {
					t.Fatal("failed scope check mutated the original entry")
				}
				return
			}
			if err != nil || len(scoped.Files) != 1 || scoped.Size != 100 || scoped.Name != entry.Name {
				t.Fatalf("scope refresh failed: %+v %v", scoped, err)
			}
			placement, err := entry.AddTorrentProvider(remote)
			if err != nil || len(entry.Files) != 1 || entry.Files["s1.mkv"] == nil || len(placement.Files) != 1 || placement.Files["s1.mkv"].Id != "s1" {
				t.Fatalf("direct provider refresh expanded scope: entry=%+v placement=%+v err=%v", entry, placement, err)
			}
		})
	}
}

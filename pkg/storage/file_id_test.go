package storage

import (
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestFileIDsAreStableAcrossUpdates(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	store, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const infohash = "aabbccddeeff00112233445566778899aabbccdd"
	entry := &Entry{InfoHash: infohash, Name: "Movie.2023", Files: map[string]*File{
		"Movie.2023.mkv": {Name: "Movie.2023.mkv", Size: 100, InfoHash: infohash},
	}}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	id := entry.Files["Movie.2023.mkv"].ID
	if len(id) != fileIDBytes*2 {
		t.Fatalf("file id length = %d, want %d", len(id), fileIDBytes*2)
	}

	rebuilt := &Entry{InfoHash: infohash, Name: "Movie.2023", Files: map[string]*File{
		"Movie.2023.mkv": {Name: "Movie.2023.mkv", Size: 100, InfoHash: infohash},
		"Movie.2023.srt": {Name: "Movie.2023.srt", Size: 10, InfoHash: infohash},
	}}
	rebuilt.MainGeneration = entry.MainGeneration
	if err := store.AddOrUpdate(rebuilt); err != nil {
		t.Fatal(err)
	}
	if got := rebuilt.Files["Movie.2023.mkv"].ID; got != id {
		t.Fatalf("existing file ID changed: %q -> %q", id, got)
	}
	if rebuilt.Files["Movie.2023.srt"].ID == "" || rebuilt.Files["Movie.2023.srt"].ID == id {
		t.Fatal("new file did not receive a distinct ID")
	}

	loaded, err := store.Get(infohash)
	if err != nil {
		t.Fatal(err)
	}
	file, err := loaded.GetFileByID(id)
	if err != nil || file.Name != "Movie.2023.mkv" {
		t.Fatalf("GetFileByID(%q) = %v, %v", id, file, err)
	}
}

func TestAssignFileIDsRejectsDuplicates(t *testing.T) {
	entry := &Entry{Files: map[string]*File{
		"one.mkv": {Name: "one.mkv", ID: "duplicate"},
		"two.mkv": {Name: "two.mkv", ID: "duplicate"},
	}}
	if err := assignFileIDs(entry, nil); err == nil {
		t.Fatal("expected duplicate file ID to be rejected")
	}
}

func TestAssignFileIDsSurvivesProviderRename(t *testing.T) {
	previous := &Entry{
		Files: map[string]*File{"old-name.mkv": {ID: "stable-id", Name: "old-name.mkv", Size: 100}},
		Providers: map[string]*ProviderEntry{"torbox": {
			Provider: "torbox", Files: map[string]*ProviderFile{"old-name.mkv": {Id: "provider-file-42"}},
		}},
	}
	updated := &Entry{
		Files: map[string]*File{"new-name.mkv": {Name: "new-name.mkv", Size: 100}},
		Providers: map[string]*ProviderEntry{"torbox": {
			Provider: "torbox", Files: map[string]*ProviderFile{"new-name.mkv": {Id: "provider-file-42"}},
		}},
	}
	if err := assignFileIDs(updated, previous); err != nil {
		t.Fatal(err)
	}
	if got := updated.Files["new-name.mkv"].ID; got != "stable-id" {
		t.Fatalf("renamed file ID = %q, want stable-id", got)
	}
}

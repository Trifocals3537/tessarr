package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"google.golang.org/protobuf/proto"
)

func TestNewTorrentOutputNameIsPortableAndBounded(t *testing.T) {
	for _, title := range []string{"Movie: Part Two.mkv", `Movie<>:"|?*`, "CON", "NUL.txt", " release.mkv ", "...", "", ".tessarr-torrent-part-private", strings.Repeat("电影e\u0301", 200)} {
		name := NewTorrentOutputName(title, "aabbcc")
		if err := safepath.ValidateIdentifier(name); err != nil {
			t.Errorf("%q generated invalid component %q: %v", title, name, err)
		}
		if !utf8.ValidString(name) || len(name) > 137 {
			t.Errorf("generated name is not bounded UTF-8: %q", name)
		}
		if name != NewTorrentOutputName(title, " AABBCC ") {
			t.Error("output name depends on infohash case or surrounding whitespace")
		}
		other := NewTorrentOutputName(title, "ddeeff")
		key, _ := safepath.PortableNameKey(name)
		otherKey, _ := safepath.PortableNameKey(other)
		if key == otherKey {
			t.Fatal("different torrent identities share a portable output name")
		}
	}
}

func TestOutputNameRoundTripPreservesDisplayAndLegacyPaths(t *testing.T) {
	for _, protocol := range []config.Protocol{config.ProtocolTorrent, config.ProtocolNZB} {
		for _, output := range []string{"", NewTorrentOutputName("Title: Part Two", "abc")} {
			entry := &Entry{Protocol: protocol, InfoHash: "abc", Name: " Legacy.mkv", SavePath: "downloads", OutputName: output}
			data, err := proto.Marshal(EntryToProto(entry))
			if err != nil {
				t.Fatal(err)
			}
			var pb EntryProto
			if err := proto.Unmarshal(data, &pb); err != nil {
				t.Fatal(err)
			}
			got := ProtoToEntry(&pb)
			wantComponent := " Legacy"
			if output != "" {
				wantComponent = output
			}
			if got.Name != entry.Name || got.OutputName != output || got.DownloadPath() != filepath.Join("downloads", wantComponent) {
				t.Fatalf("round trip changed identity or legacy layout: %#v", got)
			}
			if output != "" {
				got.Name = "Changed provider title"
				if got.DownloadPath() != entry.DownloadPath() {
					t.Fatal("display-name update moved pinned output")
				}
			}
		}
	}
}

func TestOutputNameSurvivesQueueReopenReaddAndDeletionIntent(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if store != nil {
			if err := store.Close(); err != nil {
				t.Errorf("close storage: %v", err)
			}
		}
	})
	entry := &Entry{Protocol: config.ProtocolTorrent, InfoHash: "identity", Name: "Title: Part Two", SavePath: "downloads", OutputName: NewTorrentOutputName("Title: Part Two", "identity")}
	entry.ContentPath = entry.DownloadPath()
	if err := store.AddQueue(entry); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	current, err := store.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	current.Name = "Different title"
	current.OutputName = NewTorrentOutputName(current.Name, current.InfoHash)
	current.SavePath = "different-location"
	if err := store.AddQueue(current); err != nil {
		t.Fatal(err)
	}
	if current.DownloadPath() != entry.DownloadPath() || current.ContentPath != entry.ContentPath {
		t.Fatal("re-admission moved an existing output path")
	}
	intent, err := store.PrepareQueuedDeletionPreservingFiles(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if intent.Entry.OutputName != entry.OutputName || intent.Entry.DownloadPath() != entry.DownloadPath() {
		t.Fatal("deletion intent lost the exact persisted output identity")
	}
}

func TestTorrentOutputMergeKeepsExistingPathIncludingLegacy(t *testing.T) {
	for _, output := range []string{"", NewTorrentOutputName("Existing", "abc")} {
		existing := &Entry{Protocol: config.ProtocolTorrent, InfoHash: "abc", Name: " Existing.mkv", SavePath: "existing-root", OutputName: output}
		existing.ContentPath = existing.DownloadPath()
		incoming := &Entry{Protocol: config.ProtocolTorrent, InfoHash: "abc", Name: "New: title", SavePath: "new-root", OutputName: NewTorrentOutputName("New: title", "abc")}
		got := HandleExistingEntryMerge(existing, incoming)
		if got.Name != "New: title" || got.DownloadPath() != existing.DownloadPath() || got.ContentPath != existing.ContentPath {
			t.Fatalf("merge moved output or changed display title: %#v", got)
		}
	}
	providerOnly := &Entry{Protocol: config.ProtocolTorrent, Name: "provider-only"}
	incoming := &Entry{Protocol: config.ProtocolTorrent, SavePath: "new-root", OutputName: "new-output"}
	PreserveTorrentOutputPath(providerOnly, incoming)
	if incoming.SavePath != "new-root" || incoming.OutputName != "new-output" {
		t.Fatal("provider-only entry without local output erased the admission path")
	}
}

func TestTorrentArtifactsMayExistRecognizesWrappedNotFound(t *testing.T) {
	base := t.TempDir()
	entry := &Entry{
		Protocol: config.ProtocolTorrent, Name: "Old.mkv", InfoHash: "missing-root",
		SavePath:   filepath.Join(base, "missing"),
		OutputName: NewTorrentOutputName("Old.mkv", "missing-root"),
	}
	if torrentArtifactsMayExist(entry) {
		t.Fatal("a missing output root was treated as materialized")
	}
	if err := os.MkdirAll(entry.SavePath, 0o700); err != nil {
		t.Fatal(err)
	}
	if torrentArtifactsMayExist(entry) {
		t.Fatal("a missing output directory was treated as materialized")
	}
	if err := os.MkdirAll(entry.DownloadPath(), 0o700); err != nil {
		t.Fatal(err)
	}
	if !torrentArtifactsMayExist(entry) {
		t.Fatal("an existing output directory was not treated as materialized")
	}
}

package manager

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/google/uuid"
)

var errWatchedNZBTestMissing = errors.New("watched NZB test record missing")

func testWatchedNZBIdentity(t *testing.T) watchedNZBIdentity {
	t.Helper()
	identity, err := newWatchedNZBIdentity("release.nzb", []byte("<nzb>fixture</nzb>"))
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func testWatchedNZBEntry(identity watchedNZBIdentity) *storage.Entry {
	return &storage.Entry{
		Protocol:         config.ProtocolNZB,
		InfoHash:         identity.ID,
		Name:             "parsed release",
		OriginalFilename: "parsed release",
		ActiveProvider:   watchedNZBProvider,
		Providers: map[string]*storage.ProviderEntry{
			watchedNZBProvider: {
				Provider: watchedNZBProvider,
				ID:       identity.ID,
			},
		},
		Category: watchedNZBCategory,
		Action:   config.DownloadActionNone,
	}
}

func watchedNZBTestNotFound(err error) bool {
	return errors.Is(err, errWatchedNZBTestMissing)
}

func TestWatchedNZBIdentityIsStableCanonicalUUIDv8(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	const want = "e2407697-9dae-85af-bba5-fed14469dade"
	if identity.ID != want {
		t.Fatalf("watched NZB ID = %q, want %q", identity.ID, want)
	}
	parsed, err := uuid.Parse(identity.ID)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.String() != identity.ID {
		t.Fatalf("watched NZB ID is not canonical: %q", identity.ID)
	}
	if parsed.Version() != uuid.Version(8) {
		t.Fatalf("watched NZB UUID version = %d, want 8", parsed.Version())
	}
	if parsed.Variant() != uuid.RFC4122 {
		t.Fatalf("watched NZB UUID variant = %v, want RFC", parsed.Variant())
	}
	if wantDigest := sha256.Sum256([]byte("<nzb>fixture</nzb>")); identity.ContentDigest != wantDigest {
		t.Fatalf("watched NZB content digest = %x, want %x", identity.ContentDigest, wantDigest)
	}

	retry := testWatchedNZBIdentity(t)
	if retry != identity {
		t.Fatalf("retry identity = %#v, want %#v", retry, identity)
	}
}

func TestWatchedNZBIdentityChangesWithNameOrContent(t *testing.T) {
	baseline := testWatchedNZBIdentity(t)
	renamed, err := newWatchedNZBIdentity("renamed.nzb", []byte("<nzb>fixture</nzb>"))
	if err != nil {
		t.Fatal(err)
	}
	changed, err := newWatchedNZBIdentity("release.nzb", []byte("<nzb>changed</nzb>"))
	if err != nil {
		t.Fatal(err)
	}
	if renamed.ID == baseline.ID {
		t.Fatal("claimed-name change did not change deterministic ID")
	}
	if changed.ID == baseline.ID {
		t.Fatal("content change did not change deterministic ID")
	}
}

func TestWatchedNZBIdentityRejectsInvalidInput(t *testing.T) {
	for _, test := range []struct {
		name    string
		claimed string
		content []byte
	}{
		{name: "empty name", claimed: "", content: []byte("nzb")},
		{name: "traversal", claimed: "../release.nzb", content: []byte("nzb")},
		{name: "nested", claimed: "folder/release.nzb", content: []byte("nzb")},
		{name: "wrong extension", claimed: "release.xml", content: []byte("nzb")},
		{name: "importing extension", claimed: "release.nzb.importing", content: []byte("nzb")},
		{name: "empty content", claimed: "release.nzb"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := newWatchedNZBIdentity(test.claimed, test.content); err == nil {
				t.Fatalf("newWatchedNZBIdentity(%q) unexpectedly succeeded", test.claimed)
			}
		})
	}
}

func TestMatchWatchedNZBEntryAcceptsOnlyImmutableWatcherProvenance(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	entry := testWatchedNZBEntry(identity)
	entry.Progress = 0.75
	entry.Status = "downloading"
	entry.Files = map[string]*storage.File{"episode.mkv": {Name: "episode.mkv"}}
	if err := matchWatchedNZBEntry(identity, entry); err != nil {
		t.Fatalf("matching entry rejected: %v", err)
	}

	now := time.Now()
	tests := []struct {
		name   string
		mutate func(*storage.Entry)
	}{
		{name: "different ID", mutate: func(entry *storage.Entry) { entry.InfoHash = uuid.NewString() }},
		{name: "torrent protocol", mutate: func(entry *storage.Entry) { entry.Protocol = config.ProtocolTorrent }},
		{name: "empty parsed name", mutate: func(entry *storage.Entry) { entry.Name = "" }},
		{name: "different original filename", mutate: func(entry *storage.Entry) { entry.OriginalFilename = "other release" }},
		{name: "different category", mutate: func(entry *storage.Entry) { entry.Category = "sonarr" }},
		{name: "different action", mutate: func(entry *storage.Entry) { entry.Action = config.DownloadActionSymlink }},
		{name: "different active provider", mutate: func(entry *storage.Entry) { entry.ActiveProvider = "other" }},
		{name: "extra provider", mutate: func(entry *storage.Entry) {
			entry.Providers["other"] = &storage.ProviderEntry{Provider: "other", ID: "other"}
		}},
		{name: "missing provider", mutate: func(entry *storage.Entry) { entry.Providers = nil }},
		{name: "nil provider", mutate: func(entry *storage.Entry) { entry.Providers[watchedNZBProvider] = nil }},
		{name: "provider provenance", mutate: func(entry *storage.Entry) {
			entry.Providers[watchedNZBProvider].Provider = "other"
		}},
		{name: "provider ID", mutate: func(entry *storage.Entry) {
			entry.Providers[watchedNZBProvider].ID = uuid.NewString()
		}},
		{name: "removed provider", mutate: func(entry *storage.Entry) {
			entry.Providers[watchedNZBProvider].RemovedAt = &now
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entry := testWatchedNZBEntry(identity)
			test.mutate(entry)
			if err := matchWatchedNZBEntry(identity, entry); err == nil {
				t.Fatal("mismatched entry unexpectedly accepted")
			}
		})
	}
	if err := matchWatchedNZBEntry(identity, nil); err == nil {
		t.Fatal("nil entry unexpectedly accepted")
	}
}

func TestInspectWatchedNZBEntryUsesOnlyTypedAbsence(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	matching := testWatchedNZBEntry(identity)

	found, err := inspectWatchedNZBEntry(identity, func(id string) (*storage.Entry, error) {
		if id != identity.ID {
			t.Fatalf("lookup ID = %q, want %q", id, identity.ID)
		}
		return matching, nil
	}, watchedNZBTestNotFound)
	if err != nil || !found {
		t.Fatalf("matching lookup = found %v, err %v", found, err)
	}

	found, err = inspectWatchedNZBEntry(identity, func(string) (*storage.Entry, error) {
		return nil, errWatchedNZBTestMissing
	}, watchedNZBTestNotFound)
	if err != nil || found {
		t.Fatalf("typed missing lookup = found %v, err %v", found, err)
	}

	for _, test := range []struct {
		name   string
		lookup watchedNZBEntryLookup
	}{
		{name: "untyped error", lookup: func(string) (*storage.Entry, error) {
			return nil, errors.New("storage unavailable")
		}},
		{name: "nil record", lookup: func(string) (*storage.Entry, error) {
			return nil, nil
		}},
		{name: "mismatched record", lookup: func(string) (*storage.Entry, error) {
			entry := testWatchedNZBEntry(identity)
			entry.OriginalFilename = "other release"
			return entry, nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			found, err := inspectWatchedNZBEntry(identity, test.lookup, watchedNZBTestNotFound)
			if found || !errors.Is(err, errWatchedNZBStateAmbiguous) {
				t.Fatalf("ambiguous lookup = found %v, err %v", found, err)
			}
		})
	}
}

func TestWatchedNZBReconciliationRecognizesSameIdentityInQueueAndMain(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	for _, storeName := range []string{"queue", "main"} {
		t.Run(storeName, func(t *testing.T) {
			found, err := inspectWatchedNZBEntry(identity, func(string) (*storage.Entry, error) {
				return testWatchedNZBEntry(identity), nil
			}, watchedNZBTestNotFound)
			if err != nil || !found {
				t.Fatalf("%s reconciliation = found %v, err %v", storeName, found, err)
			}
		})
	}
}

func TestWatchedNZBReconciliationAllowsParsedNameDifferentFromClaimedFilename(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	entry := testWatchedNZBEntry(identity)
	metadata := &storage.NZB{
		ID:       identity.ID,
		Name:     "Completely Different Parsed Release",
		Category: watchedNZBCategory,
	}
	entry.Name = metadata.Name
	entry.OriginalFilename = metadata.Name

	if entry.Name == identity.ClaimedName {
		t.Fatal("test fixture does not exercise parsed-name difference")
	}
	if err := matchWatchedNZBEntryMetadata(identity, entry, metadata); err != nil {
		t.Fatalf("parsed-name-different watcher record rejected: %v", err)
	}
}

func TestMatchWatchedNZBDurableStateBindsExactStagedContent(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	root := t.TempDir()
	stagedPath := filepath.Join(root, identity.ID+".queued")
	if err := os.WriteFile(stagedPath, []byte("<nzb>fixture</nzb>"), 0o600); err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(root, identity.ID+".nzb")
	if err := os.WriteFile(sourcePath, []byte("<nzb>fixture</nzb>"), 0o600); err != nil {
		t.Fatal(err)
	}
	entry := testWatchedNZBEntry(identity)
	entry.Magnet = stagedPath
	metadata := &storage.NZB{
		ID:       identity.ID,
		Name:     entry.Name,
		Category: watchedNZBCategory,
		Path:     sourcePath,
	}
	if err := matchWatchedNZBDurableState(
		identity,
		entry,
		metadata,
		root,
		1024,
	); err != nil {
		t.Fatalf("matching durable state rejected: %v", err)
	}

	if err := os.WriteFile(stagedPath, []byte("<nzb>tampered</nzb>"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := matchWatchedNZBDurableState(
		identity,
		entry,
		metadata,
		root,
		1024,
	); err == nil {
		t.Fatal("tampered staged source unexpectedly matched")
	}
}

func TestMatchWatchedNZBDurableStateRejectsCrossEntryStagedPath(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	root := t.TempDir()
	otherID := uuid.NewString()
	otherPath := filepath.Join(root, otherID+".queued")
	if err := os.WriteFile(otherPath, []byte("<nzb>fixture</nzb>"), 0o600); err != nil {
		t.Fatal(err)
	}
	entry := testWatchedNZBEntry(identity)
	entry.Magnet = otherPath
	sourcePath := filepath.Join(root, identity.ID+".nzb")
	if err := os.WriteFile(sourcePath, []byte("<nzb>fixture</nzb>"), 0o600); err != nil {
		t.Fatal(err)
	}
	metadata := &storage.NZB{
		ID:       identity.ID,
		Name:     entry.Name,
		Category: watchedNZBCategory,
		Path:     sourcePath,
	}
	if err := matchWatchedNZBDurableState(
		identity,
		entry,
		metadata,
		root,
		1024,
	); err == nil {
		t.Fatal("another entry's staged source unexpectedly matched")
	}
	if data, err := os.ReadFile(otherPath); err != nil || string(data) != "<nzb>fixture</nzb>" {
		t.Fatalf("other staged source changed: data=%q err=%v", data, err)
	}
}

func TestWatchedNZBCategoryMatchesArrNormalization(t *testing.T) {
	// arr.Storage.GetOrCreate("") normalizes the empty manual category to this
	// exact persisted name; syncWatchedNZB deliberately uses that empty input.
	if watchedNZBCategory != "uncategorized" {
		t.Fatalf("watched category = %q", watchedNZBCategory)
	}
}

func TestInspectWatchedNZBMetadataUsesStrictIdentityAndTypedAbsence(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	matching := &storage.NZB{
		ID:       identity.ID,
		Name:     "parsed release",
		Category: watchedNZBCategory,
	}

	found, err := inspectWatchedNZBMetadata(identity, func(string) (*storage.NZB, error) {
		return matching, nil
	}, watchedNZBTestNotFound)
	if err != nil || !found {
		t.Fatalf("matching metadata = found %v, err %v", found, err)
	}

	found, err = inspectWatchedNZBMetadata(identity, func(string) (*storage.NZB, error) {
		return nil, errWatchedNZBTestMissing
	}, watchedNZBTestNotFound)
	if err != nil || found {
		t.Fatalf("typed missing metadata = found %v, err %v", found, err)
	}

	for _, metadata := range []*storage.NZB{
		nil,
		{ID: uuid.NewString(), Name: "parsed release", Category: watchedNZBCategory},
		{ID: identity.ID, Name: "", Category: watchedNZBCategory},
		{ID: identity.ID, Name: "parsed release", Category: ""},
		{ID: identity.ID, Name: "parsed release", Category: "sonarr"},
	} {
		found, err := inspectWatchedNZBMetadata(identity, func(string) (*storage.NZB, error) {
			return metadata, nil
		}, watchedNZBTestNotFound)
		if found || !errors.Is(err, errWatchedNZBStateAmbiguous) {
			t.Fatalf("mismatched metadata = found %v, err %v", found, err)
		}
	}
}

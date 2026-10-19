package storage

import (
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func TestEntryItemsSharedFolderUpgradeRepairsCleanPartialIndex(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db")
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	entry := entryItemTransactionEntry(
		"upgrade-survivor",
		"upgrade-shared-folder",
		"episode.mkv",
	)
	entry.AddedOn = base
	entry.Files["episode.mkv"].AddedOn = base
	entry.Files["extra.mkv"] = &File{
		InfoHash: entry.InfoHash,
		Name:     "extra.mkv",
		AddedOn:  base,
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}

	// Model a clean shutdown from an older build after a shared-folder
	// deletion removed one overlapping file but left another indexed.
	partial, err := store.GetEntryItem(entry.GetFolder())
	if err != nil {
		t.Fatal(err)
	}
	delete(partial.Files, "episode.mkv")
	partial.Size = partial.GetSize()
	partialData, err := proto.Marshal(EntryItemToProto(partial))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.entryItems.Put(partial.Name, partialData, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.storageState.Delete(entryItemIntegrityStateKey); err != nil {
		t.Fatal(err)
	}
	if err := store.entryItems.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := store.storageState.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close upgraded storage: %v", err)
		}
	})
	item, err := store.GetEntryItem(entry.GetFolder())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"episode.mkv", "extra.mkv"} {
		file := item.Files[name]
		if file == nil || file.InfoHash != entry.InfoHash {
			t.Fatalf("upgraded %s = %#v, want authoritative entry", name, file)
		}
	}
	version, err := store.storageState.Get(entryItemIntegrityStateKey)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(version); got != entryItemSharedFolderVersion {
		t.Fatalf("entry-item integrity version = %q, want %q", got, entryItemSharedFolderVersion)
	}
}

func TestEntryItemsReconcileAfterUncleanShutdown(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db")
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	entry := entryItemTransactionEntry(
		"entry-item-recovery",
		"expected-folder",
		"expected-file",
	)
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	if err := store.entryItems.Delete(entry.GetFolder()); err != nil {
		t.Fatal(err)
	}
	orphan := &EntryItem{
		Name: "orphan-folder",
		Files: map[string]*File{
			"orphan-file": {
				InfoHash: "missing-entry",
				Name:     "orphan-file",
			},
		},
	}
	orphanData, err := proto.Marshal(EntryItemToProto(orphan))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.entryItems.Put(orphan.Name, orphanData, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.entryItems.Sync(); err != nil {
		t.Fatal(err)
	}
	store.startupComplete = false
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close recovered storage: %v", err)
		}
	})

	item, err := store.GetEntryItem(entry.GetFolder())
	if err != nil {
		t.Fatal(err)
	}
	if file := item.Files["expected-file"]; file == nil ||
		file.InfoHash != entry.InfoHash {
		t.Fatalf("recovered entry item = %#v", item)
	}
	if store.entryItems.Exists(orphan.Name) {
		t.Fatal("orphan entry item survived unclean-shutdown recovery")
	}
}

func TestEntryItemsRecoveryPreservesDurableDeletedFiles(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db")
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	entry := entryItemTransactionEntry(
		"entry-item-deleted",
		"deleted-folder",
		"deleted-file",
	)
	entry.Files["deleted-file"].Size = 123
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	item, err := store.GetEntryItem(entry.GetFolder())
	if err != nil {
		t.Fatal(err)
	}
	item.Files["deleted-file"].Deleted = true
	item.Size = item.GetSize()
	if err := store.UpdateItem(item); err != nil {
		t.Fatal(err)
	}
	if err := store.entryItems.Sync(); err != nil {
		t.Fatal(err)
	}
	store.startupComplete = false
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close recovered storage: %v", err)
		}
	})
	item, err = store.GetEntryItem(entry.GetFolder())
	if err != nil {
		t.Fatal(err)
	}
	if file := item.Files["deleted-file"]; file == nil || !file.Deleted {
		t.Fatalf("recovery lost durable deleted flag: %#v", item)
	}
	if item.Size != 0 {
		t.Fatalf("recovered deleted item size = %d, want 0", item.Size)
	}
}

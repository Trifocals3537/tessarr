package storage

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
)

func TestMainEntryDeleteKeepsFolderServedByRemainingEntry(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	base := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)

	survivor := entryItemTransactionEntry("shared-survivor", "shared-folder", "episode.mkv")
	survivor.AddedOn = base
	survivor.Files["episode.mkv"].AddedOn = base
	doomed := entryItemTransactionEntry("shared-doomed", "shared-folder", "episode.mkv")
	doomed.AddedOn = base.Add(time.Minute)
	doomed.Files["episode.mkv"].AddedOn = base.Add(time.Minute)

	if err := store.AddOrUpdate(survivor); err != nil {
		t.Fatal(err)
	}
	if err := store.AddOrUpdate(doomed); err != nil {
		t.Fatal(err)
	}
	before, err := store.GetEntryItem("shared-folder")
	if err != nil {
		t.Fatal(err)
	}
	if got := before.Files["episode.mkv"].InfoHash; got != doomed.InfoHash {
		t.Fatalf("indexed file before delete = %q, want newer entry %q", got, doomed.InfoHash)
	}
	before.Files["episode.mkv"].Deleted = true
	before.Size = before.GetSize()
	if err := store.UpdateItem(before); err != nil {
		t.Fatal(err)
	}

	if err := store.Delete(doomed.InfoHash); err != nil {
		t.Fatal(err)
	}
	after, err := store.GetEntryItem("shared-folder")
	if err != nil {
		t.Fatalf("shared folder disappeared with a live entry: %v", err)
	}
	if got := after.Files["episode.mkv"].InfoHash; got != survivor.InfoHash {
		t.Fatalf("indexed file after delete = %q, want survivor %q", got, survivor.InfoHash)
	}
	if after.Files["episode.mkv"].Deleted {
		t.Fatal("deleted flag from removed identity leaked onto the surviving file")
	}
}

func TestMainEntryDeleteRemovesFolderAfterLastEntry(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	entry := entryItemTransactionEntry("last-entry", "last-folder", "episode.mkv")
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(entry.InfoHash); err != nil {
		t.Fatal(err)
	}
	if store.entryItems.Exists(entry.GetFolder()) {
		t.Fatal("folder survived deletion of its final authoritative entry")
	}
}

func TestConcurrentSharedFolderDeleteAndUpdatePreservesIndex(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	base := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)

	survivor := entryItemTransactionEntry("concurrent-survivor", "concurrent-folder", "episode.mkv")
	survivor.AddedOn = base
	survivor.Files["episode.mkv"].AddedOn = base
	doomed := entryItemTransactionEntry("concurrent-doomed", "concurrent-folder", "episode.mkv")
	doomed.AddedOn = base.Add(time.Minute)
	doomed.Files["episode.mkv"].AddedOn = base.Add(time.Minute)
	if err := store.AddOrUpdate(survivor); err != nil {
		t.Fatal(err)
	}
	if err := store.AddOrUpdate(doomed); err != nil {
		t.Fatal(err)
	}

	updated, err := store.Get(survivor.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	updated.Files["extra.mkv"] = &File{
		InfoHash: survivor.InfoHash,
		Name:     "extra.mkv",
		AddedOn:  base.Add(2 * time.Minute),
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		errs <- store.Delete(doomed.InfoHash)
	}()
	go func() {
		defer workers.Done()
		<-start
		errs <- store.AddOrUpdate(updated)
	}()
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent mutation: %v", err)
		}
	}

	item, err := store.GetEntryItem("concurrent-folder")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"episode.mkv", "extra.mkv"} {
		file := item.Files[name]
		if file == nil || file.InfoHash != survivor.InfoHash {
			t.Fatalf("indexed %s after concurrent mutation = %#v, want survivor", name, file)
		}
	}
}

func TestSharedFolderRebuildFailureRollsBackDeletion(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	doomed := entryItemTransactionEntry("rollback-doomed", "rollback-folder", "episode.mkv")
	if err := store.AddOrUpdate(doomed); err != nil {
		t.Fatal(err)
	}
	if err := store.entries.Put(
		"corrupt-survivor",
		[]byte("not-a-protobuf-entry"),
		&hybrid.EntryMeta{Name: doomed.GetFolder()},
	); err != nil {
		t.Fatal(err)
	}

	if err := store.Delete(doomed.InfoHash); err == nil {
		t.Fatal("deletion succeeded despite an indeterminate shared-folder survivor")
	}
	if _, err := store.Get(doomed.InfoHash); err != nil {
		t.Fatalf("failed shared-folder rebuild removed the authoritative entry: %v", err)
	}
	item, err := store.GetEntryItem(doomed.GetFolder())
	if err != nil {
		t.Fatalf("failed shared-folder rebuild removed the prior index: %v", err)
	}
	if file := item.Files["episode.mkv"]; file == nil || file.InfoHash != doomed.InfoHash {
		t.Fatalf("rolled-back entry item = %#v, want original entry", item)
	}
}

func TestMainEntryWriteFailureRollsBackNewEntryItem(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	store.startupComplete = false
	if err := store.entries.Close(); err != nil {
		t.Fatal(err)
	}

	entry := entryItemTransactionEntry("new-main", "new-folder", "new-file")
	if err := store.AddOrUpdate(entry); !errors.Is(err, hybrid.ErrStoreClosed) {
		t.Fatalf("AddOrUpdate() error = %v, want ErrStoreClosed", err)
	}
	if store.entryItems.Exists(entry.GetFolder()) {
		t.Fatal("failed authoritative write left a new secondary entry item")
	}
}

func TestMainEntrySecondaryFailureLeavesAuthoritativeRowUnchanged(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	entry := entryItemTransactionEntry("existing-main", "existing-folder", "old-file")
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	store.startupComplete = false
	if err := store.entryItems.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot.Files["new-file"] = &File{InfoHash: snapshot.InfoHash, Name: "new-file"}

	if err := store.AddOrUpdate(snapshot); !errors.Is(err, hybrid.ErrStoreClosed) {
		t.Fatalf("AddOrUpdate() error = %v, want ErrStoreClosed", err)
	}
	current, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := current.Files["new-file"]; exists {
		t.Fatal("secondary write failure committed the authoritative main update")
	}
}

func TestMainEntryDeleteSecondaryFailureRetainsAuthoritativeRow(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	entry := entryItemTransactionEntry("delete-main", "delete-folder", "file")
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	store.startupComplete = false
	if err := store.entryItems.Close(); err != nil {
		t.Fatal(err)
	}

	if err := store.Delete(entry.InfoHash); !errors.Is(err, hybrid.ErrStoreClosed) {
		t.Fatalf("Delete() error = %v, want ErrStoreClosed", err)
	}
	if _, err := store.Get(entry.InfoHash); err != nil {
		t.Fatalf("Get() after failed delete error = %v", err)
	}
	if _, found, err := store.loadMainEntryTombstone(entry.InfoHash); err != nil || found {
		t.Fatalf("delete tombstone after failed delete = found %v, error %v", found, err)
	}
}

func TestMainEntryFolderChangeRepairsBothEntryItems(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	entry := entryItemTransactionEntry("move-main", "old-folder", "file")
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Name = "new-folder"
	if err := store.AddOrUpdate(snapshot); err != nil {
		t.Fatal(err)
	}

	if store.entryItems.Exists("old-folder") {
		t.Fatal("old entry item survived a main-entry folder change")
	}
	item, err := store.GetEntryItem("new-folder")
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := item.Files["file"]; !exists {
		t.Fatal("new entry item is missing the moved main-entry file")
	}
}

func entryItemTransactionEntry(key, folder, fileName string) *Entry {
	return &Entry{
		InfoHash: key,
		Name:     folder,
		Files: map[string]*File{
			fileName: {
				InfoHash: key,
				Name:     fileName,
			},
		},
	}
}

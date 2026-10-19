package storage

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"google.golang.org/protobuf/proto"
)

func TestMain(m *testing.M) {
	configDir, err := os.MkdirTemp("", "tessarr-storage-test-")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "create storage test config directory: %v\n", err)
		os.Exit(1)
	}
	config.SetConfigPath(configDir)
	code := m.Run()
	_ = os.RemoveAll(configDir)
	os.Exit(code)
}

func TestDeleteQueuedHidesRecordAndRetainsIntentWhenCleanupFails(t *testing.T) {
	store := newDeleteTestStorage(t)
	entry := &Entry{InfoHash: "cleanup-fails", Name: "release"}
	putQueuedForDeleteTest(t, store, entry)
	cleanupErr := errors.New("cleanup failed")

	err := store.DeleteQueued(entry.InfoHash, func(*Entry) error {
		return cleanupErr
	})
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("DeleteQueued() error = %v, want cleanup error", err)
	}
	if _, err := store.GetQueued(entry.InfoHash); !IsQueuedEntryNotFound(err) {
		t.Fatalf("queued record remained visible after cleanup failure: %v", err)
	}
	if !store.queue.Exists(entry.InfoHash) {
		t.Fatal("raw queue row was retired before failed cleanup could be retried")
	}
	intents, err := store.QueuedDeletionIntents()
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 1 ||
		intents[0].InfoHash != entry.InfoHash ||
		!intents[0].UnrecoverableCleanupPending {
		t.Fatalf("durable cleanup intent = %#v", intents)
	}
}

func TestDeleteQueuedDeletesRecordAfterCleanupSucceeds(t *testing.T) {
	store := newDeleteTestStorage(t)
	entry := &Entry{InfoHash: "cleanup-succeeds", Name: "release"}
	putQueuedForDeleteTest(t, store, entry)
	called := false

	if err := store.DeleteQueued(entry.InfoHash, func(*Entry) error {
		called = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("cleanup was not called")
	}
	if _, err := store.GetQueued(entry.InfoHash); err == nil {
		t.Fatal("queued record still exists after successful cleanup")
	}
}

func TestDeleteWhereQueuedHidesCleanupFailuresAndRetiresSuccesses(t *testing.T) {
	store := newDeleteTestStorage(t)
	keep := &Entry{InfoHash: "keep-after-failure", Name: "keep"}
	remove := &Entry{InfoHash: "remove-after-success", Name: "remove"}
	putQueuedForDeleteTest(t, store, keep)
	putQueuedForDeleteTest(t, store, remove)
	cleanupErr := errors.New("cannot clean keep")

	err := store.DeleteWhereQueued(nil, func(entry *Entry) error {
		if entry.InfoHash == keep.InfoHash {
			return cleanupErr
		}
		return nil
	})
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("DeleteWhereQueued() error = %v, want cleanup error", err)
	}
	if _, err := store.GetQueued(keep.InfoHash); !IsQueuedEntryNotFound(err) {
		t.Fatalf("failed-cleanup record remained visible: %v", err)
	}
	if !store.queue.Exists(keep.InfoHash) {
		t.Fatal("failed-cleanup raw row was retired before retry")
	}
	if _, err := store.GetQueued(remove.InfoHash); err == nil {
		t.Fatal("successful-cleanup record still exists")
	}
}

func TestUpdateQueueExistingCannotRecreateDeletedRow(t *testing.T) {
	store := newDeleteTestStorage(t)
	entry := &Entry{InfoHash: "no-resurrection", Name: "release"}
	if err := store.AddQueue(entry); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteQueued(entry.InfoHash, nil); err != nil {
		t.Fatal(err)
	}

	entry.Progress = 0.5
	err := store.UpdateQueueExisting(entry)
	if !IsQueuedEntryNotFound(err) {
		t.Fatalf("UpdateQueueExisting() error = %v, want queued-entry-not-found", err)
	}
	if _, err := store.GetQueued(entry.InfoHash); !IsQueuedEntryNotFound(err) {
		t.Fatalf("deleted row was recreated: %v", err)
	}
}

func TestGetQueuedRejectsPayloadWhoseIdentityDoesNotMatchKey(t *testing.T) {
	store := newDeleteTestStorage(t)
	putQueuedAtKeyForDeleteTest(t, store, "entry-a", &Entry{
		InfoHash: "entry-b",
		Name:     "wrong-owner",
	})

	if _, err := store.GetQueued("entry-a"); !errors.Is(err, ErrQueuedEntryIdentityMismatch) {
		t.Fatalf("GetQueued() error = %v, want identity mismatch", err)
	}
	if !store.queue.Exists("entry-a") {
		t.Fatal("identity-mismatched row was mutated")
	}

	cleanupCalled := false
	err := store.DeleteQueued("entry-a", func(*Entry) error {
		cleanupCalled = true
		return nil
	})
	if !errors.Is(err, ErrQueuedEntryIdentityMismatch) {
		t.Fatalf("DeleteQueued() error = %v, want identity mismatch", err)
	}
	if cleanupCalled {
		t.Fatal("DeleteQueued() ran cleanup for an identity-mismatched row")
	}
	if !store.queue.Exists("entry-a") {
		t.Fatal("DeleteQueued() removed an identity-mismatched row")
	}
}

func TestDeleteWhereQueuedRejectsIdentityMismatchBeforeAnyCleanup(t *testing.T) {
	store := newDeleteTestStorage(t)
	valid := &Entry{InfoHash: "valid-entry", Name: "valid"}
	putQueuedForDeleteTest(t, store, valid)
	putQueuedAtKeyForDeleteTest(t, store, "entry-a", &Entry{
		InfoHash: "entry-b",
		Name:     "wrong-owner",
	})

	cleanupCalls := 0
	err := store.DeleteWhereQueued(nil, func(*Entry) error {
		cleanupCalls++
		return nil
	})
	if !errors.Is(err, ErrQueuedEntryIdentityMismatch) {
		t.Fatalf("DeleteWhereQueued() error = %v, want identity mismatch", err)
	}
	if cleanupCalls != 0 {
		t.Fatalf("cleanup called %d times before corrupt scan failed, want 0", cleanupCalls)
	}
	if !store.queue.Exists("entry-a") || !store.queue.Exists(valid.InfoHash) {
		t.Fatal("batch deletion mutated rows after identity mismatch")
	}
}

func TestGetAndDeleteMainEntryRejectPayloadWhoseIdentityDoesNotMatchKey(t *testing.T) {
	store := newDeleteTestStorage(t)
	putMainAtKeyForDeleteTest(t, store, "entry-a", &Entry{
		InfoHash: "entry-b",
		Name:     "wrong-owner",
	})

	if _, err := store.Get("entry-a"); !errors.Is(err, ErrEntryIdentityMismatch) {
		t.Fatalf("Get() error = %v, want identity mismatch", err)
	}
	if err := store.Delete("entry-a"); !errors.Is(err, ErrEntryIdentityMismatch) {
		t.Fatalf("Delete() error = %v, want identity mismatch", err)
	}
	if !store.entries.Exists("entry-a") {
		t.Fatal("Delete() removed an identity-mismatched main row")
	}
}

func TestMainEntryScansDoNotSilentlySkipIdentityMismatch(t *testing.T) {
	store := newDeleteTestStorage(t)
	putMainAtKeyForDeleteTest(t, store, "entry-a", &Entry{
		InfoHash: "entry-b",
		Name:     "wrong-owner",
	})

	if _, err := store.List(nil); !errors.Is(err, ErrEntryIdentityMismatch) {
		t.Fatalf("List() error = %v, want identity mismatch", err)
	}
	called := false
	err := store.ForEach(func(*Entry) error {
		called = true
		return nil
	})
	if !errors.Is(err, ErrEntryIdentityMismatch) {
		t.Fatalf("ForEach() error = %v, want identity mismatch", err)
	}
	if called {
		t.Fatal("ForEach() passed an identity-mismatched entry to its callback")
	}
}

func TestGetMainEntryReturnsAuthoritativeNotFound(t *testing.T) {
	store := newDeleteTestStorage(t)
	if _, err := store.Get("missing"); !IsEntryNotFound(err) {
		t.Fatalf("Get() error = %v, want authoritative entry-not-found", err)
	}
}

func newDeleteTestStorage(t *testing.T) *Storage {
	t.Helper()
	store, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	})
	return store
}

func putQueuedForDeleteTest(t *testing.T, store *Storage, entry *Entry) {
	t.Helper()
	putQueuedAtKeyForDeleteTest(t, store, entry.InfoHash, entry)
}

func putQueuedAtKeyForDeleteTest(t *testing.T, store *Storage, key string, entry *Entry) {
	t.Helper()
	data, err := proto.Marshal(EntryToProto(entry))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.queue.Put(strings.ToLower(key), data, nil); err != nil {
		t.Fatal(err)
	}
}

func putMainAtKeyForDeleteTest(t *testing.T, store *Storage, key string, entry *Entry) {
	t.Helper()
	data, err := proto.Marshal(EntryToProto(entry))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.entries.Put(key, data, nil); err != nil {
		t.Fatal(err)
	}
}

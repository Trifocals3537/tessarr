package storage

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func TestQueuedDeletionCrashPhasesStayHiddenAcrossRestart(t *testing.T) {
	tests := []struct {
		name  string
		phase queueDeletionPhase
	}{
		{name: "prepared", phase: queueDeletionPrepared},
		{name: "cleanup started", phase: queueDeletionCleanupStarted},
		{name: "row retired", phase: queueDeletionRowRetired},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "db")
			store, err := NewStorage(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			entry := &Entry{InfoHash: "restart-delete", Name: "release"}
			if err := store.AddQueue(entry); err != nil {
				t.Fatal(err)
			}
			oldIncarnation := entry.QueueIncarnation
			intent, err := store.PrepareQueuedDeletion(entry.InfoHash, false)
			if err != nil {
				t.Fatal(err)
			}
			if test.phase != queueDeletionPrepared {
				if err := store.StartQueuedDeletionCleanup(
					intent.InfoHash,
					intent.QueueIncarnation,
				); err != nil {
					t.Fatal(err)
				}
			}
			if test.phase == queueDeletionRowRetired {
				if err := store.RetireQueuedDeletionRow(
					intent.InfoHash,
					intent.QueueIncarnation,
				); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			store, err = NewStorage(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := store.Close(); err != nil {
					t.Errorf("close reopened storage: %v", err)
				}
			}()

			if _, err := store.GetQueued(entry.InfoHash); !IsQueuedEntryNotFound(err) ||
				!errors.Is(err, ErrQueuedEntryDeleting) {
				t.Fatalf("restarted GetQueued() error = %v", err)
			}
			if got, err := store.FilterQueued(nil); err != nil || len(got) != 0 {
				t.Fatalf("restarted FilterQueued() = %#v, %v", got, err)
			}
			if err := store.UpdateQueueExisting(entry); !errors.Is(err, ErrQueuedEntryDeleting) {
				t.Fatalf("restarted UpdateQueueExisting() error = %v", err)
			}
			replacement := &Entry{InfoHash: entry.InfoHash, Name: "replacement"}
			if err := store.AddQueue(replacement); !errors.Is(err, ErrQueuedEntryDeleting) {
				t.Fatalf("restarted AddQueue() error = %v", err)
			}

			intents, err := store.QueuedDeletionIntents()
			if err != nil {
				t.Fatal(err)
			}
			if len(intents) != 1 ||
				intents[0].QueueIncarnation != oldIncarnation ||
				intents[0].Entry.Name != "release" ||
				intents[0].Phase != string(test.phase) {
				t.Fatalf("recovered intent = %#v", intents)
			}

			if test.phase == queueDeletionPrepared {
				if err := store.StartQueuedDeletionCleanup(
					intent.InfoHash,
					intent.QueueIncarnation,
				); err != nil {
					t.Fatal(err)
				}
			}
			if test.phase != queueDeletionRowRetired {
				if err := store.RetireQueuedDeletionRow(
					intent.InfoHash,
					intent.QueueIncarnation,
				); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.CompleteQueuedDeletion(
				intent.InfoHash,
				intent.QueueIncarnation,
			); err != nil {
				t.Fatal(err)
			}
			if err := store.AddQueue(replacement); err != nil {
				t.Fatalf("replacement AddQueue() after completion: %v", err)
			}
			if replacement.QueueIncarnation == oldIncarnation {
				t.Fatal("replacement reused deleted queue incarnation")
			}
		})
	}
}

func TestQueuedDeletionPersistsIndependentPlacementSnapshot(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db")
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	queueEntry := &Entry{
		InfoHash: "placement-snapshot",
		Name:     "queued",
		Providers: map[string]*ProviderEntry{
			"queue": {Provider: "fake", ID: "queue-placement"},
		},
	}
	if err := store.AddQueue(queueEntry); err != nil {
		t.Fatal(err)
	}
	mainSnapshot := &Entry{
		InfoHash: queueEntry.InfoHash,
		Name:     "main",
		Providers: map[string]*ProviderEntry{
			"main": {Provider: "fake", ID: "main-placement"},
		},
	}
	intent, err := store.PrepareQueuedDeletion(
		queueEntry.InfoHash,
		true,
		mainSnapshot,
	)
	if err != nil {
		t.Fatal(err)
	}
	mainSnapshot.Providers["main"].ID = "mutated-after-snapshot"
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close reopened storage: %v", err)
		}
	}()
	intents, err := store.QueuedDeletionIntents()
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 1 || !intents[0].PlacementCleanupPending {
		t.Fatalf("recovered placement intent = %#v", intents)
	}
	if got := intents[0].PlacementEntries[0].Providers["main"].ID; got != "main-placement" {
		t.Fatalf("durable placement snapshot ID = %q", got)
	}
	if intents[0].QueueIncarnation != intent.QueueIncarnation {
		t.Fatal("placement intent changed queue incarnation across restart")
	}
}

func TestQueuedDeletionPersistsFilePreservationPolicy(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db")
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	entry := &Entry{InfoHash: "preserve-files-restart", Name: "release"}
	if err := store.AddQueue(entry); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PrepareQueuedDeletionPreservingFiles(entry.InfoHash); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close reopened storage: %v", err)
		}
	}()
	intents, err := store.QueuedDeletionIntents()
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 1 || !intents[0].PreserveFiles {
		t.Fatalf("recovered deletion intent = %#v, want preserve-files policy", intents)
	}
}

func TestQueuedDeletionRejectsDifferentDurableIncarnation(t *testing.T) {
	store := newDeleteTestStorage(t)
	entry := &Entry{InfoHash: "incarnation-mismatch", Name: "release"}
	if err := store.AddQueue(entry); err != nil {
		t.Fatal(err)
	}
	intent, err := store.PrepareQueuedDeletion(entry.InfoHash, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartQueuedDeletionCleanup(
		intent.InfoHash,
		intent.QueueIncarnation,
	); err != nil {
		t.Fatal(err)
	}

	replacement := *entry
	replacement.QueueIncarnation = "different-incarnation"
	if err := store.writeQueueRaw(&replacement, true); err != nil {
		t.Fatal(err)
	}
	if err := store.queue.Sync(); err != nil {
		t.Fatal(err)
	}

	err = store.RetireQueuedDeletionRow(
		intent.InfoHash,
		intent.QueueIncarnation,
	)
	if !errors.Is(err, ErrQueuedDeletionIdentityMismatch) {
		t.Fatalf("RetireQueuedDeletionRow() error = %v", err)
	}
	if !store.queue.Exists(entry.InfoHash) {
		t.Fatal("identity-mismatched replacement was deleted")
	}
	if _, err := store.GetQueued(entry.InfoHash); !IsQueuedEntryNotFound(err) {
		t.Fatalf("identity-mismatched replacement became visible: %v", err)
	}
}

func TestQueuedDeletionBlocksConcurrentReadsAndMutations(t *testing.T) {
	store := newDeleteTestStorage(t)
	entry := &Entry{InfoHash: "concurrent-delete", Name: "release"}
	if err := store.AddQueue(entry); err != nil {
		t.Fatal(err)
	}
	intent, err := store.PrepareQueuedDeletion(entry.InfoHash, false)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 24
	errs := make(chan error, goroutines*3)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(3)
		go func() {
			defer wg.Done()
			_, err := store.GetQueued(entry.InfoHash)
			if !IsQueuedEntryNotFound(err) ||
				!errors.Is(err, ErrQueuedEntryDeleting) {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			update := *entry
			update.Progress = 0.5
			err := store.UpdateQueueExisting(&update)
			if !errors.Is(err, ErrQueuedEntryDeleting) {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			err := store.AddQueue(&Entry{
				InfoHash: entry.InfoHash,
				Name:     "replacement",
			})
			if !errors.Is(err, ErrQueuedEntryDeleting) {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent operation escaped durable deletion: %v", err)
	}

	if err := store.StartQueuedDeletionCleanup(
		intent.InfoHash,
		intent.QueueIncarnation,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.RetireQueuedDeletionRow(
		intent.InfoHash,
		intent.QueueIncarnation,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteQueuedDeletion(
		intent.InfoHash,
		intent.QueueIncarnation,
	); err != nil {
		t.Fatal(err)
	}
}

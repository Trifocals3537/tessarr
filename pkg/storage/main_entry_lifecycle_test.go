package storage

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestMainEntryDeleteRejectsConcurrentAndStaleMutations(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	entry := mainLifecycleTestEntry("Race-Key", "provider-a")
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("seed entry: %v", err)
	}
	stale, err := store.Get("RACE-KEY")
	if err != nil {
		t.Fatalf("get stale snapshot: %v", err)
	}

	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- store.DeleteWithCleanup("race-key", func(*Entry) error {
			close(cleanupStarted)
			<-releaseCleanup
			return nil
		})
	}()
	<-cleanupStarted

	stale.Name = "must-not-win"
	if err := store.AddOrUpdate(stale); !errors.Is(err, ErrEntryDeleting) {
		t.Fatalf("AddOrUpdate during delete error = %v, want ErrEntryDeleting", err)
	}
	if err := store.BatchAddOrUpdate([]*Entry{stale}); !errors.Is(err, ErrEntryDeleting) {
		t.Fatalf("BatchAddOrUpdate during delete error = %v, want ErrEntryDeleting", err)
	}
	if _, err := store.Get("race-key"); !errors.Is(err, ErrEntryDeleting) {
		t.Fatalf("read during delete error = %v, want ErrEntryDeleting", err)
	}
	if _, err := store.Exists("race-key"); !errors.Is(err, ErrEntryDeleting) {
		t.Fatalf("Exists during delete error = %v, want ErrEntryDeleting", err)
	}
	visible := false
	if err := store.ForEach(func(candidate *Entry) error {
		if candidate.InfoHash == "race-key" {
			visible = true
		}
		return nil
	}); err != nil {
		t.Fatalf("ForEach during delete: %v", err)
	}
	if visible {
		t.Fatal("ForEach exposed a row with durable deleting intent")
	}

	close(releaseCleanup)
	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteWithCleanup: %v", err)
	}
	if err := store.AddOrUpdate(stale); !errors.Is(err, ErrStaleEntryGeneration) {
		t.Fatalf("stale AddOrUpdate error = %v, want ErrStaleEntryGeneration", err)
	}
	blind := mainLifecycleTestEntry("race-key", "provider-a")
	if err := store.AddOrUpdate(blind); !errors.Is(err, ErrStaleEntryGeneration) {
		t.Fatalf("blind recreation error = %v, want ErrStaleEntryGeneration", err)
	}
	if _, err := store.Get("race-key"); !IsEntryNotFound(err) {
		t.Fatalf("deleted row Get error = %v, want ErrEntryNotFound", err)
	}
}

func TestMainEntryFailedDeleteDoesNotAdvanceOrRetire(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	entry := mainLifecycleTestEntry("delete-fails", "provider-a")
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("seed entry: %v", err)
	}
	snapshot, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}

	cleanupErr := errors.New("cleanup failed")
	err = store.DeleteWithCleanup(entry.InfoHash, func(*Entry) error {
		tombstone, found, tombstoneErr := store.loadMainEntryTombstone(entry.InfoHash)
		if tombstoneErr != nil {
			t.Fatalf("load deleting tombstone in cleanup: %v", tombstoneErr)
		}
		if !found || tombstone.Phase != mainEntryTombstoneDeleting {
			t.Fatalf("cleanup began without durable deleting tombstone: %#v", tombstone)
		}
		return cleanupErr
	})
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("DeleteWithCleanup error = %v, want cleanup failure", err)
	}

	snapshot.Name = "still-current"
	if err := store.AddOrUpdate(snapshot); err != nil {
		t.Fatalf("pre-delete snapshot rejected after failed deletion: %v", err)
	}
	got, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatalf("get retained row: %v", err)
	}
	if got.Name != "still-current" {
		t.Fatalf("retained row name = %q, want still-current", got.Name)
	}
	if _, found, err := store.loadMainEntryTombstone(entry.InfoHash); err != nil || found {
		t.Fatalf("failed deletion left tombstone: found=%v err=%v", found, err)
	}
}

func TestMainEntryEveryMutationInvalidatesOtherSnapshots(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	entry := mainLifecycleTestEntry("optimistic", "provider-a")
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("seed entry: %v", err)
	}
	first, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Get(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}

	first.Name = "winner"
	if err := store.AddOrUpdate(first); err != nil {
		t.Fatalf("winning update: %v", err)
	}
	second.Name = "lost-update"
	if err := store.AddOrUpdate(second); !errors.Is(err, ErrStaleEntryGeneration) {
		t.Fatalf("second update error = %v, want ErrStaleEntryGeneration", err)
	}
}

func TestMainEntryProviderRediscoveryRequiresLaterPresence(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	entry := mainLifecycleTestEntry("rediscovery", "provider-a")
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("seed entry: %v", err)
	}

	preDeleteSnapshot := store.BeginProviderSnapshot()
	if err := store.Delete(entry.InfoHash); err != nil {
		t.Fatalf("delete entry: %v", err)
	}

	candidate := mainLifecycleTestEntry(entry.InfoHash, "provider-a")
	if err := store.PrepareProviderEntry(candidate, "provider-a", preDeleteSnapshot); !errors.Is(err, ErrEntryRediscoveryPending) {
		t.Fatalf("pre-delete provider response error = %v, want ErrEntryRediscoveryPending", err)
	}

	stalePresence := store.BeginProviderSnapshot()
	if err := store.ObserveProviderSnapshot(
		"provider-a",
		stalePresence,
		map[string]struct{}{entry.InfoHash: {}},
	); err != nil {
		t.Fatalf("observe stale presence: %v", err)
	}
	if err := store.PrepareProviderEntry(candidate, "provider-a", stalePresence); !errors.Is(err, ErrEntryRediscoveryPending) {
		t.Fatalf("presence without absence error = %v, want ErrEntryRediscoveryPending", err)
	}

	absence := store.BeginProviderSnapshot()
	if err := store.ObserveProviderSnapshot("provider-a", absence, nil); err != nil {
		t.Fatalf("observe provider absence: %v", err)
	}
	if err := store.PrepareProviderEntry(candidate, "provider-a", absence); !errors.Is(err, ErrEntryRediscoveryPending) {
		t.Fatalf("same-snapshot recreation error = %v, want ErrEntryRediscoveryPending", err)
	}

	reappearance := store.BeginProviderSnapshot()
	if err := store.PrepareProviderEntry(candidate, "provider-a", reappearance); err != nil {
		t.Fatalf("authorize later reappearance: %v", err)
	}
	if err := store.AddOrUpdate(candidate); err != nil {
		t.Fatalf("persist later reappearance: %v", err)
	}
	if _, err := store.Get(entry.InfoHash); err != nil {
		t.Fatalf("get rediscovered row: %v", err)
	}
}

func TestMainEntryRetirementAndAbsenceSurviveRestart(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	entry := mainLifecycleTestEntry("restart-handshake", "provider-a")
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(entry.InfoHash); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	presence := store.BeginProviderSnapshot()
	candidate := mainLifecycleTestEntry(entry.InfoHash, "provider-a")
	if err := store.PrepareProviderEntry(candidate, "provider-a", presence); !errors.Is(err, ErrEntryRediscoveryPending) {
		t.Fatalf("restart presence error = %v, want ErrEntryRediscoveryPending", err)
	}
	absence := store.BeginProviderSnapshot()
	if err := store.ObserveProviderSnapshot("provider-a", absence, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	reappearance := store.BeginProviderSnapshot()
	if err := store.PrepareProviderEntry(candidate, "provider-a", reappearance); err != nil {
		t.Fatalf("durable absence did not authorize later presence: %v", err)
	}
	if err := store.AddOrUpdate(candidate); err != nil {
		t.Fatalf("persist reappearance after restart: %v", err)
	}
	if _, found, err := store.loadMainEntryTombstone(entry.InfoHash); err != nil || found {
		t.Fatalf("rediscovery tombstone retained: found=%v err=%v", found, err)
	}
}

func TestMainEntryRecoveryFinishesInterruptedDelete(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	entry := mainLifecycleTestEntry("interrupted-delete", "provider-a")
	if err := store.AddOrUpdateDurable(entry); err != nil {
		t.Fatal(err)
	}
	ref, err := store.acquireMainEntryState(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	ref.state.mu.Lock()
	if err := store.persistMainEntryTombstone(
		ref.key,
		ref.state,
		"",
		mainEntryTombstoneDeleting,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.entryTombstones.Sync(); err != nil {
		t.Fatal(err)
	}
	ref.state.mu.Unlock()
	ref.release()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.Get(entry.InfoHash); !IsEntryNotFound(err) {
		t.Fatalf("interrupted deletion row error = %v, want ErrEntryNotFound", err)
	}
	if _, found, err := store.loadMainEntryTombstone(entry.InfoHash); err != nil || !found {
		t.Fatalf("retirement tombstone missing after recovery: found=%v err=%v", found, err)
	}
}

func TestMainEntryRecoveryKeepsDurablePendingReplacement(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	entry := mainLifecycleTestEntry("pending-replacement", "provider-a")
	if err := store.AddOrUpdateDurable(entry); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(entry.InfoHash); err != nil {
		t.Fatal(err)
	}
	absence := store.BeginProviderSnapshot()
	if err := store.ObserveProviderSnapshot("provider-a", absence, nil); err != nil {
		t.Fatal(err)
	}
	reappearance := store.BeginProviderSnapshot()
	replacement := mainLifecycleTestEntry(entry.InfoHash, "provider-a")
	if err := store.PrepareProviderEntry(replacement, "provider-a", reappearance); err != nil {
		t.Fatal(err)
	}

	ref, err := store.acquireMainEntryState(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	ref.state.mu.Lock()
	if err := store.persistMainEntryTombstone(
		ref.key,
		ref.state,
		"",
		mainEntryTombstoneReplacementPending,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.entryTombstones.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := store.addOrUpdateMainRaw(replacement); err != nil {
		t.Fatal(err)
	}
	if err := store.entries.Sync(); err != nil {
		t.Fatal(err)
	}
	ref.state.mu.Unlock()
	ref.release()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.Get(entry.InfoHash); err != nil {
		t.Fatalf("recovery removed durable pending replacement: %v", err)
	}
	if _, found, err := store.loadMainEntryTombstone(entry.InfoHash); err != nil || found {
		t.Fatalf("pending replacement tombstone retained: found=%v err=%v", found, err)
	}
}

func TestMainEntryForEachSnapshotsAndCaseShareOneGeneration(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	entry := mainLifecycleTestEntry("Mixed-Case-Key", "provider-a")
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	if entry.InfoHash != "mixed-case-key" {
		t.Fatalf("durable key = %q, want normalized lowercase", entry.InfoHash)
	}

	var snapshot *Entry
	if err := store.ForEach(func(candidate *Entry) error {
		if candidate.InfoHash == entry.InfoHash {
			snapshot = candidate
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if snapshot == nil || snapshot.MainGeneration == 0 {
		t.Fatal("ForEach did not bind a main generation")
	}
	if err := store.Delete("MIXED-CASE-KEY"); err != nil {
		t.Fatal(err)
	}
	if err := store.AddOrUpdate(snapshot); !errors.Is(err, ErrStaleEntryGeneration) {
		t.Fatalf("ForEach stale write error = %v, want ErrStaleEntryGeneration", err)
	}
}

func TestMainEntryGenerationFieldsAreNotSerialized(t *testing.T) {
	entry := mainLifecycleTestEntry("nonserialized", "provider-a")
	entry.MainGeneration = 42
	entry.MainProviderSnapshot = 43
	entry.MainMutationProvider = "provider-a"
	entry.MainReimportIncarnation = "transient"
	entry.QueueIncarnation = "durable"
	roundTrip := ProtoToEntry(EntryToProto(entry))
	if roundTrip.MainGeneration != 0 ||
		roundTrip.MainProviderSnapshot != 0 ||
		roundTrip.MainMutationProvider != "" ||
		roundTrip.MainReimportIncarnation != "" {
		t.Fatalf(
			"transient lifecycle fields survived protobuf round trip: %#v",
			roundTrip,
		)
	}
	if roundTrip.QueueIncarnation != entry.QueueIncarnation {
		t.Fatalf(
			"QueueIncarnation = %q, want durable value %q",
			roundTrip.QueueIncarnation,
			entry.QueueIncarnation,
		)
	}
}

func TestMainEntryMissingProbeStateIsGarbageCollected(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	if _, err := store.Get("never-seen"); !IsEntryNotFound(err) {
		t.Fatalf("Get error = %v, want ErrEntryNotFound", err)
	}
	store.mainEntries.mu.Lock()
	defer store.mainEntries.mu.Unlock()
	if got := len(store.mainEntries.states); got != 0 {
		t.Fatalf("transient lifecycle states = %d, want 0", got)
	}
}

func TestPrepareProviderEntryAllowsGenuineFirstDiscovery(t *testing.T) {
	store := newMainLifecycleTestStorage(t)

	snapshot := store.BeginProviderSnapshot()
	entry := mainLifecycleTestEntry("FIRST-DISCOVERY", "torbox")
	if err := store.PrepareProviderEntry(entry, "torbox", snapshot); err != nil {
		t.Fatalf("PrepareProviderEntry() error = %v", err)
	}
	if entry.MainGeneration != 0 {
		t.Fatalf("MainGeneration = %d, want zero for a genuine first discovery", entry.MainGeneration)
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate() after first discovery error = %v", err)
	}
}

func TestExplicitQueueReimportSurvivesRestart(t *testing.T) {
	tests := []struct {
		name     string
		protocol config.Protocol
	}{
		{name: "torrent", protocol: config.ProtocolTorrent},
		{name: "nzb", protocol: config.ProtocolNZB},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			key := "same-key-" + tt.name

			store, err := NewStorage(dir)
			if err != nil {
				t.Fatal(err)
			}
			main := &Entry{InfoHash: key, Name: tt.name, Protocol: tt.protocol}
			if err := store.AddOrUpdateDurable(main); err != nil {
				t.Fatal(err)
			}
			oldQueue := &Entry{InfoHash: key, Name: tt.name, Protocol: tt.protocol}
			if err := store.AddQueue(oldQueue); err != nil {
				t.Fatal(err)
			}
			oldIncarnation := oldQueue.QueueIncarnation
			if oldIncarnation == "" {
				t.Fatal("old queue incarnation is empty")
			}
			if err := store.DeleteWithCleanup(key, func(*Entry) error {
				return store.DeleteQueued(key, nil)
			}); err != nil {
				t.Fatalf("DeleteWithCleanup() error = %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			store, err = NewStorage(dir)
			if err != nil {
				t.Fatal(err)
			}
			replacement := &Entry{InfoHash: key, Name: tt.name + "-again", Protocol: tt.protocol}
			if err := store.AddQueue(replacement); err != nil {
				t.Fatalf("AddQueue() replacement error = %v", err)
			}
			if replacement.QueueIncarnation == "" ||
				replacement.QueueIncarnation == oldIncarnation {
				t.Fatalf(
					"replacement queue incarnation = %q, old = %q",
					replacement.QueueIncarnation,
					oldIncarnation,
				)
			}
			newIncarnation := replacement.QueueIncarnation
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			store, err = NewStorage(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()

			staleWorker := &Entry{
				InfoHash:         key,
				Name:             "stale",
				Protocol:         tt.protocol,
				QueueIncarnation: oldIncarnation,
			}
			if err := store.PrepareQueuedReplacement(staleWorker); !errors.Is(err, ErrEntryRediscoveryPending) {
				t.Fatalf(
					"PrepareQueuedReplacement(stale) error = %v, want ErrEntryRediscoveryPending",
					err,
				)
			}

			current, err := store.GetQueued(key)
			if err != nil {
				t.Fatal(err)
			}
			if current.QueueIncarnation != newIncarnation {
				t.Fatalf(
					"durable queue incarnation = %q, want %q",
					current.QueueIncarnation,
					newIncarnation,
				)
			}
			if err := store.PrepareQueuedReplacement(current); err != nil {
				t.Fatalf("PrepareQueuedReplacement(current) error = %v", err)
			}
			if err := store.AddOrUpdateDurable(current); err != nil {
				t.Fatalf("AddOrUpdateDurable(reimport) error = %v", err)
			}
			if _, err := store.Get(key); err != nil {
				t.Fatalf("Get() reimported main entry error = %v", err)
			}
			if _, found, err := store.loadMainEntryTombstone(key); err != nil || found {
				t.Fatalf("main tombstone after reimport = found %v, error %v", found, err)
			}
		})
	}
}

func TestExplicitQueueReimportSurvivesCrashBeforeMainWrite(t *testing.T) {
	dir := t.TempDir()
	key := "queued-replacement-crash"

	store, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	main := &Entry{
		InfoHash: key,
		Name:     "original",
		Protocol: config.ProtocolNZB,
	}
	if err := store.AddOrUpdateDurable(main); err != nil {
		t.Fatal(err)
	}
	originalQueue := &Entry{
		InfoHash: key,
		Name:     "original",
		Protocol: config.ProtocolNZB,
	}
	if err := store.AddQueue(originalQueue); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteWithCleanup(key, func(*Entry) error {
		return store.DeleteQueued(key, nil)
	}); err != nil {
		t.Fatal(err)
	}

	replacement := &Entry{
		InfoHash: key,
		Name:     "replacement",
		Protocol: config.ProtocolNZB,
	}
	if err := store.AddQueue(replacement); err != nil {
		t.Fatal(err)
	}
	if err := store.PrepareQueuedReplacement(replacement); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash after AddOrUpdate has durably recorded the authorized
	// replacement transition but before the replacement main row is written.
	ref, err := store.acquireMainEntryState(key)
	if err != nil {
		t.Fatal(err)
	}
	ref.state.mu.Lock()
	if err := store.persistMainEntryTombstone(
		ref.key,
		ref.state,
		"",
		mainEntryTombstoneReplacementPending,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.entryTombstones.Sync(); err != nil {
		t.Fatal(err)
	}
	ref.state.mu.Unlock()
	ref.release()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	current, err := store.GetQueued(key)
	if err != nil {
		t.Fatal(err)
	}
	if current.QueueIncarnation != replacement.QueueIncarnation {
		t.Fatalf(
			"recovered queue incarnation = %q, want %q",
			current.QueueIncarnation,
			replacement.QueueIncarnation,
		)
	}
	if err := store.PrepareQueuedReplacement(current); err != nil {
		t.Fatalf("PrepareQueuedReplacement() after recovery error = %v", err)
	}
	if err := store.AddOrUpdateDurable(current); err != nil {
		t.Fatalf("AddOrUpdateDurable() after recovery error = %v", err)
	}
	if _, err := store.Get(key); err != nil {
		t.Fatalf("Get() recovered replacement error = %v", err)
	}
}

func TestExistingQueueRowDoesNotAuthorizeRetiredMainEntry(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	key := "pre-retirement-queue"

	if err := store.AddOrUpdate(&Entry{InfoHash: key, Name: "main"}); err != nil {
		t.Fatal(err)
	}
	queued := &Entry{InfoHash: key, Name: "queued"}
	if err := store.AddQueue(queued); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(key); err != nil {
		t.Fatal(err)
	}

	if err := store.PrepareQueuedReplacement(queued); !errors.Is(err, ErrEntryRediscoveryPending) {
		t.Fatalf(
			"PrepareQueuedReplacement(pre-retirement row) error = %v, want ErrEntryRediscoveryPending",
			err,
		)
	}
	if err := store.AddQueue(&Entry{InfoHash: key, Name: "overwrite"}); !errors.Is(err, ErrEntryRediscoveryPending) {
		t.Fatalf("AddQueue(existing row) error = %v, want ErrEntryRediscoveryPending", err)
	}
}

func newMainLifecycleTestStorage(t *testing.T) *Storage {
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

func mainLifecycleTestEntry(key, provider string) *Entry {
	return &Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       key,
		Name:           fmt.Sprintf("entry-%s", key),
		ActiveProvider: provider,
		Providers: map[string]*ProviderEntry{
			provider: {
				Provider: provider,
				ID:       "provider-id",
			},
		},
		Files: map[string]*File{},
	}
}

package storage

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestHandoffCompletionRetriesDurability(t *testing.T) {
	s, err := NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := handoffTestEntry()
	if err = s.AddQueue(e); err != nil {
		t.Fatal(err)
	}
	if err = s.AuthorizeUncachedHandoff(e); err != nil {
		t.Fatal(err)
	}
	intent, err := s.PrepareQBitQueuedDeletion(e.InfoHash, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.StartQueuedDeletionCleanup(intent.InfoHash, intent.QueueIncarnation); err != nil {
		t.Fatal(err)
	}
	if err = s.RetireQueuedDeletionRow(intent.InfoHash, intent.QueueIncarnation); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("completion sync unavailable")
	s.handoffSyncForTest = func() error { return injected }
	for i := 0; i < 3; i++ {
		if err = s.CompleteQueuedDeletion(intent.InfoHash, intent.QueueIncarnation); !errors.Is(err, injected) {
			t.Errorf("completion %d bypassed barrier: %v", i, err)
		}
	}
	s.handoffSyncForTest = nil
	if err = s.CompleteQueuedDeletion(intent.InfoHash, intent.QueueIncarnation); err != nil {
		t.Fatal(err)
	}
}

func TestQueueOnlyAdminRejectsClientIntent(t *testing.T) {
	s, err := NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := handoffTestEntry()
	if err = s.AddQueue(e); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PrepareQBitQueuedDeletion(e.InfoHash, true); err != nil {
		t.Fatal(err)
	}
	if _, err = s.PrepareQueuedDeletion(e.InfoHash, true); err == nil {
		t.Fatal("queue-only admin accepted immutable benign intent")
	}
}

func TestPendingAdminBlocksQueueCompletion(t *testing.T) {
	s, err := NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := handoffTestEntry()
	if err = s.AddQueue(e); err != nil {
		t.Fatal(err)
	}
	if err = s.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}
	intent, err := s.PrepareQBitQueuedDeletion(e.InfoHash, true)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.StartQueuedDeletionCleanup(intent.InfoHash, intent.QueueIncarnation); err != nil {
		t.Fatal(err)
	}
	offline := errors.New("provider offline")
	err = s.DeleteWithCleanup(e.InfoHash, func(main *Entry) error {
		if err := s.PrepareMainDeletionPlacements(main); err != nil {
			return err
		}
		return offline
	})
	if !errors.Is(err, offline) {
		t.Fatal(err)
	}
	if err = s.RetireQueuedDeletionRow(intent.InfoHash, intent.QueueIncarnation); err != nil {
		t.Fatal(err)
	}
	if err = s.CompleteQueuedDeletion(intent.InfoHash, intent.QueueIncarnation); err == nil {
		t.Fatal("pending admin obligation discarded queue intent")
	}
}

func TestHandoffRevocationRetriesDurability(t *testing.T) {
	s, err := NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := handoffTestEntry()
	if err = s.AddQueue(e); err != nil {
		t.Fatal(err)
	}
	if err = s.AuthorizeUncachedHandoff(e); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("sync unavailable")
	s.handoffSyncForTest = func() error { return injected }
	e.Progress = .25
	for i := 0; i < 3; i++ {
		if err = s.UpdateQueueExisting(e); !errors.Is(err, injected) {
			t.Errorf("update %d bypassed failed durability: %v", i, err)
		}
	}
	e.Progress = 0
	if err = s.UpdateQueueExisting(e); !errors.Is(err, injected) {
		t.Errorf("reverted evidence bypassed barrier: %v", err)
	}
	if _, err = s.PrepareQBitQueuedDeletion(e.InfoHash, true); !errors.Is(err, injected) {
		t.Errorf("deletion bypassed failed durability: %v", err)
	}
	if err = s.AuthorizeUncachedHandoff(e); !errors.Is(err, injected) {
		t.Errorf("reauthorization bypassed barrier: %v", err)
	}
	if _, err = s.storageState.Get(handoffAuthorizationPrefix + e.InfoHash); err == nil {
		t.Error("reauthorization restored proof before revocation was durable")
	}
	s.handoffSyncForTest = nil
	if t.Failed() {
		return
	}
	if err = s.UpdateQueueExisting(e); err != nil {
		t.Fatal(err)
	}
	intent, err := s.PrepareQBitQueuedDeletion(e.InfoHash, true)
	if err != nil {
		t.Fatal(err)
	}
	if intent.PlacementCleanupPending {
		t.Fatal("revoked proof reused")
	}
}

func TestHandoffFailedAuthorizationCannotBeUsed(t *testing.T) {
	s, err := NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := handoffTestEntry()
	if err = s.AddQueue(e); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("sync unavailable")
	s.handoffSyncForTest = func() error { return injected }
	if err = s.AuthorizeUncachedHandoff(e); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if _, err = s.PrepareQBitQueuedDeletion(e.InfoHash, true); !errors.Is(err, injected) {
		t.Fatalf("unconfirmed authorization used: %v", err)
	}
}

func TestPreservingRetryRejectsDestructiveIntent(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(map[bool]string{false: "provider-pending", true: "files-pending"}[clean], func(t *testing.T) {
			s, err := NewStorage(filepath.Join(t.TempDir(), "db"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			e := handoffTestEntry()
			if err = s.AddQueue(e); err != nil {
				t.Fatal(err)
			}
			if err = s.AuthorizeUncachedHandoff(e); err != nil {
				t.Fatal(err)
			}
			intent, err := s.PrepareQBitQueuedDeletion(e.InfoHash, true)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.StartQueuedDeletionCleanup(intent.InfoHash, intent.QueueIncarnation); err != nil {
				t.Fatal(err)
			}
			if clean {
				if err = s.MarkQueuedDeletionPlacementsClean(intent.InfoHash, intent.QueueIncarnation); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = s.PrepareQBitQueuedDeletion(e.InfoHash, false); err == nil {
				t.Fatal("preserving retry accepted unfinished destructive intent")
			}
		})
	}
}

func TestTransferCleanupMigrationMetadata(t *testing.T) {
	s, err := NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := handoffTestEntry()
	if err = s.AddQueue(e); err != nil {
		t.Fatal(err)
	}
	if err = s.AuthorizeUncachedHandoff(e); err != nil {
		t.Fatal(err)
	}
	if err = s.SaveMigrationStatus(&SystemMigrationStatus{}); err != nil {
		t.Fatal(err)
	}
	if err = s.ValidateTransferCleanup(e); err != nil {
		t.Fatalf("legitimate migration metadata blocks cleanup: %v", err)
	}
	intent, err := s.PrepareQBitQueuedDeletion(e.InfoHash, true)
	if err != nil || !intent.PlacementCleanupPending {
		t.Fatalf("migration metadata blocked authorized cleanup: %+v %v", intent, err)
	}
	if err = s.entries.Put("broken-real-entry", []byte{255}, nil); err != nil {
		t.Fatal(err)
	}
	if err = s.ValidateTransferCleanup(e); err == nil {
		t.Fatal("corrupt real entry accepted")
	}
}

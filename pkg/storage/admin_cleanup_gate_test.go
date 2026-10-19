package storage

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestAdminCompletionSyncFailureGatesLocalCleanup(t *testing.T) {
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
	if err = s.RetireQueuedDeletionRow(intent.InfoHash, intent.QueueIncarnation); err != nil {
		t.Fatal(err)
	}
	offline := errors.New("offline")
	if err = s.DeleteWithCleanup(e.InfoHash, func(main *Entry) error {
		if err := s.PrepareMainDeletionPlacements(main); err != nil {
			return err
		}
		return offline
	}); !errors.Is(err, offline) {
		t.Fatal(err)
	}
	called := false
	if err = s.CompleteQueuedDeletion(intent.InfoHash, intent.QueueIncarnation, func() error { called = true; return nil }); err == nil || called {
		t.Fatal("pending admin did not gate local files")
	}
	failedSync := errors.New("admin completion sync failed")
	s.mainCleanupSyncForTest = func() error { return failedSync }
	if err = s.CompleteMainDeletionPlacements(e.InfoHash); !errors.Is(err, failedSync) {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err = s.CompleteQueuedDeletion(intent.InfoHash, intent.QueueIncarnation, func() error { called = true; return nil }); !errors.Is(err, failedSync) || called {
			t.Fatalf("retry %d bypassed admin sync: %v", i, err)
		}
	}
	s.mainCleanupSyncForTest = nil
	ref, err := s.acquireMainEntryState(e.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	defer ref.release()
	if err = s.CompleteQueuedDeletion(intent.InfoHash, intent.QueueIncarnation, func() error {
		// Admin installation uses this same lock: there is no check/use window.
		if ref.state.mu.TryLock() {
			ref.state.mu.Unlock()
			t.Error("admin install can race local cleanup")
		}
		called = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("durable completion did not unblock files")
	}
}

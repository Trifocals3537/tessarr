package storage

import (
	"errors"
	"path/filepath"
	"testing"
)

func prepareSyncRetryDeletion(t *testing.T, s *Storage) *QueueDeletionIntent {
	t.Helper()
	e := &Entry{InfoHash: "sync-retry", Name: "release"}
	if err := s.AddQueue(e); err != nil {
		t.Fatal(err)
	}
	intent, err := s.PrepareQBitQueuedDeletion(e.InfoHash, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StartQueuedDeletionCleanup(intent.InfoHash, intent.QueueIncarnation); err != nil {
		t.Fatal(err)
	}
	return intent
}

func TestRetireQueuedDeletionRetriesAbsentRowSync(t *testing.T) {
	s, err := NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	intent := prepareSyncRetryDeletion(t, s)
	failure := errors.New("queue retirement sync failed")
	syncs := 0
	s.queueRowSyncForTest = func() error { syncs++; return failure }
	for i := 0; i < 3; i++ {
		if err := s.RetireQueuedDeletionRow(intent.InfoHash, intent.QueueIncarnation); !errors.Is(err, failure) {
			t.Errorf("retirement retry %d bypassed failed row sync: %v", i, err)
		}
		if _, err := s.getQueuedRaw(intent.InfoHash); !IsQueuedEntryNotFound(err) {
			t.Fatalf("row still in memory: %v", err)
		}
	}
	if syncs != 3 {
		t.Errorf("row syncs = %d, want 3", syncs)
	}
	if err := s.CompleteQueuedDeletion(intent.InfoHash, intent.QueueIncarnation); err == nil {
		t.Error("completion accepted a retirement without a successful row sync")
	}
	s.queueRowSyncForTest = nil
	if t.Failed() {
		return
	}
	if err := s.RetireQueuedDeletionRow(intent.InfoHash, intent.QueueIncarnation); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteQueuedDeletion(intent.InfoHash, intent.QueueIncarnation); err != nil {
		t.Fatal(err)
	}
}

func TestQueuedDeletionRetriesAbsentIntentSync(t *testing.T) {
	for _, retry := range []string{"complete", "client"} {
		t.Run(retry, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "db")
			s, err := NewStorage(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if s != nil {
					if err := s.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			intent := prepareSyncRetryDeletion(t, s)
			if err := s.RetireQueuedDeletionRow(intent.InfoHash, intent.QueueIncarnation); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("queue intent completion sync failed")
			s.queueIntentSyncForTest = func() error { return failure }
			if err := s.CompleteQueuedDeletion(intent.InfoHash, intent.QueueIncarnation); !errors.Is(err, failure) {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				if retry == "complete" {
					err = s.CompleteQueuedDeletion(intent.InfoHash, intent.QueueIncarnation)
				} else {
					_, err = s.PrepareQBitQueuedDeletion(intent.InfoHash, false)
				}
				if !errors.Is(err, failure) {
					t.Errorf("%s retry %d bypassed failed completion sync: %v", retry, i, err)
				}
				if err := s.AddQueue(&Entry{InfoHash: intent.InfoHash, Name: "too early"}); !errors.Is(err, ErrQueuedEntryDeleting) {
					t.Errorf("replacement admitted before durable completion: %v", err)
				}
			}
			s.queueIntentSyncForTest = nil
			if t.Failed() {
				return
			}
			if retry == "complete" {
				err = s.CompleteQueuedDeletion(intent.InfoHash, intent.QueueIncarnation)
			} else {
				_, err = s.PrepareQBitQueuedDeletion(intent.InfoHash, false)
				if IsQueuedEntryNotFound(err) {
					err = nil
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			replacement := &Entry{InfoHash: intent.InfoHash, Name: "replacement"}
			if err := s.AddQueue(replacement); err != nil {
				t.Fatalf("durable completion did not reopen the queue key: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = NewStorage(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.GetQueued(intent.InfoHash)
			if err != nil || got.QueueIncarnation != replacement.QueueIncarnation {
				t.Fatalf("replacement did not survive reopen: %+v %v", got, err)
			}
			pending, err := s.QueuedDeletionIntents()
			if err != nil || len(pending) != 0 {
				t.Fatalf("completed intent returned after reopen: %+v %v", pending, err)
			}
		})
	}
}

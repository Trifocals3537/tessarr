package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
)

func TestRepairRecoveryIntentIsImmediatelyDurable(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	dir := t.TempDir()
	s, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	job := &RepairRecovery{ID: "recovery", Original: BrokenFile{EntryName: "entry", ArrName: "radarr", ArrFileID: 10}, State: RecoveryAttention, SearchIntentAt: time.Now()}
	if err := s.SaveRepairRecovery(job); err != nil {
		t.Fatal(err)
	}
	// Open the append log before Close or a periodic sync could mask a missing
	// durability boundary. This mirrors the hybrid store's crash-read tests.
	reader, err := hybrid.New(hybrid.Config{DataPath: filepath.Join(dir, "repair_recoveries.db"), SyncInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.Get(job.ID); err != nil {
		t.Fatalf("intent not recoverable immediately: %v", err)
	}
	if err := s.PruneRepairRecoveries(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	ids, err := s.PendingRepairRecoveryIDs([]string{"entry"})
	if err != nil || len(ids) != 1 {
		t.Fatalf("unfinished intent pruned: %v %v", ids, err)
	}
	ids, err = s.PendingRepairRecoveryIDs([]string{"other"})
	if err != nil || len(ids) != 0 {
		t.Fatalf("entry filter ignored: %v %v", ids, err)
	}
}

func TestRepairRecoveryReopensAndRejectsClosedStore(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	dir := t.TempDir()
	s, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	job := &RepairRecovery{ID: "recovery", Original: BrokenFile{ArrName: "radarr", ArrFileID: 10}, State: RecoveryWaiting, Deleted: true, SearchReceiptID: 17}
	if err := s.SaveRepairRecovery(job); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveRepairRecovery(job); err == nil {
		t.Fatal("closed store accepted intent")
	}
	s, err = NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.GetRepairRecovery(job.ID)
	if err != nil || got == nil || got.SearchReceiptID != 17 || !got.Deleted {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	got.State = RecoveryComplete
	if err := s.SaveRepairRecovery(got); err != nil {
		t.Fatal(err)
	}
	if err := s.PruneRepairRecoveries(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetRepairRecovery(job.ID); err != nil || got != nil {
		t.Fatalf("completed receipt not pruned: %+v %v", got, err)
	}
}

package storage

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func testMigrationCleanupIntent() *MigrationCleanupIntent {
	return &MigrationCleanupIntent{
		JobID:           "job-1",
		InfoHash:        "0123456789ABCDEF0123456789ABCDEF01234567",
		SourceProvider:  "torbox-primary",
		SourceTorrentID: "17",
		TargetProvider:  "torbox-secondary",
		TargetTorrentID: "99",
	}
}

func TestMigrationCleanupPersistsRetryStateAcrossRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db")
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	prepared, err := store.PrepareMigrationCleanup(testMigrationCleanupIntent())
	if err != nil {
		t.Fatal(err)
	}
	if prepared.ID == "" {
		t.Fatal("prepared intent has no stable ID")
	}
	if prepared.InfoHash != "0123456789abcdef0123456789abcdef01234567" {
		t.Fatalf("normalized info hash = %q", prepared.InfoHash)
	}
	attemptedAt := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	nextAttemptAt := attemptedAt.Add(4 * time.Minute)
	if err := store.MarkMigrationCleanupFailed(
		prepared.ID,
		errors.New("provider temporarily unavailable"),
		attemptedAt,
		nextAttemptAt,
	); err != nil {
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

	recovered, err := store.GetMigrationCleanup(prepared.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Attempts != 1 ||
		!recovered.LastAttemptAt.Equal(attemptedAt) ||
		!recovered.NextAttemptAt.Equal(nextAttemptAt) ||
		recovered.LastError != "provider temporarily unavailable" {
		t.Fatalf("recovered retry state = %#v", recovered)
	}
	if got := store.MigrationCleanupCount(); got != 1 {
		t.Fatalf("migration cleanup count = %d, want 1", got)
	}
	if err := store.CompleteMigrationCleanup(prepared.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetMigrationCleanup(prepared.ID); !IsMigrationCleanupNotFound(err) {
		t.Fatalf("completed cleanup lookup error = %v", err)
	}
}

func TestPrepareMigrationCleanupIsIdempotentWithoutResettingBackoff(t *testing.T) {
	store, err := NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	}()

	prepared, err := store.PrepareMigrationCleanup(testMigrationCleanupIntent())
	if err != nil {
		t.Fatal(err)
	}
	attemptedAt := time.Date(2026, time.August, 24, 13, 0, 0, 0, time.UTC)
	nextAttemptAt := attemptedAt.Add(8 * time.Minute)
	if err := store.MarkMigrationCleanupFailed(
		prepared.ID,
		errors.New("retry later"),
		attemptedAt,
		nextAttemptAt,
	); err != nil {
		t.Fatal(err)
	}

	reprepare := testMigrationCleanupIntent()
	reprepare.JobID = "job-2"
	recovered, err := store.PrepareMigrationCleanup(reprepare)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.ID != prepared.ID || recovered.JobID != "job-2" {
		t.Fatalf("reprepared identity = %#v", recovered)
	}
	if recovered.Attempts != 1 || !recovered.NextAttemptAt.Equal(nextAttemptAt) {
		t.Fatalf("reprepare reset durable backoff: %#v", recovered)
	}
}

func TestMigrationCleanupRejectsAmbiguousIdentity(t *testing.T) {
	store, err := NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	}()

	tests := []struct {
		name   string
		mutate func(*MigrationCleanupIntent)
	}{
		{
			name: "same configured account",
			mutate: func(intent *MigrationCleanupIntent) {
				intent.TargetProvider = intent.SourceProvider
			},
		},
		{
			name: "missing source id",
			mutate: func(intent *MigrationCleanupIntent) {
				intent.SourceTorrentID = ""
			},
		},
		{
			name: "forged stable id",
			mutate: func(intent *MigrationCleanupIntent) {
				intent.ID = "not-the-derived-id"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			intent := testMigrationCleanupIntent()
			test.mutate(intent)
			if _, err := store.PrepareMigrationCleanup(intent); err == nil {
				t.Fatal("PrepareMigrationCleanup() error = nil")
			}
		})
	}
	if got := store.MigrationCleanupCount(); got != 0 {
		t.Fatalf("rejected intents persisted = %d", got)
	}
}

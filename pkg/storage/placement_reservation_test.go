package storage

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestPlacementFenceSurvivesRestartAndFailedDeletion(t *testing.T) {
	db := filepath.Join(t.TempDir(), "db")
	s, err := NewStorage(db)
	if err != nil {
		t.Fatal(err)
	}
	e := &Entry{InfoHash: "transfer", ActiveProvider: "fake", Providers: map[string]*ProviderEntry{"fake": {Provider: "fake", ID: "active"}}}
	// A reservation itself is the persisted pre-network boundary. Simulate a
	// timeout/crash after it: no completion notification can release stale IDs.
	if err := s.ReserveTransferCleanup(e); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStorage(db)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	main := *e
	main.InfoHash = "library"
	if err := s.AddOrUpdate(&main); !errors.Is(err, ErrPlacementCleanupReserved) {
		t.Fatalf("reopened publication=%v", err)
	}
	replacement := *e
	replacement.InfoHash = "replacement"
	replacement.Providers = map[string]*ProviderEntry{"fake": {Provider: "fake", ID: "new-id"}}
	if err := s.AddOrUpdate(&replacement); err != nil {
		t.Fatalf("unrelated placement blocked: %v", err)
	}
	if err := s.ReserveTransferCleanup(e); err != nil {
		t.Fatalf("retry fence: %v", err)
	}
}

func TestMainPublicationBeforeReservationWins(t *testing.T) {
	s, err := NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	main := &Entry{InfoHash: "library", ActiveProvider: "fake", Providers: map[string]*ProviderEntry{"fake": {Provider: "fake", ID: "shared"}}}
	if err := s.AddOrUpdate(main); err != nil {
		t.Fatal(err)
	}
	transfer := *main
	transfer.InfoHash = "transfer"
	if err := s.ReserveTransferCleanup(&transfer); err == nil {
		t.Fatal("existing main owner not protected")
	}
	// A rejected reservation must not poison the protected library's next write.
	if err := s.AddOrUpdate(main); err != nil {
		t.Fatal(err)
	}
}

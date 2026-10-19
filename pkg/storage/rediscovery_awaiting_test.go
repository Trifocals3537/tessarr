package storage

import (
	"errors"
	"testing"
)

// TestRediscoveryAwaitingAbsenceMirrorsGuardAuthorization walks the same
// lifecycle as TestMainEntryProviderRediscoveryRequiresLaterPresence and
// asserts the cheap read-only probe agrees with the guard at every step, so
// provider sync can rely on it to skip expensive work without weakening the
// rediscovery rule.
func TestRediscoveryAwaitingAbsenceMirrorsGuardAuthorization(t *testing.T) {
	store := newMainLifecycleTestStorage(t)
	entry := mainLifecycleTestEntry("awaiting-probe", "provider-a")
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatalf("seed entry: %v", err)
	}

	liveSnapshot := store.BeginProviderSnapshot()
	awaiting, err := store.RediscoveryAwaitingAbsence(entry.InfoHash, "provider-a", liveSnapshot)
	if err != nil {
		t.Fatalf("live entry probe: %v", err)
	}
	if awaiting {
		t.Fatal("live entry must not report awaiting absence")
	}

	preDeleteSnapshot := store.BeginProviderSnapshot()
	if err := store.Delete(entry.InfoHash); err != nil {
		t.Fatalf("delete entry: %v", err)
	}

	if awaiting, err := store.RediscoveryAwaitingAbsence(entry.InfoHash, "provider-a", preDeleteSnapshot); err != nil || !awaiting {
		t.Fatalf("post-delete presence must report awaiting, got awaiting=%v err=%v", awaiting, err)
	}
	candidate := mainLifecycleTestEntry(entry.InfoHash, "provider-a")
	if err := store.PrepareProviderEntry(candidate, "provider-a", preDeleteSnapshot); !errors.Is(err, ErrEntryRediscoveryPending) {
		t.Fatalf("guard mismatch on post-delete presence: %v", err)
	}

	// A snapshot that still contains the key is a presence, not an absence.
	presenceSnapshot := store.BeginProviderSnapshot()
	if err := store.ObserveProviderSnapshot("provider-a", presenceSnapshot, map[string]struct{}{entry.InfoHash: {}}); err != nil {
		t.Fatalf("observe stale presence: %v", err)
	}
	if awaiting, err := store.RediscoveryAwaitingAbsence(entry.InfoHash, "provider-a", presenceSnapshot); err != nil || !awaiting {
		t.Fatalf("presence without absence must report awaiting, got awaiting=%v err=%v", awaiting, err)
	}

	// An authoritative absence authorizes only a strictly later reappearance.
	absenceSnapshot := store.BeginProviderSnapshot()
	if err := store.ObserveProviderSnapshot("provider-a", absenceSnapshot, nil); err != nil {
		t.Fatalf("observe provider absence: %v", err)
	}
	if awaiting, err := store.RediscoveryAwaitingAbsence(entry.InfoHash, "provider-a", absenceSnapshot); err != nil || !awaiting {
		t.Fatalf("same-snapshot reappearance must still report awaiting, got awaiting=%v err=%v", awaiting, err)
	}
	if err := store.PrepareProviderEntry(candidate, "provider-a", absenceSnapshot); !errors.Is(err, ErrEntryRediscoveryPending) {
		t.Fatalf("guard mismatch on same-snapshot reappearance: %v", err)
	}

	reappearanceSnapshot := store.BeginProviderSnapshot()
	if awaiting, err := store.RediscoveryAwaitingAbsence(entry.InfoHash, "provider-a", reappearanceSnapshot); err != nil || awaiting {
		t.Fatalf("strictly later reappearance must be authorized, got awaiting=%v err=%v", awaiting, err)
	}
	if err := store.PrepareProviderEntry(candidate, "provider-a", reappearanceSnapshot); err != nil {
		t.Fatalf("guard must authorize strictly later reappearance: %v", err)
	}
}

// TestRediscoveryAwaitingAbsenceSurvivesRestart proves a restarted process
// still reports awaiting before the absence handshake, and stops reporting it
// once the durable absence from the tombstone authorizes any later snapshot.
func TestRediscoveryAwaitingAbsenceSurvivesRestart(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	entry := mainLifecycleTestEntry("awaiting-restart", "provider-a")
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
	if awaiting, err := store.RediscoveryAwaitingAbsence(entry.InfoHash, "provider-a", presence); err != nil || !awaiting {
		t.Fatalf("restart must still report awaiting, got awaiting=%v err=%v", awaiting, err)
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
	later := store.BeginProviderSnapshot()
	if awaiting, err := store.RediscoveryAwaitingAbsence(entry.InfoHash, "provider-a", later); err != nil || awaiting {
		t.Fatalf("durable absence must authorize later presence, got awaiting=%v err=%v", awaiting, err)
	}
}

// TestRediscoveryAwaitingAbsenceNilStorageFailsClosed proves the probe never
// requests a skip when the lifecycle store is unavailable.
func TestRediscoveryAwaitingAbsenceNilStorageFailsClosed(t *testing.T) {
	var store *Storage
	awaiting, err := store.RediscoveryAwaitingAbsence("some-key", "torbox", 7)
	if err != nil {
		t.Fatalf("nil storage probe error = %v, want nil", err)
	}
	if awaiting {
		t.Fatal("nil storage must never report awaiting absence")
	}
}

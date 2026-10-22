package hybrid

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/stateowner"
)

func TestWriterExclusionPrecedesRecoveryAndSurvivesCompaction(t *testing.T) {
	cfg := Config{DataPath: filepath.Join(t.TempDir(), "exclusive.db"), SyncInterval: 0}
	first, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	alias := cfg
	alias.DataPath = filepath.Join(filepath.Dir(cfg.DataPath), strings.ToUpper(filepath.Base(cfg.DataPath)))
	if second, err := New(alias); !errors.Is(err, stateowner.ErrInUse) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("case variant bypassed ownership: %v", err)
	}
	if err := first.Put("first", []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	staged := cfg.DataPath + compactionSuffix
	stagedBytes := []byte("uncommitted artifact")
	if err := os.WriteFile(staged, stagedBytes, 0600); err != nil {
		t.Fatal(err)
	}
	if second, err := New(cfg); !errors.Is(err, stateowner.ErrInUse) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("second writer was not rejected: %v", err)
	}
	got, err := os.ReadFile(staged)
	if err != nil || !bytes.Equal(got, stagedBytes) {
		t.Fatal("rejected writer modified compaction artifacts", err)
	}
	if err := os.Remove(staged); err != nil {
		t.Fatal(err)
	}
	if err := first.Compact(); err != nil {
		t.Fatal(err)
	}
	if second, err := New(cfg); !errors.Is(err, stateowner.ErrInUse) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("compaction released ownership: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err = reopened.Get("first")
	if err != nil || string(got) != "value" {
		t.Fatalf("synced write lost: %q %v", got, err)
	}
}

func TestSyncFailureSuspendsMutationsUntilSuccessfulBarrier(t *testing.T) {
	store, err := New(Config{DataPath: filepath.Join(t.TempDir(), "fault.db"), SyncInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put("existing", []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected sync failure")
	store.syncForTest = func() error { return failure }
	if err := store.Sync(); !errors.Is(err, failure) || !errors.Is(err, ErrDurability) {
		t.Fatal(err)
	}
	for _, mutate := range []func() error{
		func() error { return store.Put("new", []byte("value"), nil) },
		func() error { return store.Delete("existing") }, store.Compact,
	} {
		if err := mutate(); !errors.Is(err, failure) {
			t.Fatalf("mutation bypassed durability fault: %v", err)
		}
	}
	if _, err := store.Get("existing"); err != nil {
		t.Fatal("read access was lost", err)
	}
	store.syncForTest = nil
	if err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("new", []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
}

func TestPeriodicSyncFailureIsVisibleAndRecovers(t *testing.T) {
	store, err := New(Config{DataPath: filepath.Join(t.TempDir(), "periodic.db"), SyncInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	failure := errors.New("periodic sync failed")
	store.mu.Lock()
	store.syncForTest = func() error { return failure }
	store.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for !errors.Is(store.DurabilityError(), failure) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := store.Put("new", []byte("value"), nil); !errors.Is(err, failure) {
		t.Fatalf("background failure not enforced: %v", err)
	}
	store.mu.Lock()
	store.syncForTest = nil
	store.mu.Unlock()
	for store.DurabilityError() != nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := store.Put("new", []byte("value"), nil); err != nil {
		t.Fatalf("periodic retry did not restore durability: %v", err)
	}
}

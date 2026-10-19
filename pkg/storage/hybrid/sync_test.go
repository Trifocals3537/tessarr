package hybrid

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestSyncIntervalZeroWritesAndDeletesAreImmediatelyRecoverable(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	path := filepath.Join(t.TempDir(), "store.db")
	store, err := New(Config{
		DataPath:     path,
		SyncInterval: 0,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.Put("key", []byte("value"), nil); err != nil {
		t.Fatalf("Put: %v", err)
	}
	recovered, err := New(Config{
		DataPath:     path,
		SyncInterval: -1,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatalf("recover immediately after Put: %v", err)
	}
	value, err := recovered.Get("key")
	if err != nil {
		t.Fatalf("Get recovered value: %v", err)
	}
	if string(value) != "value" {
		t.Fatalf("recovered value = %q, want value", value)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}

	if err := store.Delete("key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	recovered, err = New(Config{
		DataPath:     path,
		SyncInterval: -1,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatalf("recover immediately after Delete: %v", err)
	}
	defer recovered.Close()
	if _, err := recovered.Get("key"); !IsNotFound(err) {
		t.Fatalf("Get after recovered delete error = %v, want not found", err)
	}
}

func TestCloseReturnsFinalSyncErrorAndStillClosesLog(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	path := filepath.Join(t.TempDir(), "close-sync.db")
	store, err := New(Config{
		DataPath:     path,
		SyncInterval: -1,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatal(err)
	}
	syncErr := errors.New("injected final sync failure")
	store.syncForTest = func() error { return syncErr }

	if err := store.Close(); !errors.Is(err, syncErr) {
		t.Fatalf("Close() error = %v, want injected sync error", err)
	}

	// A reported sync error must not skip descriptor cleanup.
	reopened, err := New(Config{
		DataPath:     path,
		SyncInterval: -1,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatalf("reopen after Close() sync error: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

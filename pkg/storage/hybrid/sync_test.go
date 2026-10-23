package hybrid

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestSyncIntervalZeroWritesAndDeletesAreImmediatelyRecoverable(t *testing.T) {
	for _, phase := range []string{"put", "delete"} {
		t.Run(phase, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.db")
			cmd := exec.Command(os.Args[0], "-test.run=^TestSyncDurabilityCrashHelper$")
			cmd.Env = append(os.Environ(), "TESSARR_SYNC_CRASH_PATH="+path, "TESSARR_SYNC_CRASH_PHASE="+phase)
			output, err := cmd.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 84 {
				t.Fatalf("crash helper: %v %s", err, output)
			}
			recovered, err := New(Config{DataPath: path, SyncInterval: -1})
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			value, err := recovered.Get("key")
			if phase == "delete" {
				if !IsNotFound(err) {
					t.Fatalf("deleted record recovered: %q %v", value, err)
				}
			} else if err != nil || string(value) != "value" {
				t.Fatalf("write lost: %q %v", value, err)
			}
		})
	}
}

func TestSyncDurabilityCrashHelper(t *testing.T) {
	path := os.Getenv("TESSARR_SYNC_CRASH_PATH")
	if path == "" {
		return
	}
	config.SetConfigPath(filepath.Dir(path))
	store, err := New(Config{DataPath: path, SyncInterval: 0})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put("key", []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("TESSARR_SYNC_CRASH_PHASE") == "delete" {
		if err := store.Delete("key"); err != nil {
			t.Fatal(err)
		}
	}
	os.Exit(84) // No Close/final sync may mask a missing write barrier.
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

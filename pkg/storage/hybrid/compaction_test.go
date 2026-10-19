package hybrid

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
)

const (
	compactionCrashPathEnv  = "TESSARR_COMPACTION_CRASH_PATH"
	compactionCrashPhaseEnv = "TESSARR_COMPACTION_CRASH_PHASE"
	compactionCrashExitCode = 91
)

func TestCompactionCrashHelper(t *testing.T) {
	path := os.Getenv(compactionCrashPathEnv)
	phase := compactionPhase(os.Getenv(compactionCrashPhaseEnv))
	if path == "" || phase == "" {
		return
	}
	config.SetConfigPath(filepath.Dir(path))

	store, err := New(Config{
		DataPath:     path,
		SyncInterval: 0,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatalf("open crash-helper store: %v", err)
	}
	store.compactionPhaseForTest = func(reached compactionPhase) {
		if reached == phase {
			os.Exit(compactionCrashExitCode)
		}
	}
	if err := store.Compact(); err != nil {
		t.Fatalf("compact before injected crash at %s: %v", phase, err)
	}
	t.Fatalf("compaction never reached injected crash phase %s", phase)
}

func TestCompactionRecoversEveryDurableCrashPhase(t *testing.T) {
	phases := []compactionPhase{
		compactionPhaseStaged,
		compactionPhaseCanonicalReplaced,
		compactionPhaseReplacementSynced,
	}
	if runtime.GOOS == "windows" {
		phases = []compactionPhase{
			compactionPhaseStaged,
			compactionPhaseCanonicalBackedUp,
			compactionPhaseCanonicalReplaced,
			compactionPhaseReplacementSynced,
			compactionPhaseBackupRemoved,
		}
	}

	for _, phase := range phases {
		t.Run(string(phase), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.db")
			seedCompactionStore(t, path)

			cmd := exec.Command(os.Args[0], "-test.run=^TestCompactionCrashHelper$")
			cmd.Env = append(
				os.Environ(),
				compactionCrashPathEnv+"="+path,
				compactionCrashPhaseEnv+"="+string(phase),
			)
			output, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != compactionCrashExitCode {
				t.Fatalf(
					"crash helper at %s returned %v, output:\n%s",
					phase,
					err,
					output,
				)
			}
			assertCrashArtifactState(t, path, phase)

			recovered, err := New(Config{
				DataPath:     path,
				SyncInterval: -1,
				AutoCompact:  false,
			})
			if err != nil {
				t.Fatalf("recover after crash at %s: %v", phase, err)
			}
			assertCompactionContents(t, recovered)
			if err := recovered.Close(); err != nil {
				t.Fatalf("close recovered store: %v", err)
			}
			assertNoCompactionArtifacts(t, path)
		})
	}
}

func TestCompactPublishesOnlyInstalledLogAndRemainsWritable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	seedCompactionStore(t, path)
	store, err := New(Config{
		DataPath:     path,
		SyncInterval: 0,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	before := store.DiskSize()
	if err := store.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if after := store.DiskSize(); after >= before {
		t.Fatalf("compacted size = %d, want less than original %d", after, before)
	}
	assertCompactionContents(t, store)
	if err := store.Put("after", []byte("replacement is writable"), nil); err != nil {
		t.Fatalf("write after replacement: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close compacted store: %v", err)
	}
	assertNoCompactionArtifacts(t, path)

	reopened, err := New(Config{
		DataPath:     path,
		SyncInterval: -1,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatalf("reopen compacted store: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened compacted store: %v", err)
		}
	}()
	value, err := reopened.Get("after")
	if err != nil {
		t.Fatalf("read post-replacement write: %v", err)
	}
	if string(value) != "replacement is writable" {
		t.Fatalf("post-replacement value = %q", value)
	}
}

func assertCrashArtifactState(t *testing.T, path string, phase compactionPhase) {
	t.Helper()
	wantCanonical, wantCompact, wantBackup := true, false, false
	switch phase {
	case compactionPhaseStaged:
		wantCompact = true
	case compactionPhaseCanonicalBackedUp:
		wantCanonical = false
		wantCompact = true
		wantBackup = true
	case compactionPhaseCanonicalReplaced, compactionPhaseReplacementSynced:
		wantBackup = runtime.GOOS == "windows"
	case compactionPhaseBackupRemoved:
	default:
		t.Fatalf("unknown crash phase %q", phase)
	}
	for artifact, want := range map[string]bool{
		path:                    wantCanonical,
		path + compactionSuffix: wantCompact,
		path + backupSuffix:     wantBackup,
	} {
		_, err := os.Lstat(artifact)
		got := err == nil
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("inspect crash artifact %s: %v", artifact, err)
		}
		if got != want {
			t.Fatalf("crash artifact %s exists=%t, want %t at phase %s", artifact, got, want, phase)
		}
	}
}

func TestStartupRecoveryPrefersSyncedCompactWhenCanonicalIsBackedUp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	writeSingleValueStore(t, path, "old")
	writeSingleValueStore(t, path+compactionSuffix, "new")
	if err := os.Rename(path, path+backupSuffix); err != nil {
		t.Fatalf("stage canonical backup: %v", err)
	}

	store, err := New(Config{
		DataPath:     path,
		SyncInterval: -1,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatalf("recover compact plus backup: %v", err)
	}
	value, err := store.Get("key")
	if err != nil {
		t.Fatalf("get recovered compact value: %v", err)
	}
	if string(value) != "new" {
		t.Fatalf("recovered value = %q, want compact generation %q", value, "new")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close recovered store: %v", err)
	}
	assertNoCompactionArtifacts(t, path)
}

func TestStartupRecoveryRestoresBackupWhenCompactIsTorn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	writeSingleValueStore(t, path, "old")
	if err := os.Rename(path, path+backupSuffix); err != nil {
		t.Fatalf("stage canonical backup: %v", err)
	}
	if err := os.WriteFile(path+compactionSuffix, []byte(logMagic), 0644); err != nil {
		t.Fatalf("write torn compact log: %v", err)
	}

	store, err := New(Config{
		DataPath:     path,
		SyncInterval: -1,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatalf("restore backup around torn compact: %v", err)
	}
	value, err := store.Get("key")
	if err != nil {
		t.Fatalf("get restored backup value: %v", err)
	}
	if string(value) != "old" {
		t.Fatalf("restored value = %q, want backup generation %q", value, "old")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close restored store: %v", err)
	}
	assertNoCompactionArtifacts(t, path)
}

func TestStartupRecoveryRestoresBackupWhenPromotedCanonicalIsTorn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	writeSingleValueStore(t, path+backupSuffix, "old")
	if err := os.WriteFile(path, []byte(logMagic), 0644); err != nil {
		t.Fatalf("write torn promoted canonical: %v", err)
	}

	store, err := New(Config{
		DataPath:     path,
		SyncInterval: -1,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatalf("restore backup around torn promoted canonical: %v", err)
	}
	value, err := store.Get("key")
	if err != nil {
		t.Fatalf("get restored promoted-backup value: %v", err)
	}
	if string(value) != "old" {
		t.Fatalf("restored value = %q, want backup generation %q", value, "old")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close restored store: %v", err)
	}
	assertNoCompactionArtifacts(t, path)
}

func TestStartupRecoveryKeepsValidCanonicalOverStaleArtifacts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	writeSingleValueStore(t, path, "canonical")
	writeSingleValueStore(t, path+compactionSuffix, "compact")
	writeSingleValueStore(t, path+backupSuffix, "backup")

	store, err := New(Config{
		DataPath:     path,
		SyncInterval: -1,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatalf("recover authoritative canonical: %v", err)
	}
	value, err := store.Get("key")
	if err != nil {
		t.Fatalf("get canonical value: %v", err)
	}
	if string(value) != "canonical" {
		t.Fatalf("recovered value = %q, want canonical generation", value)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close canonical store: %v", err)
	}
	assertNoCompactionArtifacts(t, path)
}

func TestCompactionRejectsSymlinkArtifactWithoutTouchingTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	writeSingleValueStore(t, path, "canonical")
	store, err := New(Config{
		DataPath:     path,
		SyncInterval: -1,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatalf("open store before planting symlink: %v", err)
	}

	target := filepath.Join(t.TempDir(), "must-not-change")
	original := []byte("unrelated file")
	if err := os.WriteFile(target, original, 0600); err != nil {
		t.Fatalf("write symlink target: %v", err)
	}
	if err := os.Symlink(target, path+compactionSuffix); err != nil {
		if closeErr := store.Close(); closeErr != nil {
			t.Fatalf("close store after unavailable symlink test: %v", closeErr)
		}
		t.Skipf("symlinks unavailable on this host: %v", err)
	}

	if err := store.Compact(); err == nil {
		t.Fatal("Compact() accepted a symlink compaction artifact")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store after rejected symlink: %v", err)
	}
	after, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read symlink target: %v", err)
	}
	if !bytes.Equal(after, original) {
		t.Fatalf("symlink target changed: got %q, want %q", after, original)
	}
}

func TestStoreRejectsSymlinkCanonicalWithoutTouchingTarget(t *testing.T) {
	target := filepath.Join(t.TempDir(), "real.db")
	writeSingleValueStore(t, target, "target")
	before, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read canonical target: %v", err)
	}

	link := filepath.Join(t.TempDir(), "linked.db")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable on this host: %v", err)
	}
	if _, err := New(Config{DataPath: link, AutoCompact: false}); err == nil {
		t.Fatal("New() accepted a symlink canonical log")
	}
	after, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read canonical target after rejection: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("canonical symlink target changed after rejection")
	}
}

func TestCompactSyncFailureLeavesCanonicalUsable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	seedCompactionStore(t, path)
	store, err := New(Config{
		DataPath:     path,
		SyncInterval: -1,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	injected := errors.New("injected pre-compaction sync failure")
	store.syncForTest = func() error { return injected }
	if err := store.Compact(); !errors.Is(err, injected) {
		t.Fatalf("Compact() error = %v, want injected sync failure", err)
	}
	store.syncForTest = nil
	assertCompactionContents(t, store)
	assertNoCompactionArtifacts(t, path)
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
}

func seedCompactionStore(t *testing.T, path string) {
	t.Helper()
	config.SetConfigPath(filepath.Dir(path))
	store, err := New(Config{
		DataPath:     path,
		SyncInterval: 0,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatalf("create compaction seed store: %v", err)
	}
	meta := &EntryMeta{
		Category:  "movies",
		Provider:  "provider",
		Status:    "downloaded",
		Name:      "Live Entry",
		TotalSize: 42,
		Protocol:  "torrent",
		AddedOn:   time.Unix(1_700_000_000, 0).Unix(),
	}
	if err := store.Put("live", []byte("first"), meta); err != nil {
		t.Fatalf("put first live value: %v", err)
	}
	if err := store.Put("live", []byte("current"), meta); err != nil {
		t.Fatalf("update live value: %v", err)
	}
	if err := store.Put("deleted", []byte("dead"), nil); err != nil {
		t.Fatalf("put deleted value: %v", err)
	}
	if err := store.Delete("deleted"); err != nil {
		t.Fatalf("delete dead value: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close compaction seed store: %v", err)
	}
}

func assertCompactionContents(t *testing.T, store *Store) {
	t.Helper()
	value, err := store.Get("live")
	if err != nil {
		t.Fatalf("get live value: %v", err)
	}
	if string(value) != "current" {
		t.Fatalf("live value = %q, want %q", value, "current")
	}
	if _, err := store.Get("deleted"); !IsNotFound(err) {
		t.Fatalf("deleted key error = %v, want not found", err)
	}
	meta, err := store.GetMeta("live")
	if err != nil {
		t.Fatalf("get live metadata: %v", err)
	}
	if meta.Category != "movies" || meta.Provider != "provider" ||
		meta.Status != "downloaded" || meta.Name != "Live Entry" ||
		meta.TotalSize != 42 || meta.Protocol != "torrent" ||
		meta.AddedOn != 1_700_000_000 {
		t.Fatalf("recovered metadata = %+v", meta)
	}
}

func writeSingleValueStore(t *testing.T, path, value string) {
	t.Helper()
	config.SetConfigPath(filepath.Dir(path))
	store, err := New(Config{
		DataPath:     path,
		SyncInterval: 0,
		AutoCompact:  false,
	})
	if err != nil {
		t.Fatalf("create store %s: %v", path, err)
	}
	if err := store.Put("key", []byte(value), nil); err != nil {
		t.Fatalf("put %q in %s: %v", value, path, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store %s: %v", path, err)
	}
}

func assertNoCompactionArtifacts(t *testing.T, path string) {
	t.Helper()
	for _, artifact := range []string{path + compactionSuffix, path + backupSuffix} {
		if _, err := os.Lstat(artifact); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("artifact %s still exists (stat error %v)", artifact, err)
		}
	}
}

func TestCompactionPhaseNamesRemainStable(t *testing.T) {
	// Subprocess fault injection passes these names through the environment.
	// A duplicate would silently leave one crash boundary untested.
	seen := make(map[compactionPhase]struct{})
	for _, phase := range []compactionPhase{
		compactionPhaseStaged,
		compactionPhaseCanonicalBackedUp,
		compactionPhaseCanonicalReplaced,
		compactionPhaseReplacementSynced,
		compactionPhaseBackupRemoved,
	} {
		if phase == "" {
			t.Fatal("empty compaction phase")
		}
		if _, exists := seen[phase]; exists {
			t.Fatalf("duplicate compaction phase %q", phase)
		}
		seen[phase] = struct{}{}
	}
	if got, want := len(seen), 5; got != want {
		t.Fatalf("phase count = %d, want %d", got, want)
	}
}

package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
)

func TestPersistConfigCreatesBackupBeforeReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "config.json")
	original := []byte(`{"port":"8282","debrids":[{"name":"primary"}]}`)
	updated := []byte(`{"port":"9090","debrids":[{"name":"primary"}]}`)

	if err := persistConfig(path, original); err != nil {
		t.Fatalf("persist original config: %v", err)
	}
	if err := persistConfig(path, updated); err != nil {
		t.Fatalf("persist updated config: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read current config: %v", err)
	}
	if string(got) != string(updated) {
		t.Fatalf("current config = %s, want %s", got, updated)
	}

	backups, err := os.ReadDir(filepath.Join(filepath.Dir(path), "backups"))
	if err != nil {
		t.Fatalf("read backup directory: %v", err)
	}
	if len(backups) != 1 {
		t.Fatalf("backup count = %d, want 1", len(backups))
	}
	backup, err := os.ReadFile(filepath.Join(filepath.Dir(path), "backups", backups[0].Name()))
	if err != nil {
		t.Fatalf("read backup: %v", err)
	}
	if string(backup) != string(original) {
		t.Fatalf("backup = %s, want %s", backup, original)
	}

	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat config: %v", err)
		}
		if gotMode := info.Mode().Perm(); gotMode != privateFileMode {
			t.Fatalf("config mode = %o, want %o", gotMode, privateFileMode)
		}
	}
}

func TestPersistConfigRetainsNewestBackups(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	if err := persistConfig(path, []byte(`{"revision":0}`)); err != nil {
		t.Fatalf("persist initial config: %v", err)
	}
	for revision := 1; revision <= maxConfigBackups+3; revision++ {
		data := []byte(fmt.Sprintf(`{"revision":%d}`, revision))
		if err := persistConfig(path, data); err != nil {
			t.Fatalf("persist revision %d: %v", revision, err)
		}
	}

	backupDir := filepath.Join(dir, "backups")
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		t.Fatalf("read backup directory: %v", err)
	}
	if len(entries) != maxConfigBackups {
		t.Fatalf("backup count = %d, want %d", len(entries), maxConfigBackups)
	}

	revisions := make([]string, 0, len(entries))
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(backupDir, entry.Name()))
		if err != nil {
			t.Fatalf("read backup %s: %v", entry.Name(), err)
		}
		revisions = append(revisions, string(data))
	}
	for _, removed := range []string{`{"revision":0}`, `{"revision":1}`, `{"revision":2}`} {
		if slices.Contains(revisions, removed) {
			t.Fatalf("old backup %s was not pruned", removed)
		}
	}
}

func TestPersistAuthCreatesConfigDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new", "config", "auth.json")
	data := []byte(`{"username":"admin"}`)

	if err := persistAuth(path, data); err != nil {
		t.Fatalf("persist auth: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read auth: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("auth = %s, want %s", got, data)
	}
}

func TestPersistConfigEnforcesReadableSizeLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	prefix := []byte(`{"padding":"`)
	suffix := []byte(`"}`)
	maximum := append(append(prefix, bytes.Repeat([]byte{'x'}, maxConfigurationFileBytes-len(prefix)-len(suffix))...), suffix...)
	if err := persistConfig(path, maximum); err != nil {
		t.Fatalf("persist maximum-sized config: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read maximum-sized config: %v", err)
	}
	if !bytes.Equal(got, maximum) {
		t.Fatal("maximum-sized config did not round trip")
	}

	tooLarge := append(append([]byte(nil), maximum...), 'x')
	if err := persistConfig(path, tooLarge); err == nil {
		t.Fatal("oversized config was persisted")
	}
	got, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config after rejected update: %v", err)
	}
	if !bytes.Equal(got, maximum) {
		t.Fatal("rejected oversized update changed the persisted config")
	}
}

func TestPersistAuthRejectsUnreadableSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.json")
	if err := persistAuth(path, make([]byte, maxConfigurationFileBytes+1)); err == nil {
		t.Fatal("oversized authentication configuration was persisted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("oversized authentication configuration created a file: %v", err)
	}
}

func TestPersistConfigSkipsUnchangedBackup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	data := []byte(`{"port":"8282"}`)

	if err := persistConfig(path, data); err != nil {
		t.Fatalf("persist initial config: %v", err)
	}
	if err := persistConfig(path, data); err != nil {
		t.Fatalf("persist unchanged config: %v", err)
	}

	_, err := os.Stat(filepath.Join(dir, "backups"))
	if !os.IsNotExist(err) {
		t.Fatalf("unchanged write created a backup directory; stat error = %v", err)
	}

	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0644); err != nil {
			t.Fatalf("make config permissions permissive: %v", err)
		}
		if err := persistConfig(path, data); err != nil {
			t.Fatalf("persist unchanged config with permissive mode: %v", err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat unchanged config: %v", err)
		}
		if gotMode := info.Mode().Perm(); gotMode != privateFileMode {
			t.Fatalf("unchanged config mode = %o, want %o", gotMode, privateFileMode)
		}
	}
}

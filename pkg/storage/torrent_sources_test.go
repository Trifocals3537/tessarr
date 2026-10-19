package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/testutil"
	"github.com/Trifocals3537/tessarr/internal/utils"
)

func testTorrentSource(t *testing.T) ([]byte, *utils.Magnet) {
	t.Helper()
	data, err := os.ReadFile(testutil.GetTestTorrentPath())
	if err != nil {
		t.Fatal(err)
	}
	magnet, err := utils.GetMagnetFromBytes(data, false)
	if err != nil {
		t.Fatal(err)
	}
	return data, magnet
}

func TestTorrentSourceRoundTripSurvivesStorageRestart(t *testing.T) {
	data, magnet := testTorrentSource(t)
	dbPath := t.TempDir()
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveTorrentSource(strings.ToUpper(magnet.InfoHash), data); err != nil {
		t.Fatalf("SaveTorrentSource() error = %v", err)
	}
	if err := store.AddQueue(&Entry{InfoHash: magnet.InfoHash, Name: magnet.Name}); err != nil {
		t.Fatalf("AddQueue() error = %v", err)
	}
	path, err := store.torrentSourcePath(magnet.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != torrentSourceMode {
			t.Fatalf("torrent source mode = %o, want %o", got, torrentSourceMode)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	got, err := store.LoadTorrentSource(magnet.InfoHash)
	if err != nil {
		t.Fatalf("LoadTorrentSource() error = %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("reloaded torrent source differs from admitted bytes")
	}
}

func TestTorrentSourceRejectsMismatchesAndCorruption(t *testing.T) {
	data, magnet := testTorrentSource(t)
	store, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.SaveTorrentSource("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", data); err == nil {
		t.Fatal("SaveTorrentSource() accepted mismatched content")
	}
	if err := store.SaveTorrentSource("../"+magnet.InfoHash, data); err == nil {
		t.Fatal("SaveTorrentSource() accepted an unsafe key")
	}
	if err := store.SaveTorrentSource(magnet.InfoHash, data); err != nil {
		t.Fatal(err)
	}
	path, err := store.torrentSourcePath(magnet.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("not a torrent"), torrentSourceMode); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadTorrentSource(magnet.InfoHash); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("LoadTorrentSource() error = %v, want a corruption error", err)
	}
}

func TestTorrentSourceStartupPrunesOrphans(t *testing.T) {
	data, magnet := testTorrentSource(t)
	dbPath := t.TempDir()
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveTorrentSource(magnet.InfoHash, data); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.LoadTorrentSource(magnet.InfoHash); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphaned LoadTorrentSource() error = %v, want not found", err)
	}
}

func TestTorrentSourceStoreEnforcesTotalQuota(t *testing.T) {
	data, magnet := testTorrentSource(t)
	store, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	previousLimit := torrentSourceStoreMaxBytes
	torrentSourceStoreMaxBytes = int64(len(data) - 1)
	defer func() { torrentSourceStoreMaxBytes = previousLimit }()
	if err := store.SaveTorrentSource(magnet.InfoHash, data); err == nil {
		t.Fatal("SaveTorrentSource() accepted data beyond the total quota")
	}
}

func TestTorrentSourceStoreRejectsSymlinkedPrivateDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation is not consistently available on Windows")
	}
	data, magnet := testTorrentSource(t)
	dbPath := t.TempDir()
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(dbPath, torrentSourceDirName)); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveTorrentSource(magnet.InfoHash, data); err == nil {
		t.Fatal("SaveTorrentSource() accepted a symlinked private directory")
	}
	entries, err := os.ReadDir(outside)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("outside directory received files: %v", entries)
	}
}

func TestTorrentSourceTemporaryNameRecognitionIsExact(t *testing.T) {
	hash := "0123456789abcdef0123456789abcdef01234567"
	valid := "." + hash + torrentSourceSuffix + ".tmp-0123456789abcdef0123456789abcdef"
	if !isTorrentSourceTempName(valid) {
		t.Fatalf("isTorrentSourceTempName(%q) = false", valid)
	}
	for _, name := range []string{
		"notes.tmp-do-not-delete",
		"." + hash + torrentSourceSuffix + ".tmp-short",
		"." + strings.Repeat("g", 40) + torrentSourceSuffix + ".tmp-0123456789abcdef0123456789abcdef",
		valid + ".extra",
	} {
		if isTorrentSourceTempName(name) {
			t.Fatalf("isTorrentSourceTempName(%q) = true", name)
		}
	}
}

func TestReplaceTorrentSourceUsesPinnedRoot(t *testing.T) {
	container := t.TempDir()
	parentPath := filepath.Join(container, "original")
	rootPath := filepath.Join(parentPath, "root")
	if err := os.MkdirAll(rootPath, 0o700); err != nil {
		t.Fatal(err)
	}
	rooted, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rooted.Close()
	if err := os.WriteFile(filepath.Join(rootPath, "source.tmp"), []byte("pinned source"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "destination.torrent"), []byte("pinned destination"), 0o600); err != nil {
		t.Fatal(err)
	}

	movedParent := filepath.Join(container, "moved")
	movedRoot := filepath.Join(movedParent, "root")
	if err := os.Rename(parentPath, movedParent); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("Windows keeps the opened root ancestry rename-protected: %v", err)
		}
		t.Fatalf("rename parent of opened torrent-source root: %v", err)
	}
	if err := os.MkdirAll(rootPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "source.tmp"), []byte("replacement source"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "destination.torrent"), []byte("replacement destination"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := replaceTorrentSource(rooted, "source.tmp", "destination.torrent"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(movedRoot, "destination.torrent"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "pinned source" {
		t.Fatalf("pinned destination = %q, want pinned source", got)
	}
	if _, err := os.Stat(filepath.Join(movedRoot, "source.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pinned source still exists: %v", err)
	}
	got, err = os.ReadFile(filepath.Join(rootPath, "destination.torrent"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "replacement destination" {
		t.Fatalf("replacement destination changed to %q", got)
	}
	got, err = os.ReadFile(filepath.Join(rootPath, "source.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "replacement source" {
		t.Fatalf("replacement source changed to %q", got)
	}
}

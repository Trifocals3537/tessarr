package manager

import (
	stdjson "encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

func TestLoadCacheTorrentsReadsDirectoriesInBatches(t *testing.T) {
	cacheRoot := t.TempDir()
	providerRoot := filepath.Join(cacheRoot, "provider")
	if err := os.Mkdir(providerRoot, 0o700); err != nil {
		t.Fatal(err)
	}

	const count = legacyCacheReadBatch + 17
	for i := range count {
		cached := storage.CachedTorrent{
			ID:       fmt.Sprintf("id-%d", i),
			InfoHash: fmt.Sprintf("%040x", i+1),
			Name:     fmt.Sprintf("torrent-%d", i),
			Debrid:   "provider",
		}
		data, err := stdjson.Marshal(cached)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(
			filepath.Join(providerRoot, fmt.Sprintf("%04d.json", i)),
			data,
			0o600,
		); err != nil {
			t.Fatal(err)
		}
	}

	migrator := &Migrator{cacheDir: cacheRoot, logger: zerolog.Nop()}
	got, err := migrator.loadCacheTorrents()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != count {
		t.Fatalf("loaded %d torrents, want %d", len(got), count)
	}
}

func TestLoadCacheTorrentsRejectsOversizedSparseMetadata(t *testing.T) {
	cacheRoot := t.TempDir()
	providerRoot := filepath.Join(cacheRoot, "provider")
	if err := os.Mkdir(providerRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(
		filepath.Join(providerRoot, "oversized.json"),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(legacyCacheMaxFileBytes + 1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	migrator := &Migrator{cacheDir: cacheRoot, logger: zerolog.Nop()}
	if _, err := migrator.loadCacheTorrents(); err == nil {
		t.Fatal("oversized legacy cache metadata was accepted")
	}
}

func TestLoadCacheTorrentsDoesNotFollowMetadataSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("creating symlinks may require elevated privileges on Windows")
	}
	cacheRoot := t.TempDir()
	providerRoot := filepath.Join(cacheRoot, "provider")
	if err := os.Mkdir(providerRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside.json")
	data, err := stdjson.Marshal(storage.CachedTorrent{
		ID:       "outside",
		InfoHash: "0123456789012345678901234567890123456789",
		Name:     "outside",
		Debrid:   "provider",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outside, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(providerRoot, "linked.json")); err != nil {
		t.Fatal(err)
	}

	migrator := &Migrator{cacheDir: cacheRoot, logger: zerolog.Nop()}
	got, err := migrator.loadCacheTorrents()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("loaded symlinked metadata: %#v", got)
	}
}

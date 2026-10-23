package vfs

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSeedboxUpgradeRetainsMaximumNameCache(t *testing.T) {
	cache := newTestCache(t.TempDir())
	directory := filepath.Join(cache.config.CacheDir, "entry")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	filename := strings.Repeat("a", 251) + ".mkv"
	if err := os.WriteFile(filepath.Join(directory, filename), []byte("cached"), 0o600); err != nil {
		t.Fatal(err)
	}
	metadataPath := filepath.Join(directory, fmt.Sprintf(".tessarr-meta-%x.json", sha256.Sum256([]byte(filename))))
	metadata, err := json.Marshal(map[string]any{"filename": filename, "size": 6, "ranges": []map[string]int{{"Pos": 0, "Size": 6}}, "atime": time.Now(), "mod_time": time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metadataPath, metadata, 0o600); err != nil {
		t.Fatal(err)
	}
	scan := cache.scanDiskCandidates()
	if _, err := os.Stat(metadataPath); err != nil {
		t.Fatalf("upgrade removed persisted metadata for an existing 255-byte cache filename: %v; scan=%+v", err, scan)
	}
	if scan.totalSize != 6 || len(scan.candidates) != 1 || scan.candidates[0].dataPath != filepath.Join(directory, filename) {
		t.Fatalf("upgrade lost existing cache accounting: %+v", scan)
	}
}

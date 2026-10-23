package vfs

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/mount/dfs/vfs/ranges"
)

func TestCacheMetadataMaximumNativeNames(t *testing.T) {
	for _, filename := range []string{strings.Repeat("a", 242) + ".mkv", strings.Repeat("b", 246) + ".mkv", strings.Repeat("c", 251) + ".mkv", strings.Repeat("界", 83) + ".mkv"} {
		t.Run(fmt.Sprintf("%d-bytes", len(filename)), func(t *testing.T) {
			cache := newTestCache(t.TempDir())
			directory := filepath.Join(cache.config.CacheDir, "entry")
			if err := os.Mkdir(directory, 0o755); err != nil {
				t.Fatal(err)
			}
			dataPath := filepath.Join(directory, filename)
			if err := os.WriteFile(dataPath, []byte("cached"), 0o600); err != nil {
				t.Fatal(err)
			}
			metaPath := cacheMetadataPath(directory, filename)
			item := &CacheItem{cache: cache, key: buildCacheKey("entry", filename), metaPath: metaPath, info: ItemInfo{Size: 6, Rs: ranges.Ranges{{Pos: 0, Size: 6}}, ATime: time.Now(), ModTime: time.Now()}}
			if filepath.Base(metaPath) != filename+".json" {
				item.info.Filename = filename
			}
			item.metaDirty.Store(true)
			item.flushMetadata(false)
			if item.metaDirty.Load() {
				t.Fatal("metadata publication failed")
			}
			var persisted ItemInfo
			if err := decodeJSONFile(metaPath, &persisted); err != nil {
				t.Fatal(err)
			}
			if persisted.Rs.Size() != 6 {
				t.Fatalf("persisted ranges = %+v", persisted.Rs)
			}
			for _, cacheInstance := range []*Cache{cache, newTestCache(cache.config.CacheDir)} {
				scan := cacheInstance.scanDiskCandidates()
				if scan.errors != 0 || scan.totalSize != 6 || len(scan.candidates) != 1 {
					t.Fatalf("scan lost cache accounting: %+v", scan)
				}
				candidate := scan.candidates[0]
				if candidate.key != item.key || candidate.dataPath != dataPath || candidate.metaPath != metaPath {
					t.Fatalf("scan changed identity: %+v", candidate)
				}
			}
			files, err := os.ReadDir(directory)
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != 2 {
				t.Fatalf("temporary file leaked: %+v", files)
			}
			cache.items.Store(item.key, item)
			item.opens.Store(1)
			scan := cache.scanDiskCandidates()
			if len(scan.candidates) != 1 || !scan.candidates[0].inMap || scan.candidates[0].opens != 1 {
				t.Fatalf("scan lost the live item identity: %+v", scan)
			}
			if _, removed, failures, _ := cache.evictCandidates(time.Now(), scan.candidates, 6, 0); removed != 0 || failures != 0 {
				t.Fatalf("evicted busy item: removed=%d failures=%d", removed, failures)
			}
			cache.items.Delete(item.key)
			item.opens.Store(0)
			scan = cache.scanDiskCandidates()
			if remaining, removed, failures, _ := cache.evictCandidates(time.Now(), scan.candidates, 6, 0); remaining != 0 || removed != 1 || failures != 0 {
				t.Fatalf("closed cache reclamation: remaining=%d removed=%d failures=%d", remaining, removed, failures)
			}
		})
	}
}

func TestCacheMetadataRejectsCrossFileIdentity(t *testing.T) {
	for _, target := range []string{"../outside", filepath.Join(t.TempDir(), "outside"), "other.mkv"} {
		cache := newTestCache(t.TempDir())
		directory := filepath.Join(cache.config.CacheDir, "entry")
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "claimed.mkv.json"), []byte(`{"size":1,"filename":`+strconv.Quote(target)+`,"ranges":[{"Pos":0,"Size":1}]}`), 0o600); err != nil {
			t.Fatal(err)
		}
		scan := cache.scanDiskCandidates()
		if scan.errors != 1 || len(scan.candidates) != 0 {
			t.Fatalf("accepted foreign identity: %+v", scan)
		}
	}
}

func TestCacheMetadataAtomicReplacement(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "video.mkv.json")
	for _, contents := range []string{"first", "replacement"} {
		if err := writeCacheMetadata(path, []byte(contents)); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(path)
		if err != nil || string(got) != contents {
			t.Fatalf("metadata = %q, %v; want %q", got, err, contents)
		}
		if runtime.GOOS != "windows" {
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0o600 {
				t.Fatalf("metadata should remain private after atomic publication: info=%v err=%v", info, err)
			}
		}
	}
	files, err := os.ReadDir(directory)
	if err != nil || len(files) != 1 {
		t.Fatalf("temporary files after replacement: %+v, %v", files, err)
	}
}

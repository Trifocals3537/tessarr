package dfs

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

var cacheWarmTestRoot string

func TestMain(m *testing.M) {
	root, err := os.MkdirTemp("", "tessarr-dfs-cache-warm-*")
	if err != nil {
		panic(err)
	}
	cacheWarmTestRoot = root
	code := m.Run()
	_ = logger.Close()
	_ = os.RemoveAll(root)
	os.Exit(code)
}

func newCacheWarmResolveFixture(t *testing.T) (*Manager, string, string) {
	t.Helper()

	configRoot := filepath.Join(cacheWarmTestRoot, strings.NewReplacer("\\", "-", "/", "-").Replace(t.Name()))
	mountRoot := filepath.Join(configRoot, "mount")
	if err := os.MkdirAll(configRoot, 0o755); err != nil {
		t.Fatalf("create config root: %v", err)
	}
	config.SetConfigPath(configRoot)
	cfg := config.Get()
	cfg.Mount.MountPath = mountRoot

	managerInstance := manager.New()
	t.Cleanup(func() {
		_ = managerInstance.Stop()
	})

	fileName := "Season 01/Episode 01.mkv"
	entry := &storage.Entry{
		InfoHash:       "cache-warm-resolve",
		Name:           "Release",
		Protocol:       config.ProtocolTorrent,
		ActiveProvider: "test",
		Providers: map[string]*storage.ProviderEntry{
			"test": {
				Provider: "test",
				ID:       "placement-1",
				Files: map[string]*storage.ProviderFile{
					fileName: {Link: "https://example.invalid/video"},
				},
			},
		},
		Files: map[string]*storage.File{
			fileName: {
				ID:       "file-1",
				Name:     fileName,
				Size:     123,
				AddedOn:  time.Now(),
				InfoHash: "cache-warm-resolve",
			},
		},
	}
	if err := managerInstance.Storage().AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	dfsManager := NewManager(managerInstance)
	return dfsManager, mountRoot, fileName
}

func TestResolveCacheWarmFileInfoUsesDirectMountPathWithoutStat(t *testing.T) {
	dfsManager, mountRoot, fileName := newCacheWarmResolveFixture(t)
	target := filepath.Join(mountRoot, manager.EntryAllFolder, "Release", filepath.FromSlash(fileName))

	info, err := dfsManager.resolveCacheWarmFileInfo(target)
	if err != nil {
		t.Fatalf("resolveCacheWarmFileInfo() error = %v", err)
	}
	if info.Parent() != "Release" || info.Name() != fileName || info.Size() != 123 {
		t.Fatalf("resolved info = parent:%q name:%q size:%d", info.Parent(), info.Name(), info.Size())
	}
}

func TestResolveCacheWarmFileInfoUsesSymlinkTargetWithoutFollowingIt(t *testing.T) {
	dfsManager, mountRoot, fileName := newCacheWarmResolveFixture(t)
	linkRoot := t.TempDir()
	linkPath := filepath.Join(linkRoot, "episode.mkv")
	target := filepath.Join(mountRoot, manager.EntryAllFolder, "Release", filepath.FromSlash(fileName))
	if err := os.Symlink(target, linkPath); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}

	info, err := dfsManager.resolveCacheWarmFileInfo(linkPath)
	if err != nil {
		t.Fatalf("resolveCacheWarmFileInfo() error = %v", err)
	}
	if info.Parent() != "Release" || info.Name() != fileName {
		t.Fatalf("resolved info = parent:%q name:%q", info.Parent(), info.Name())
	}
}

func TestResolveCacheWarmFileInfoRejectsOutsideSymlinkTarget(t *testing.T) {
	dfsManager, _, _ := newCacheWarmResolveFixture(t)
	linkRoot := t.TempDir()
	linkPath := filepath.Join(linkRoot, "outside.mkv")
	target := filepath.Join(linkRoot, "outside-target.mkv")
	if err := os.Symlink(target, linkPath); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}

	if _, err := dfsManager.resolveCacheWarmFileInfo(linkPath); !errors.Is(err, manager.ErrCacheWarmUnavailable) {
		t.Fatalf("resolveCacheWarmFileInfo() error = %v, want %v", err, manager.ErrCacheWarmUnavailable)
	}
}

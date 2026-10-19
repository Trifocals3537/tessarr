package rclone

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/rs/zerolog"
)

func TestNewManagerPropagatesUnsafeConfigDirectory(t *testing.T) {
	oldConfigPath := config.GetMainPath()
	config.Reset()
	configRoot := t.TempDir()
	if err := config.SetConfigPath(configRoot); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		config.Reset()
		_ = config.SetConfigPath(oldConfigPath)
	})

	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(configRoot, "rclone")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if manager, err := NewManager(nil); err == nil {
		if manager != nil {
			_ = manager.Stop()
		}
		t.Fatal("NewManager() silently accepted an unsafe rclone config directory")
	}
}

func TestStartRejectsUnsafeConfiguredCacheDirectory(t *testing.T) {
	oldConfigPath := config.GetMainPath()
	config.Reset()
	configRoot := t.TempDir()
	if err := config.SetConfigPath(configRoot); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		config.Reset()
		_ = config.SetConfigPath(oldConfigPath)
	})

	outside := t.TempDir()
	cacheLink := filepath.Join(t.TempDir(), "cache")
	if err := os.Symlink(outside, cacheLink); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	cfg := config.Get()
	cfg.Mount.Rclone.CacheDir = cacheLink
	m := &Manager{ctx: context.Background(), logger: zerolog.Nop()}
	if err := m.Start(context.Background()); err == nil {
		t.Fatal("Start() silently ignored an unsafe rclone cache directory")
	}
}

//go:build linux

package hanwen

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	internalconfig "github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	mountconfig "github.com/Trifocals3537/tessarr/pkg/mount/dfs/config"
	"github.com/Trifocals3537/tessarr/pkg/mount/dfs/vfs"
)

// TestMountReadSeekUnmount is opt-in because CI runners and containers often
// expose no usable /dev/fuse. Run it on a Linux host with:
//
//	TESSARR_FUSE_INTEGRATION=1 go test -run TestMountReadSeekUnmount ./pkg/mount/dfs/backend/hanwen
func TestMountReadSeekUnmount(t *testing.T) {
	if os.Getenv("TESSARR_FUSE_INTEGRATION") != "1" {
		t.Skip("set TESSARR_FUSE_INTEGRATION=1 to run the real FUSE canary")
	}
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Fatalf("usable /dev/fuse is required: %v", err)
	}

	internalconfig.SetConfigPath(t.TempDir())
	internalconfig.Reset()
	t.Cleanup(internalconfig.Reset)

	managerInstance := manager.New()
	t.Cleanup(func() {
		if err := managerInstance.Stop(); err != nil {
			t.Errorf("stop manager: %v", err)
		}
	})

	cfg := mountconfig.DefaultFuseConfig()
	cfg.MountPath = t.TempDir()
	cfg.CacheDir = t.TempDir()
	cfg.UID = uint32(os.Getuid())
	cfg.GID = uint32(os.Getgid())

	vfsManager, err := vfs.NewManager(context.Background(), managerInstance, cfg)
	if err != nil {
		t.Fatalf("create VFS manager: %v", err)
	}
	backend, err := NewBackend(vfsManager, cfg)
	if err != nil {
		t.Fatalf("create Hanwen backend: %v", err)
	}

	mountCtx, cancelMount := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelMount()
	if err := backend.Mount(mountCtx); err != nil {
		t.Fatalf("mount Hanwen backend: %v", err)
	}
	if !backend.IsReady() {
		t.Fatal("backend did not report ready after mount")
	}
	if active, err := mountPointActive(cfg.MountPath); err != nil || !active {
		t.Fatalf("mounted path was not present in mountinfo: active=%v err=%v", active, err)
	}
	mounted := true
	t.Cleanup(func() {
		if !mounted {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := backend.Unmount(ctx); err != nil {
			t.Errorf("cleanup unmount: %v", err)
		}
	})

	entries, err := os.ReadDir(cfg.MountPath)
	if err != nil {
		t.Fatalf("scan mounted root: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("mounted root is empty")
	}

	versionPath := filepath.Join(cfg.MountPath, "version.txt")
	want, err := os.ReadFile(versionPath)
	if err != nil {
		t.Fatalf("read mounted version.txt: %v", err)
	}
	if len(want) < 4 {
		t.Fatalf("mounted version.txt is unexpectedly short: %q", want)
	}

	file, err := os.Open(versionPath)
	if err != nil {
		t.Fatalf("open mounted version.txt: %v", err)
	}
	if _, err := file.Seek(1, io.SeekStart); err != nil {
		_ = file.Close()
		t.Fatalf("seek mounted version.txt: %v", err)
	}
	got := make([]byte, 3)
	if _, err := io.ReadFull(file, got); err != nil {
		_ = file.Close()
		t.Fatalf("read after seek: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close mounted version.txt: %v", err)
	}
	if !bytes.Equal(got, want[1:4]) {
		t.Fatalf("seek read = %q, want %q", got, want[1:4])
	}

	var workers sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for range 25 {
				got, err := os.ReadFile(versionPath)
				if err != nil {
					errs <- fmt.Errorf("worker %d read: %w", worker, err)
					return
				}
				if !bytes.Equal(got, want) {
					errs <- fmt.Errorf("worker %d read inconsistent content", worker)
					return
				}
			}
		}(i)
	}
	workers.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	unmountCtx, cancelUnmount := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelUnmount()
	if err := backend.Unmount(unmountCtx); err != nil {
		t.Fatalf("unmount Hanwen backend: %v", err)
	}
	mounted = false
	if backend.IsReady() {
		t.Fatal("backend still reports ready after unmount")
	}
	if active, err := mountPointActive(cfg.MountPath); err != nil || active {
		t.Fatalf("unmounted path still present in mountinfo: active=%v err=%v", active, err)
	}
	if err := backend.Unmount(unmountCtx); err != nil {
		t.Fatalf("second unmount was not idempotent: %v", err)
	}
	if _, err := os.Stat(versionPath); !os.IsNotExist(err) {
		t.Fatalf("version.txt still visible after unmount: %v", err)
	}
}

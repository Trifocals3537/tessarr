package filelock

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAcquireContextSerializesPinnedLockFile(t *testing.T) {
	rootPath := t.TempDir()
	rooted, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rooted.Close()

	first, err := AcquireContext(context.Background(), rooted, "owner.lock", 0o600, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if second, err := AcquireContext(ctx, rooted, "owner.lock", 0o600, time.Millisecond); err == nil {
		_ = second.Unlock()
		t.Fatal("second lock unexpectedly succeeded")
	}
	if err := first.Unlock(); err != nil {
		t.Fatal(err)
	}
	third, err := AcquireContext(context.Background(), rooted, "owner.lock", 0o600, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := third.Unlock(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireContextRejectsSymlinkLockFile(t *testing.T) {
	rootPath := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.lock")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(rootPath, "owner.lock")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	rooted, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rooted.Close()
	if lock, err := AcquireContext(context.Background(), rooted, "owner.lock", 0o600, time.Millisecond); err == nil {
		_ = lock.Unlock()
		t.Fatal("symlink lock file was accepted")
	}
}

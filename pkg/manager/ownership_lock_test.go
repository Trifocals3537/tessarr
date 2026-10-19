package manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOwnershipRootKeepsMutationPinnedAfterPathReplacement(t *testing.T) {
	visible := filepath.Join(t.TempDir(), "downloads")
	ownership, _, err := acquireOwnershipRoot(visible, ".owner.lock", true, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ownership.close() })

	moved := visible + "-moved"
	if err := os.Rename(visible, moved); err != nil {
		t.Skipf("cannot replace an open directory on this platform: %v", err)
	}
	if err := os.Mkdir(visible, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ownership.root.Mkdir("pinned-write", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(moved, "pinned-write")); err != nil {
		t.Fatalf("pinned mutation did not reach original root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(visible, "pinned-write")); !os.IsNotExist(err) {
		t.Fatalf("pinned mutation reached replacement root: %v", err)
	}
	if err := ownership.close(); err == nil {
		t.Fatal("ownership close did not report the replaced visible root")
	}
}

func TestCompatibleOwnershipRootContendsWithLegacyLock(t *testing.T) {
	root := t.TempDir()
	legacy, _, err := acquireOwnershipRoot(root, ".legacy-owner.lock", true, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	if compatible, _, err := acquireCompatibleOwnershipRoot(
		root,
		[]string{".legacy-owner.lock", ".current-owner.lock"},
		true,
		50*time.Millisecond,
		time.Millisecond,
	); err == nil {
		_ = compatible.close()
		t.Fatal("compatible ownership lock ignored the legacy lock")
	}
	if err := legacy.close(); err != nil {
		t.Fatal(err)
	}

	compatible, _, err := acquireCompatibleOwnershipRoot(
		root,
		[]string{".legacy-owner.lock", ".current-owner.lock"},
		true,
		time.Second,
		time.Millisecond,
	)
	if err != nil {
		t.Fatalf("compatible ownership lock failed after legacy release: %v", err)
	}
	if err := compatible.close(); err != nil {
		t.Fatal(err)
	}
}

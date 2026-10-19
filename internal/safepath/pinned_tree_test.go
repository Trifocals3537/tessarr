package safepath

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRemovePinnedTreeContentsPreservesMarkerAndDoesNotFollowSymlinks(t *testing.T) {
	rootPath := t.TempDir()
	outside := t.TempDir()
	outsideSentinel := filepath.Join(outside, "keep")
	if err := os.WriteFile(outsideSentinel, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(rootPath, "nested", "deeper"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "nested", "deeper", "payload"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "owner.marker"), []byte("owner"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(rootPath, "outside-link")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	root, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := RemovePinnedTreeContents(root, PinnedTreeRemovalOptions{
		MaxEntries:       100,
		MaxDepth:         8,
		ReadBatch:        2,
		PreserveTopLevel: []string{"owner.marker"},
	}); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "owner.marker" {
		t.Fatalf("remaining entries = %v, want owner.marker only", entries)
	}
	contents, err := os.ReadFile(outsideSentinel)
	if err != nil || string(contents) != "outside" {
		t.Fatalf("outside sentinel changed: contents=%q error=%v", contents, err)
	}
}

func TestRemovePinnedTreeContentsUsesPinnedDirectoryAfterVisibleSwap(t *testing.T) {
	parent := t.TempDir()
	visible := filepath.Join(parent, "quarantine")
	moved := filepath.Join(parent, "moved-owned")
	if err := os.Mkdir(visible, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(visible, "owned"), []byte("owned"), 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(visible)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := os.Rename(visible, moved); err != nil {
		t.Skipf("cannot rename an open directory on this platform: %v", err)
	}
	if err := os.Mkdir(visible, 0o700); err != nil {
		t.Fatal(err)
	}
	replacement := filepath.Join(visible, "preserve")
	if err := os.WriteFile(replacement, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := RemovePinnedTreeContents(root, PinnedTreeRemovalOptions{
		MaxEntries: 10,
		MaxDepth:   4,
		ReadBatch:  2,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(moved, "owned")); !os.IsNotExist(err) {
		t.Fatalf("owned content remained in moved pinned directory: %v", err)
	}
	contents, err := os.ReadFile(replacement)
	if err != nil || string(contents) != "replacement" {
		t.Fatalf("visible replacement changed: contents=%q error=%v", contents, err)
	}
}

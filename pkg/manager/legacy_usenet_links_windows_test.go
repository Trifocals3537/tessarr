//go:build windows

package manager

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsSymlinkLinkCountUsesPinnedRootHandle(t *testing.T) {
	rootPath := t.TempDir()
	targetPath := filepath.Join(rootPath, "target.bin")
	linkPath := filepath.Join(rootPath, "artifact.link")
	if err := os.WriteFile(targetPath, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(targetPath, linkPath); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	rooted, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rooted.Close()
	before, err := rooted.Lstat("artifact.link")
	if err != nil {
		t.Fatal(err)
	}
	links, err := legacyUsenetSymlinkLinkCount(rooted, "artifact.link", before)
	if err != nil {
		t.Fatal(err)
	}
	if links != 1 {
		t.Fatalf("symlink link count = %d, want 1", links)
	}

	if err := os.Rename(linkPath, filepath.Join(rootPath, "moved.link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(targetPath, linkPath); err != nil {
		t.Fatal(err)
	}
	if _, err := legacyUsenetSymlinkLinkCount(rooted, "artifact.link", before); err == nil {
		t.Fatal("replacement symlink was accepted as the originally inspected artifact")
	}
}

//go:build windows

package manager

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsTorrentOpenFileLinkCountTracksHardLinks(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "first.part")
	second := filepath.Join(root, "second.part")
	if err := os.WriteFile(first, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(first)
	if err != nil {
		t.Fatal(err)
	}
	count, err := torrentOpenFileLinkCount(file)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("initial link count = %d, want 1", count)
	}
	if err := os.Link(first, second); err != nil {
		t.Skipf("hard links unavailable on test volume: %v", err)
	}
	file, err = os.Open(first)
	if err != nil {
		t.Fatal(err)
	}
	count, err = torrentOpenFileLinkCount(file)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("hard-link count = %d, want 2", count)
	}
}

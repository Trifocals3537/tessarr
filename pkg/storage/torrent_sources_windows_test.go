//go:build windows

package storage

import (
	"os"
	"path/filepath"
	"testing"
)

type recordingTorrentSourceRoot struct {
	directory string
	renamed   bool
}

func (r *recordingTorrentSourceRoot) Rename(source, destination string) error {
	r.renamed = true
	return os.Rename(filepath.Join(r.directory, source), filepath.Join(r.directory, destination))
}

func (r *recordingTorrentSourceRoot) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(filepath.Join(r.directory, name), flag, perm)
}

func TestWindowsReplaceTorrentSourceUsesRootOperations(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "source.tmp"), []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "destination.torrent"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	rooted := &recordingTorrentSourceRoot{directory: directory}
	if err := replaceTorrentSourceAtRoot(rooted, "source.tmp", "destination.torrent"); err != nil {
		t.Fatal(err)
	}
	if !rooted.renamed {
		t.Fatal("replacement did not use the pinned root rename operation")
	}
	got, err := os.ReadFile(filepath.Join(directory, "destination.torrent"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "source" {
		t.Fatalf("destination = %q, want source", got)
	}
}

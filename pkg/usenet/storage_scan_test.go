package usenet

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScanMetadataDirectoryAdvancesAcrossBatches(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	const fileCount = 11
	for i := 0; i < fileCount; i++ {
		name := filepath.Join(root, string(rune('a'+i))+".meta")
		if err := os.WriteFile(name, []byte("test"), 0o600); err != nil {
			t.Fatalf("write test metadata: %v", err)
		}
	}

	var visited int
	if err := scanMetadataDirectory(root, 3, func(os.DirEntry) error {
		visited++
		return nil
	}); err != nil {
		t.Fatalf("scanMetadataDirectory() error = %v", err)
	}
	if visited != fileCount {
		t.Fatalf("scanMetadataDirectory() visited %d entries, want %d", visited, fileCount)
	}
}

func TestScanMetadataDirectoryRejectsInvalidArguments(t *testing.T) {
	t.Parallel()

	if err := scanMetadataDirectory(t.TempDir(), 0, func(os.DirEntry) error { return nil }); err == nil {
		t.Fatal("scanMetadataDirectory() accepted a zero batch size")
	}
	if err := scanMetadataDirectory(t.TempDir(), 1, nil); err == nil {
		t.Fatal("scanMetadataDirectory() accepted a nil callback")
	}
}

//go:build linux

package hanwen

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMountPointActiveRejectsOrdinaryDirectory(t *testing.T) {
	mounted, err := mountPointActive(t.TempDir())
	if err != nil {
		t.Fatalf("inspect ordinary directory: %v", err)
	}
	if mounted {
		t.Fatal("ordinary temporary directory reported as a mountpoint")
	}
}

func TestMountInfoPathDecoding(t *testing.T) {
	encoded := `/tmp/media\040files/line\011tab/new\012line/back\134slash`
	want := "/tmp/media files/line\ttab/new\nline/back\\slash"
	if got := mountInfoPathReplacer.Replace(encoded); got != want {
		t.Fatalf("decoded mount path = %q, want %q", got, want)
	}
}

func TestMountPointActiveFindsRootMount(t *testing.T) {
	root, err := filepath.Abs(string(os.PathSeparator))
	if err != nil {
		t.Fatal(err)
	}
	mounted, err := mountPointActive(root)
	if err != nil {
		t.Fatalf("inspect root mount: %v", err)
	}
	if !mounted {
		t.Fatal("root filesystem was not found in mountinfo")
	}
}

//go:build linux || (darwin && amd64)

package hanwen

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog"
)

func TestMountOptionsRecoverHandlerPanics(t *testing.T) {
	b := &Backend{logger: zerolog.Nop()}
	opts := b.mountOptions()

	if opts.PanicHandler == nil {
		t.Fatal("mount options do not install a panic handler")
	}
	if status := opts.PanicHandler("test panic"); status != fuse.EIO {
		t.Fatalf("panic handler status = %v, want EIO", status)
	}
	if opts.MaxWrite != 1024*1024 {
		t.Fatalf("MaxWrite = %d, want 1 MiB", opts.MaxWrite)
	}
	if !opts.AllowOther {
		t.Fatal("AllowOther must remain enabled for media-server access")
	}
}

func TestNormalizeMountPathRejectsUnsafeTargets(t *testing.T) {
	for _, path := range []string{"", "   ", string(os.PathSeparator)} {
		if _, err := normalizeMountPath(path); err == nil {
			t.Fatalf("normalizeMountPath(%q) unexpectedly succeeded", path)
		}
	}
}

func TestNormalizeMountPathReturnsAbsoluteCleanPath(t *testing.T) {
	want, err := filepath.Abs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	got, err := normalizeMountPath(filepath.Join(want, "."))
	if err != nil {
		t.Fatalf("normalize mount path: %v", err)
	}
	if got != filepath.Clean(want) {
		t.Fatalf("normalized mount path = %q, want %q", got, filepath.Clean(want))
	}
}

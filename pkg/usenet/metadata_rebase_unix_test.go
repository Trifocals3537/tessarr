//go:build unix

package usenet

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/storage"
	"golang.org/x/sys/unix"
)

func TestRebaseNZBMetadataPathsPreservesModeUnderUmask(t *testing.T) {
	const childFlag = "TESSARR_TEST_METADATA_UMASK_CHILD"
	if os.Getenv(childFlag) != "1" {
		// Umask is process-wide. Change it only in an isolated child so other
		// tests and background goroutines retain their normal creation modes.
		command := exec.Command(os.Args[0], "-test.run=^TestRebaseNZBMetadataPathsPreservesModeUnderUmask$")
		command.Env = append(os.Environ(), childFlag+"=1")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("isolated umask regression: %v\n%s", err, output)
		}
		return
	}
	unix.Umask(0o022)
	root := t.TempDir()
	source := filepath.Join(root, "old")
	target := filepath.Join(root, "new")
	metaDir := filepath.Join(root, "stage", "usenet", "meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(source, "usenet", "nzbs"), 0o755); err != nil {
		t.Fatal(err)
	}
	id := "123e4567-e89b-12d3-a456-426614174000"
	metaPath := filepath.Join(metaDir, id+".meta")
	encoded, err := encodeNZBV2(&storage.NZB{ID: id, Path: filepath.Join(source, "usenet", "nzbs", id+".nzb")})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeMetadataFile(metaDir, metaPath, encoded, 0o660); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(metaPath, 0o660); err != nil {
		t.Fatal(err)
	}
	if count, err := RebaseNZBMetadataPaths(metaDir, source, target); err != nil || count != 1 {
		t.Fatalf("rebase = %d, %v", count, err)
	}
	info, err := os.Stat(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o660 {
		t.Fatalf("original metadata mode 0660 changed to %04o under umask 0022", info.Mode().Perm())
	}
}

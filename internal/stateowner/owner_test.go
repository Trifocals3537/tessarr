package stateowner

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestOwnerProcessHelper(t *testing.T) {
	root := os.Getenv("TESSARR_OWNER_TEST_ROOT")
	if root == "" {
		return
	}
	owner, err := Acquire(root, "owner.lock")
	if errors.Is(err, ErrInUse) {
		os.Exit(81)
	}
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("TESSARR_OWNER_TEST_CRASH") == "1" {
		os.Exit(82)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
}

func ownerChild(root string, crash bool) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run=^TestOwnerProcessHelper$")
	cmd.Env = append(os.Environ(), "TESSARR_OWNER_TEST_ROOT="+root)
	if crash {
		cmd.Env = append(cmd.Env, "TESSARR_OWNER_TEST_CRASH=1")
	}
	return cmd
}

func TestOwnerExcludesAnotherProcessAndRetainsLockFile(t *testing.T) {
	root := t.TempDir()
	first, err := Acquire(root, "owner.lock")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := Acquire(root, "owner.lock"); !errors.Is(err, ErrInUse) {
		t.Fatalf("second owner: %v", err)
	}
	output, err := ownerChild(root, false).CombinedOutput()
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.ExitCode() != 81 {
		t.Fatalf("second process: %v %s", err, output)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "owner.lock")); err != nil {
		t.Fatal("lock coordination file removed", err)
	}
	if output, err := ownerChild(root, false).CombinedOutput(); err != nil {
		t.Fatalf("released owner: %v %s", err, output)
	}
}

func TestOwnerIsReleasedByProcessCrash(t *testing.T) {
	root := t.TempDir()
	output, err := ownerChild(root, true).CombinedOutput()
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.ExitCode() != 82 {
		t.Fatalf("crash helper: %v %s", err, output)
	}
	owner, err := Acquire(root, "owner.lock")
	if err != nil {
		t.Fatal("stale lock file prevented recovery", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal("close is not idempotent", err)
	}
}

func TestOwnerRejectsRedirectedCoordinationFile(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "unrelated")
	if err := os.WriteFile(outside, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "owner.lock")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if owner, err := Acquire(root, "owner.lock"); err == nil {
		_ = owner.Close()
		t.Fatal("symlink lock accepted")
	}
	data, err := os.ReadFile(outside)
	if err != nil || string(data) != "unchanged" {
		t.Fatal("unrelated file changed", err)
	}
}

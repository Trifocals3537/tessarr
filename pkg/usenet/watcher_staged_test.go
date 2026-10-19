package usenet

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStageNZBIsIdempotentOnlyForExactContent(t *testing.T) {
	u := newMetadataTestUsenet(t)
	path, err := u.StageNZB(testNZBID, []byte("submitted"))
	if errors.Is(err, ErrWatchedNZBStageLinkUnavailable) {
		t.Skipf("hard links unavailable on test filesystem: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	retryPath, err := u.StageNZB(testNZBID, []byte("submitted"))
	if err != nil {
		t.Fatal(err)
	}
	if retryPath != path {
		t.Fatalf("retry staged path = %q, want %q", retryPath, path)
	}
	if _, err := u.StageNZB(testNZBID, []byte("tampered")); !errors.Is(err, ErrWatchedNZBStagedConflict) {
		t.Fatalf("conflicting stage error = %v", err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "submitted" {
		t.Fatalf("durable staged state changed: data=%q err=%v", data, err)
	}
}

func TestStageNZBRecoversCommittedTempLink(t *testing.T) {
	u := newMetadataTestUsenet(t)
	tempPath, err := metadataFilePath(u.metadataDir, testNZBID, nzbStagedTempSuffix)
	if err != nil {
		t.Fatal(err)
	}
	stagedPath, err := metadataFilePath(u.metadataDir, testNZBID, nzbStagedSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tempPath, []byte("submitted"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(tempPath, stagedPath); err != nil {
		t.Skipf("hard links unavailable on test filesystem: %v", err)
	}

	got, err := u.StageNZB(testNZBID, []byte("submitted"))
	if err != nil {
		t.Fatal(err)
	}
	if got != stagedPath {
		t.Fatalf("recovered staged path = %q, want %q", got, stagedPath)
	}
	if _, err := os.Lstat(tempPath); !os.IsNotExist(err) {
		t.Fatalf("committed temporary link still exists: %v", err)
	}
}

func TestReadStagedNZBAtIsBoundedNoFollowAndIDBound(t *testing.T) {
	u := newMetadataTestUsenet(t)
	path, err := u.StageNZB(testNZBID, []byte("submitted"))
	if errors.Is(err, ErrWatchedNZBStageLinkUnavailable) {
		t.Skipf("hard links unavailable on test filesystem: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	data, err := ReadStagedNZBAt(u.metadataDir, testNZBID, path, int64(len("submitted")))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "submitted" {
		t.Fatalf("staged data = %q", data)
	}
	if _, err := ReadStagedNZBAt(u.metadataDir, testNZBID, path, 4); err == nil {
		t.Fatal("oversized staged content unexpectedly read")
	}
	if _, err := ReadStagedNZBAt(u.metadataDir, otherTestNZBID, path, 64); err == nil {
		t.Fatal("cross-entry staged path unexpectedly read")
	}

	outside := filepath.Join(t.TempDir(), "outside.queued")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(u.metadataDir, otherTestNZBID+".queued")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := ReadStagedNZBAt(u.metadataDir, otherTestNZBID, link, 64); err == nil {
		t.Fatal("staged symlink unexpectedly read")
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "outside" {
		t.Fatalf("outside target changed: data=%q err=%v", data, err)
	}
}

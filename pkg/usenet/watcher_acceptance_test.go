package usenet

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

const watchedNZBTestMaxBytes int64 = 1024

func watchedNZBTestDigest(contents string) [sha256.Size]byte {
	return sha256.Sum256([]byte(contents))
}

func writeWatchedNZBTestFile(t *testing.T, root, name, contents string) string {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertWatchedNZBTestContents(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	if string(data) != want {
		t.Fatalf("%q contents = %q, want %q", path, data, want)
	}
}

func TestReadClaimedNZBAtIsRootedNoFollowAndBounded(t *testing.T) {
	root := t.TempDir()
	claimed := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "12345")

	data, err := ReadClaimedNZBAt(root, claimed, 5)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "12345" {
		t.Fatalf("claimed data = %q", data)
	}
	if _, err := ReadClaimedNZBAt(root, claimed, 4); err == nil {
		t.Fatal("oversized claimed NZB unexpectedly read")
	}
	if _, err := ReadClaimedNZBAt(root, filepath.Join(root, "release.nzb.accepted"), 5); err == nil {
		t.Fatal("accepted suffix unexpectedly treated as importing")
	}

	outside := writeWatchedNZBTestFile(t, t.TempDir(), "outside.nzb", "outside")
	link := filepath.Join(root, "linked.nzb.importing")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := ReadClaimedNZBAt(root, link, watchedNZBTestMaxBytes); err == nil {
		t.Fatal("symlink claimed NZB unexpectedly read")
	}
	assertWatchedNZBTestContents(t, outside, "outside")
}

func TestReadClaimedNZBSnapshotAtBindsStablePathAndContent(t *testing.T) {
	root := t.TempDir()
	claimed := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "payload")

	snapshot, err := ReadClaimedNZBSnapshotAt(root, claimed, watchedNZBTestMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Path != claimed {
		t.Fatalf("snapshot path = %q, want %q", snapshot.Path, claimed)
	}
	if string(snapshot.Content) != "payload" {
		t.Fatalf("snapshot content = %q", snapshot.Content)
	}
	if snapshot.ContentDigest != watchedNZBTestDigest("payload") {
		t.Fatalf("snapshot digest = %x", snapshot.ContentDigest)
	}
	if snapshot.Size != int64(len("payload")) {
		t.Fatalf("snapshot size = %d", snapshot.Size)
	}
	if snapshot.ModTime.IsZero() {
		t.Fatal("snapshot modification time is zero")
	}
}

func TestReadClaimedNZBSnapshotAtRejectsPathReplacementAfterRead(t *testing.T) {
	root := t.TempDir()
	claimed := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "payload")
	displaced := filepath.Join(root, "release.nzb.displaced")

	_, err := readClaimedNZBSnapshotAt(
		root,
		claimed,
		watchedNZBTestMaxBytes,
		func() {
			if renameErr := os.Rename(claimed, displaced); renameErr != nil {
				t.Skipf("cannot replace an open file on this filesystem: %v", renameErr)
			}
			if writeErr := os.WriteFile(claimed, []byte("replacement"), 0o600); writeErr != nil {
				t.Fatal(writeErr)
			}
		},
	)
	if !errors.Is(err, ErrWatchedNZBClaimChanged) {
		t.Fatalf("path-replacement error = %v", err)
	}
	assertWatchedNZBTestContents(t, displaced, "payload")
	assertWatchedNZBTestContents(t, claimed, "replacement")
}

func TestReadClaimedNZBSnapshotAtRejectsMutationAfterRead(t *testing.T) {
	root := t.TempDir()
	claimed := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "payload")

	_, err := readClaimedNZBSnapshotAt(
		root,
		claimed,
		watchedNZBTestMaxBytes,
		func() {
			if writeErr := os.WriteFile(claimed, []byte("payload-expanded"), 0o600); writeErr != nil {
				t.Fatal(writeErr)
			}
		},
	)
	if !errors.Is(err, ErrWatchedNZBClaimChanged) {
		t.Fatalf("post-read mutation error = %v", err)
	}
	assertWatchedNZBTestContents(t, claimed, "payload-expanded")
}

func TestAcceptClaimedNZBAtCreatesTerminalNameAndRemovesOnlyImportingLink(t *testing.T) {
	root := t.TempDir()
	claimed := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "payload")

	accepted, err := AcceptClaimedNZBAt(
		root,
		claimed,
		watchedNZBTestDigest("payload"),
		watchedNZBTestMaxBytes,
	)
	if errors.Is(err, ErrWatchedNZBHardLinkUnavailable) {
		t.Skipf("hard links unavailable on test filesystem: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	wantAccepted := filepath.Join(root, "release.nzb.accepted")
	if accepted != wantAccepted {
		t.Fatalf("accepted path = %q, want %q", accepted, wantAccepted)
	}
	if _, err := os.Lstat(claimed); !os.IsNotExist(err) {
		t.Fatalf("importing link still exists: %v", err)
	}
	assertWatchedNZBTestContents(t, accepted, "payload")
}

func TestAcceptClaimedNZBAtRecoversLinkBeforeUnlinkCrash(t *testing.T) {
	root := t.TempDir()
	claimed := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "payload")
	accepted := filepath.Join(root, "release.nzb.accepted")
	if err := os.Link(claimed, accepted); err != nil {
		t.Skipf("hard links unavailable on test filesystem: %v", err)
	}

	got, err := AcceptClaimedNZBAt(
		root,
		claimed,
		watchedNZBTestDigest("payload"),
		watchedNZBTestMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got != accepted {
		t.Fatalf("accepted path = %q, want %q", got, accepted)
	}
	if _, err := os.Lstat(claimed); !os.IsNotExist(err) {
		t.Fatalf("crash-recovery importing link still exists: %v", err)
	}
	assertWatchedNZBTestContents(t, accepted, "payload")
}

func TestAcceptClaimedNZBAtRecognizesSameBoundedContentOnDifferentInode(t *testing.T) {
	root := t.TempDir()
	claimed := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "same")
	accepted := writeWatchedNZBTestFile(t, root, "release.nzb.accepted", "same")
	before, err := os.Lstat(accepted)
	if err != nil {
		t.Fatal(err)
	}

	got, err := AcceptClaimedNZBAt(
		root,
		claimed,
		watchedNZBTestDigest("same"),
		watchedNZBTestMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got != accepted {
		t.Fatalf("accepted path = %q, want %q", got, accepted)
	}
	if _, err := os.Lstat(claimed); !os.IsNotExist(err) {
		t.Fatalf("same-content importing file still exists: %v", err)
	}
	after, err := os.Lstat(accepted)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("existing accepted file was replaced")
	}
	assertWatchedNZBTestContents(t, accepted, "same")
}

func TestAcceptClaimedNZBAtRejectsConflictingAcceptedContent(t *testing.T) {
	root := t.TempDir()
	claimed := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "new")
	accepted := writeWatchedNZBTestFile(t, root, "release.nzb.accepted", "old")

	got, err := AcceptClaimedNZBAt(
		root,
		claimed,
		watchedNZBTestDigest("new"),
		watchedNZBTestMaxBytes,
	)
	if got != accepted {
		t.Fatalf("accepted path = %q, want %q", got, accepted)
	}
	if !errors.Is(err, ErrWatchedNZBAcceptedConflict) {
		t.Fatalf("conflicting accepted error = %v", err)
	}
	assertWatchedNZBTestContents(t, claimed, "new")
	assertWatchedNZBTestContents(t, accepted, "old")
}

func TestAcceptClaimedNZBAtRejectsSourceChangedAfterSubmission(t *testing.T) {
	root := t.TempDir()
	claimed := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "changed")

	accepted, err := AcceptClaimedNZBAt(
		root,
		claimed,
		watchedNZBTestDigest("submitted"),
		watchedNZBTestMaxBytes,
	)
	if !errors.Is(err, ErrWatchedNZBAcceptedConflict) {
		t.Fatalf("changed importing error = %v", err)
	}
	if accepted != filepath.Join(root, "release.nzb.accepted") {
		t.Fatalf("accepted path = %q", accepted)
	}
	assertWatchedNZBTestContents(t, claimed, "changed")
	if _, statErr := os.Lstat(accepted); !os.IsNotExist(statErr) {
		t.Fatalf("terminal marker exists for changed source: %v", statErr)
	}
}

func TestAcceptClaimedNZBAtPreservesImportingWhenHardLinksUnavailable(t *testing.T) {
	root := t.TempDir()
	claimed := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "payload")
	accepted := filepath.Join(root, "release.nzb.accepted")

	got, err := acceptClaimedNZBAt(
		root,
		claimed,
		watchedNZBTestDigest("payload"),
		watchedNZBTestMaxBytes,
		func(*os.Root, string, string) error {
			return fs.ErrInvalid
		},
	)
	if got != accepted {
		t.Fatalf("accepted path = %q, want %q", got, accepted)
	}
	if !errors.Is(err, ErrWatchedNZBHardLinkUnavailable) {
		t.Fatalf("hard-link failure = %v", err)
	}
	assertWatchedNZBTestContents(t, claimed, "payload")
	if _, statErr := os.Lstat(accepted); !os.IsNotExist(statErr) {
		t.Fatalf("accepted file exists after hard-link failure: %v", statErr)
	}
}

func TestAcceptClaimedNZBAtPreservesImportingWhenLinkSyncFails(t *testing.T) {
	root := t.TempDir()
	claimed := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "payload")
	accepted := filepath.Join(root, "release.nzb.accepted")
	syncFailure := errors.New("injected directory sync failure")

	got, err := acceptClaimedNZBAtWithOps(
		root,
		claimed,
		watchedNZBTestDigest("payload"),
		watchedNZBTestMaxBytes,
		func(rooted *os.Root, importingLeaf, acceptedLeaf string) error {
			return rooted.Link(importingLeaf, acceptedLeaf)
		},
		func(*os.Root) error {
			return syncFailure
		},
	)
	if errors.Is(err, ErrWatchedNZBHardLinkUnavailable) {
		t.Skipf("hard links unavailable on test filesystem: %v", err)
	}
	if !errors.Is(err, syncFailure) {
		t.Fatalf("link-sync failure = %v", err)
	}
	if got != accepted {
		t.Fatalf("accepted path = %q, want %q", got, accepted)
	}
	assertWatchedNZBTestContents(t, claimed, "payload")
	assertWatchedNZBTestContents(t, accepted, "payload")
}

func TestAcceptClaimedNZBAtRetainsRecoverableAcceptedNameWhenUnlinkSyncFails(t *testing.T) {
	root := t.TempDir()
	claimed := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "payload")
	accepted := filepath.Join(root, "release.nzb.accepted")
	syncFailure := errors.New("injected unlink sync failure")
	syncCalls := 0

	got, err := acceptClaimedNZBAtWithOps(
		root,
		claimed,
		watchedNZBTestDigest("payload"),
		watchedNZBTestMaxBytes,
		func(rooted *os.Root, importingLeaf, acceptedLeaf string) error {
			return rooted.Link(importingLeaf, acceptedLeaf)
		},
		func(*os.Root) error {
			syncCalls++
			if syncCalls == 2 {
				return syncFailure
			}
			return nil
		},
	)
	if errors.Is(err, ErrWatchedNZBHardLinkUnavailable) {
		t.Skipf("hard links unavailable on test filesystem: %v", err)
	}
	if !errors.Is(err, syncFailure) {
		t.Fatalf("unlink-sync failure = %v", err)
	}
	if got != accepted {
		t.Fatalf("accepted path = %q, want %q", got, accepted)
	}
	if syncCalls != 2 {
		t.Fatalf("directory sync calls = %d, want 2", syncCalls)
	}
	if _, statErr := os.Lstat(claimed); !os.IsNotExist(statErr) {
		t.Fatalf("importing link remains after unlink-sync failure: %v", statErr)
	}
	assertWatchedNZBTestContents(t, accepted, "payload")

	recovered, retryErr := AcceptClaimedNZBAt(
		root,
		claimed,
		watchedNZBTestDigest("payload"),
		watchedNZBTestMaxBytes,
	)
	if retryErr != nil {
		t.Fatalf("retry terminal recovery = %v", retryErr)
	}
	if recovered != accepted {
		t.Fatalf("recovered path = %q, want %q", recovered, accepted)
	}
}

func TestAcceptClaimedNZBAtTreatsExistingTerminalNameAsIdempotent(t *testing.T) {
	root := t.TempDir()
	claimed := filepath.Join(root, "release.nzb.importing")
	accepted := writeWatchedNZBTestFile(t, root, "release.nzb.accepted", "payload")

	got, err := AcceptClaimedNZBAt(
		root,
		claimed,
		watchedNZBTestDigest("payload"),
		watchedNZBTestMaxBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got != accepted {
		t.Fatalf("accepted path = %q, want %q", got, accepted)
	}
	assertWatchedNZBTestContents(t, accepted, "payload")
}

func TestAcceptClaimedNZBAtRejectsWrongExistingTerminalContent(t *testing.T) {
	root := t.TempDir()
	claimed := filepath.Join(root, "release.nzb.importing")
	accepted := writeWatchedNZBTestFile(t, root, "release.nzb.accepted", "other")

	got, err := AcceptClaimedNZBAt(
		root,
		claimed,
		watchedNZBTestDigest("submitted"),
		watchedNZBTestMaxBytes,
	)
	if got != accepted {
		t.Fatalf("accepted path = %q, want %q", got, accepted)
	}
	if !errors.Is(err, ErrWatchedNZBAcceptedConflict) {
		t.Fatalf("wrong terminal content error = %v", err)
	}
	assertWatchedNZBTestContents(t, accepted, "other")
}

func TestAcceptClaimedNZBAtRejectsOversizedAndUnsafeSources(t *testing.T) {
	root := t.TempDir()
	claimed := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "12345")
	if _, err := AcceptClaimedNZBAt(root, claimed, watchedNZBTestDigest("12345"), 4); err == nil {
		t.Fatal("oversized importing source unexpectedly accepted")
	}
	assertWatchedNZBTestContents(t, claimed, "12345")

	outside := writeWatchedNZBTestFile(t, t.TempDir(), "outside.nzb", "outside")
	link := filepath.Join(root, "linked.nzb.importing")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := AcceptClaimedNZBAt(
		root,
		link,
		watchedNZBTestDigest("outside"),
		watchedNZBTestMaxBytes,
	); err == nil {
		t.Fatal("symlink importing source unexpectedly accepted")
	}
	assertWatchedNZBTestContents(t, outside, "outside")
}

func TestRemoveAcceptedNZBAtIsExactAndNoFollow(t *testing.T) {
	root := t.TempDir()
	accepted := writeWatchedNZBTestFile(t, root, "release.nzb.accepted", "payload")
	if err := RemoveAcceptedNZBAt(root, accepted, watchedNZBTestMaxBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(accepted); !os.IsNotExist(err) {
		t.Fatalf("accepted file still exists: %v", err)
	}
	if err := RemoveAcceptedNZBAt(root, accepted, watchedNZBTestMaxBytes); err != nil {
		t.Fatalf("idempotent accepted removal = %v", err)
	}

	importing := writeWatchedNZBTestFile(t, root, "other.nzb.importing", "keep")
	if err := RemoveAcceptedNZBAt(root, importing, watchedNZBTestMaxBytes); err == nil {
		t.Fatal("importing path unexpectedly accepted for terminal removal")
	}
	assertWatchedNZBTestContents(t, importing, "keep")

	outside := writeWatchedNZBTestFile(t, t.TempDir(), "outside.nzb", "outside")
	link := filepath.Join(root, "linked.nzb.accepted")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := RemoveAcceptedNZBAt(root, link, watchedNZBTestMaxBytes); err == nil {
		t.Fatal("accepted symlink unexpectedly removed")
	}
	assertWatchedNZBTestContents(t, outside, "outside")
}

func TestCleanupAcceptedNZBsAtIsCountAndByteBounded(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		writeWatchedNZBTestFile(t, root, name+".nzb.accepted", "data")
	}
	writeWatchedNZBTestFile(t, root, "unrelated.meta", "keep")

	cleaner, err := NewAcceptedNZBCleaner(root)
	if err != nil {
		t.Fatal(err)
	}
	defer cleaner.Close()

	result, err := cleaner.Cleanup(AcceptedNZBCleanupLimits{
		MaxEntries:    8,
		MaxFiles:      2,
		MaxFileBytes:  4,
		MaxTotalBytes: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned > 8 ||
		result.Attempted != 2 ||
		result.Removed != 2 ||
		result.Failed != 0 ||
		result.BytesRemoved != 8 ||
		!result.More {
		t.Fatalf("first cleanup result = %#v", result)
	}

	result, err = cleaner.Cleanup(AcceptedNZBCleanupLimits{
		MaxEntries:    8,
		MaxFiles:      8,
		MaxFileBytes:  4,
		MaxTotalBytes: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned > 8 ||
		result.Attempted < 2 ||
		result.Removed != 2 ||
		result.BytesRemoved != 8 ||
		!result.More {
		t.Fatalf("byte-bounded cleanup result = %#v", result)
	}

	result, err = cleaner.Cleanup(AcceptedNZBCleanupLimits{
		MaxEntries:    8,
		MaxFiles:      8,
		MaxFileBytes:  4,
		MaxTotalBytes: 40,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned > 8 || result.Removed != 1 || result.More {
		t.Fatalf("final cleanup result = %#v", result)
	}
	assertWatchedNZBTestContents(t, filepath.Join(root, "unrelated.meta"), "keep")
}

func TestAcceptedNZBCleanerStreamsLargeDirectoryAcrossBoundedCalls(t *testing.T) {
	root := t.TempDir()
	for index := 0; index < 130; index++ {
		writeWatchedNZBTestFile(
			t,
			root,
			fmt.Sprintf("unrelated-%03d.meta", index),
			"keep",
		)
	}
	accepted := writeWatchedNZBTestFile(t, root, "terminal.nzb.accepted", "data")

	cleaner, err := NewAcceptedNZBCleaner(root)
	if err != nil {
		t.Fatal(err)
	}
	defer cleaner.Close()

	removed := false
	for pass := 0; pass < 20; pass++ {
		result, cleanupErr := cleaner.Cleanup(AcceptedNZBCleanupLimits{
			MaxEntries:    10,
			MaxFiles:      1,
			MaxFileBytes:  4,
			MaxTotalBytes: 4,
		})
		if cleanupErr != nil {
			t.Fatal(cleanupErr)
		}
		if result.Scanned > 10 || result.Attempted > 1 || result.BytesRemoved > 4 {
			t.Fatalf("cleanup exceeded bounds: %#v", result)
		}
		if result.Removed == 1 {
			removed = true
			break
		}
	}
	if !removed {
		t.Fatal("streaming cleaner never reached terminal tombstone")
	}
	if _, statErr := os.Lstat(accepted); !os.IsNotExist(statErr) {
		t.Fatalf("accepted tombstone still exists: %v", statErr)
	}
}

func TestCleanupAcceptedNZBsAtPreservesOversizedAndSymlinkState(t *testing.T) {
	root := t.TempDir()
	oversized := writeWatchedNZBTestFile(t, root, "oversized.nzb.accepted", "12345")
	outside := writeWatchedNZBTestFile(t, t.TempDir(), "outside.nzb", "outside")
	link := filepath.Join(root, "linked.nzb.accepted")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	result, err := CleanupAcceptedNZBsAt(root, AcceptedNZBCleanupLimits{
		MaxEntries:    10,
		MaxFiles:      10,
		MaxFileBytes:  4,
		MaxTotalBytes: 40,
	})
	if err == nil {
		t.Fatal("corrupt accepted cleanup unexpectedly succeeded")
	}
	if result.Matched != 2 || result.Attempted != 2 || result.Removed != 0 || result.Failed != 2 || !result.More {
		t.Fatalf("corrupt cleanup result = %#v", result)
	}
	assertWatchedNZBTestContents(t, oversized, "12345")
	assertWatchedNZBTestContents(t, outside, "outside")
	if _, statErr := os.Lstat(link); statErr != nil {
		t.Fatalf("accepted symlink was removed: %v", statErr)
	}
}

func TestCleanupAcceptedNZBsAtRejectsUnboundedLimits(t *testing.T) {
	root := t.TempDir()
	for _, limits := range []AcceptedNZBCleanupLimits{
		{},
		{MaxEntries: 1, MaxFiles: 1, MaxFileBytes: 0, MaxTotalBytes: 1},
		{MaxEntries: 1, MaxFiles: 1, MaxFileBytes: 2, MaxTotalBytes: 1},
		{MaxEntries: 1, MaxFiles: 2, MaxFileBytes: 1, MaxTotalBytes: 1},
	} {
		if _, err := CleanupAcceptedNZBsAt(root, limits); err == nil {
			t.Fatalf("unbounded cleanup limits %#v unexpectedly accepted", limits)
		}
	}
}

package usenet

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func testClaimLimits() ClaimNewNZBLimits {
	return ClaimNewNZBLimits{
		MaxEntries:    32,
		MaxFiles:      8,
		MaxFileBytes:  1024,
		MaxTotalBytes: 4096,
	}
}

func TestNZBClaimScannerClaimsAndSnapshotsBoundedSource(t *testing.T) {
	root := t.TempDir()
	raw := writeWatchedNZBTestFile(t, root, "release.nzb", "payload")
	scanner, err := NewNZBClaimScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()

	result, err := scanner.Scan(testClaimLimits())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Pending) != 1 {
		t.Fatalf("pending claims = %d, result %#v", len(result.Pending), result)
	}
	pending := result.Pending[0]
	if pending.Name != "release.nzb" ||
		pending.Path != filepath.Join(root, "release.nzb.importing") ||
		string(pending.Content) != "payload" ||
		pending.ContentDigest != watchedNZBTestDigest("payload") ||
		pending.Size != int64(len("payload")) {
		t.Fatalf("pending claim = %#v", pending)
	}
	if _, err := os.Lstat(raw); !os.IsNotExist(err) {
		t.Fatalf("raw watched source still exists: %v", err)
	}
	assertWatchedNZBTestContents(t, pending.Path, "payload")
	if result.Scanned > testClaimLimits().MaxEntries ||
		result.Attempted > testClaimLimits().MaxFiles ||
		result.BytesRead > testClaimLimits().MaxTotalBytes {
		t.Fatalf("claim scan exceeded limits: %#v", result)
	}
}

func TestNZBClaimScannerReturnsCrashRecoveredImportingSource(t *testing.T) {
	root := t.TempDir()
	importing := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "payload")
	scanner, err := NewNZBClaimScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()

	result, err := scanner.Scan(testClaimLimits())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Pending) != 1 || result.Pending[0].Path != importing {
		t.Fatalf("recovered pending claims = %#v", result.Pending)
	}
}

func TestNZBClaimScannerNeverClaimsCanonicalManagedSource(t *testing.T) {
	root := t.TempDir()
	internal := writeWatchedNZBTestFile(t, root, testNZBID+".nzb", "managed")
	internalImporting := writeWatchedNZBTestFile(
		t,
		root,
		otherTestNZBID+".nzb.importing",
		"managed-importing",
	)
	scanner, err := NewNZBClaimScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()

	result, err := scanner.Scan(testClaimLimits())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Pending) != 0 {
		t.Fatalf("managed source was claimed: %#v", result.Pending)
	}
	assertWatchedNZBTestContents(t, internal, "managed")
	assertWatchedNZBTestContents(t, internalImporting, "managed-importing")
	if _, err := os.Lstat(internal + ".importing"); !os.IsNotExist(err) {
		t.Fatalf("managed importing path exists: %v", err)
	}
}

func TestNZBClaimScannerFailsClosedOnUnsafeManagedMarker(t *testing.T) {
	root := t.TempDir()
	raw := writeWatchedNZBTestFile(t, root, "release.nzb", "payload")
	outside := writeWatchedNZBTestFile(t, t.TempDir(), "outside.marker", "outside")
	if err := os.Symlink(outside, raw+".processing"); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	scanner, err := NewNZBClaimScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()

	result, err := scanner.Scan(testClaimLimits())
	if err == nil || result.Failed != 1 || len(result.Pending) != 0 {
		t.Fatalf("unsafe marker result = %#v, err %v", result, err)
	}
	assertWatchedNZBTestContents(t, raw, "payload")
	assertWatchedNZBTestContents(t, outside, "outside")
}

func TestNZBClaimScannerDoesNotOverwriteExistingImportingSource(t *testing.T) {
	root := t.TempDir()
	raw := writeWatchedNZBTestFile(t, root, "release.nzb", "new")
	importing := writeWatchedNZBTestFile(t, root, "release.nzb.importing", "existing")
	scanner, err := NewNZBClaimScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()

	result, err := scanner.Scan(testClaimLimits())
	if !errors.Is(err, ErrWatchedNZBClaimConflict) {
		t.Fatalf("claim conflict error = %v", err)
	}
	if len(result.Pending) != 1 || result.Pending[0].Path != importing {
		t.Fatalf("claim conflict pending = %#v", result.Pending)
	}
	assertWatchedNZBTestContents(t, raw, "new")
	assertWatchedNZBTestContents(t, importing, "existing")
}

func TestClaimWatchedNZBInRootSyncFailureLeavesRecoverableImportingName(t *testing.T) {
	root := t.TempDir()
	raw := writeWatchedNZBTestFile(t, root, "release.nzb", "payload")
	importing := filepath.Join(root, "release.nzb.importing")
	rooted, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer rooted.Close()
	info, err := rooted.Lstat("release.nzb")
	if err != nil {
		t.Fatal(err)
	}
	syncFailure := errors.New("injected claim sync failure")
	err = claimWatchedNZBInRoot(
		rooted,
		"release.nzb",
		"release.nzb.importing",
		info,
		func(*os.Root) error { return syncFailure },
	)
	if !errors.Is(err, syncFailure) {
		t.Fatalf("claim sync failure = %v", err)
	}
	assertWatchedNZBTestContents(t, raw, "payload")
	assertWatchedNZBTestContents(t, importing, "payload")
}

func TestNZBClaimScannerStreamsLargeDirectoryAcrossBoundedCalls(t *testing.T) {
	root := t.TempDir()
	for index := 0; index < 130; index++ {
		writeWatchedNZBTestFile(t, root, fmt.Sprintf("unrelated-%03d.meta", index), "keep")
	}
	raw := writeWatchedNZBTestFile(t, root, "terminal.nzb", "data")
	scanner, err := NewNZBClaimScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()

	limits := ClaimNewNZBLimits{
		MaxEntries:    10,
		MaxFiles:      1,
		MaxFileBytes:  4,
		MaxTotalBytes: 4,
	}
	found := false
	for pass := 0; pass < 20; pass++ {
		result, scanErr := scanner.Scan(limits)
		if scanErr != nil {
			t.Fatal(scanErr)
		}
		if result.Scanned > limits.MaxEntries ||
			result.Attempted > limits.MaxFiles ||
			result.BytesRead > limits.MaxTotalBytes {
			t.Fatalf("claim scan exceeded limits: %#v", result)
		}
		if len(result.Pending) == 1 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("streaming claim scanner never reached watched source")
	}
	if _, err := os.Lstat(raw); !os.IsNotExist(err) {
		t.Fatalf("raw watched source still exists: %v", err)
	}
}

func TestNZBClaimScannerRejectsUnboundedLimits(t *testing.T) {
	scanner, err := NewNZBClaimScanner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	for _, limits := range []ClaimNewNZBLimits{
		{},
		{MaxEntries: 1, MaxFiles: 2, MaxFileBytes: 1, MaxTotalBytes: 1},
		{MaxEntries: 1, MaxFiles: 1, MaxFileBytes: 2, MaxTotalBytes: 1},
	} {
		if _, err := scanner.Scan(limits); err == nil {
			t.Fatalf("limits %#v unexpectedly accepted", limits)
		}
	}
}

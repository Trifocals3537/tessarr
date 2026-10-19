package usenet

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

const (
	testNZBID      = "11111111-1111-1111-1111-111111111111"
	otherTestNZBID = "22222222-2222-2222-2222-222222222222"
)

func newMetadataTestUsenet(t *testing.T) *Usenet {
	t.Helper()
	root := t.TempDir()
	sourceDir := filepath.Join(root, "sources")
	metaDir := filepath.Join(root, "meta")
	if err := os.MkdirAll(sourceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(metaDir, 0o700); err != nil {
		t.Fatal(err)
	}
	u := &Usenet{
		metadataDir: sourceDir,
		nzbStorage: &NZBStorage{
			metaDir: metaDir,
			logger:  zerolog.Nop(),
		},
		logger: zerolog.Nop(),
		fs:     xsync.NewMap[string, *fsEntry](),
	}
	t.Cleanup(func() {
		if err := u.Close(); err != nil {
			t.Errorf("close metadata test usenet: %v", err)
		}
	})
	return u
}

func TestCanonicalNZBIDRejectsTraversalAndNonCanonicalForms(t *testing.T) {
	for _, id := range []string{
		"../escape",
		`..\escape`,
		"11111111111111111111111111111111",
		"11111111-1111-1111-1111-111111111111/extra",
		"11111111-1111-1111-1111-111111111111\n",
	} {
		if _, err := canonicalNZBID(id); err == nil {
			t.Fatalf("canonicalNZBID(%q) unexpectedly succeeded", id)
		}
	}
	if got, err := canonicalNZBID(testNZBID); err != nil || got != testNZBID {
		t.Fatalf("canonicalNZBID(valid) = %q, %v", got, err)
	}
}

func TestReadMetadataFileRejectsOversizedSparseFile(t *testing.T) {
	u := newMetadataTestUsenet(t)
	path, err := metadataFilePath(u.metadataDir, testNZBID, nzbSourceSuffix)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxNZBMetadataFileBytes + 1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := readMetadataFile(u.metadataDir, path); err == nil {
		t.Fatal("readMetadataFile accepted an oversized metadata file")
	}
}

func TestStageNZBUsesExactIDBoundPath(t *testing.T) {
	u := newMetadataTestUsenet(t)
	path, err := u.StageNZB(testNZBID, []byte("queued"))
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(u.metadataDir, testNZBID+".queued")
	if path != want {
		t.Fatalf("StageNZB path = %q, want %q", path, want)
	}
	data, err := u.ReadStagedNZB(testNZBID, path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "queued" {
		t.Fatalf("staged contents = %q", data)
	}
}

func TestRemoveStagedNZBRejectsCrossEntryPath(t *testing.T) {
	u := newMetadataTestUsenet(t)
	otherPath, err := u.StageNZB(otherTestNZBID, []byte("other"))
	if err != nil {
		t.Fatal(err)
	}
	if err := u.RemoveStagedNZB(testNZBID, otherPath); err == nil {
		t.Fatal("RemoveStagedNZB accepted another entry's path")
	}
	if data, err := os.ReadFile(otherPath); err != nil || string(data) != "other" {
		t.Fatalf("other staged source was changed: data=%q err=%v", data, err)
	}
}

func TestRemoveStagedNZBAtRejectsCrossEntryPath(t *testing.T) {
	u := newMetadataTestUsenet(t)
	otherPath, err := u.StageNZB(otherTestNZBID, []byte("other"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateStagedNZBAt(u.metadataDir, testNZBID, otherPath); err == nil {
		t.Fatal("ValidateStagedNZBAt accepted another entry's path")
	}
	if err := RemoveStagedNZBAt(u.metadataDir, testNZBID, otherPath); err == nil {
		t.Fatal("RemoveStagedNZBAt accepted another entry's path")
	}
	if data, err := os.ReadFile(otherPath); err != nil || string(data) != "other" {
		t.Fatalf("other staged source was changed: data=%q err=%v", data, err)
	}
}

func TestNZBStorageRejectsTraversalIDWithoutTouchingOutsideFile(t *testing.T) {
	u := newMetadataTestUsenet(t)
	outside := filepath.Join(filepath.Dir(u.nzbStorage.metaDir), "escape.meta")
	if err := os.WriteFile(outside, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := u.nzbStorage.DeleteNZB("../escape"); err == nil {
		t.Fatal("DeleteNZB accepted a traversal ID")
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "keep" {
		t.Fatalf("outside file was changed: data=%q err=%v", data, err)
	}
}

func TestDeleteRejectsTamperedPersistedPathAndPreservesRecord(t *testing.T) {
	u := newMetadataTestUsenet(t)
	outside := filepath.Join(filepath.Dir(u.metadataDir), "outside.nzb")
	if err := os.WriteFile(outside, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	nzb := &storage.NZB{ID: testNZBID, Name: "release", Path: outside}
	if err := u.nzbStorage.AddNZB(nzb); err != nil {
		t.Fatal(err)
	}

	if err := u.Delete(testNZBID); err == nil {
		t.Fatal("Delete accepted a tampered persisted path")
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "keep" {
		t.Fatalf("outside file was changed: data=%q err=%v", data, err)
	}
	if !u.nzbStorage.Exists(testNZBID) {
		t.Fatal("metadata record was deleted after unsafe path rejection")
	}
}

func TestDeleteTreatsConfirmedMissingMetadataAsSuccess(t *testing.T) {
	u := newMetadataTestUsenet(t)
	if err := u.Delete(testNZBID); err != nil {
		t.Fatalf("Delete(missing) = %v, want nil", err)
	}
}

func TestDeletePreservesMalformedMetadata(t *testing.T) {
	u := newMetadataTestUsenet(t)
	metaPath, err := u.nzbStorage.metaFilePath(testNZBID)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeMetadataFile(u.nzbStorage.metaDir, metaPath, []byte("not valid NZB metadata"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := u.Delete(testNZBID); err == nil {
		t.Fatal("Delete accepted malformed metadata")
	}
	if data, err := os.ReadFile(metaPath); err != nil || string(data) != "not valid NZB metadata" {
		t.Fatalf("malformed metadata changed: data=%q err=%v", data, err)
	}
}

func TestDeleteRemovesOnlyExactIDBoundArtifacts(t *testing.T) {
	u := newMetadataTestUsenet(t)
	source, err := u.saveNZBFile(testNZBID, []byte("source"))
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []metadataFileSuffix{nzbProcessingSuffix, nzbProcessedSuffix, nzbFailedSuffix} {
		path, err := metadataFilePath(u.metadataDir, testNZBID, suffix)
		if err != nil {
			t.Fatal(err)
		}
		if err := writeMetadataFile(u.metadataDir, path, []byte("marker"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := u.nzbStorage.AddNZB(&storage.NZB{ID: testNZBID, Name: "release", Path: source}); err != nil {
		t.Fatal(err)
	}

	if err := u.Delete(testNZBID); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []metadataFileSuffix{nzbSourceSuffix, nzbProcessingSuffix, nzbProcessedSuffix, nzbFailedSuffix} {
		path, err := metadataFilePath(u.metadataDir, testNZBID, suffix)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("artifact %q still exists: %v", path, err)
		}
	}
	if u.nzbStorage.Exists(testNZBID) {
		t.Fatal("metadata record still exists")
	}
}

func TestNZBStorageAtomicallyReplacesExistingRecord(t *testing.T) {
	u := newMetadataTestUsenet(t)
	first := &storage.NZB{ID: testNZBID, Name: "first"}
	if err := u.nzbStorage.AddNZB(first); err != nil {
		t.Fatal(err)
	}
	second := &storage.NZB{ID: testNZBID, Name: "second"}
	if err := u.nzbStorage.AddNZB(second); err != nil {
		t.Fatal(err)
	}
	got, err := u.nzbStorage.GetNZBHeader(testNZBID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != second.Name {
		t.Fatalf("stored name = %q, want %q", got.Name, second.Name)
	}
}

func TestMarkAsFailedWithEmptyPathNeverTouchesRelativeProcessingFile(t *testing.T) {
	u := newMetadataTestUsenet(t)
	workingDir := t.TempDir()
	t.Chdir(workingDir)
	relativeMarker := filepath.Join(workingDir, ".processing")
	if err := os.WriteFile(relativeMarker, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	nzb := &storage.NZB{ID: testNZBID, Name: "release"}
	if err := u.markAsFailed(nzb, errors.New("failed")); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(relativeMarker); err != nil || string(data) != "keep" {
		t.Fatalf("relative .processing file was changed: data=%q err=%v", data, err)
	}
}

func TestMarkAsCompletedRejectsCrossEntrySource(t *testing.T) {
	u := newMetadataTestUsenet(t)
	otherSource, err := u.saveNZBFile(otherTestNZBID, []byte("keep"))
	if err != nil {
		t.Fatal(err)
	}
	nzb := &storage.NZB{ID: testNZBID, Name: "release", Path: otherSource}
	if err := u.markAsCompleted(nzb); err == nil {
		t.Fatal("markAsCompleted accepted another entry's source path")
	}
	if data, err := os.ReadFile(otherSource); err != nil || string(data) != "keep" {
		t.Fatalf("other source was changed: data=%q err=%v", data, err)
	}
}

func TestClaimNewNZBsIgnoresSymlinkSources(t *testing.T) {
	u := newMetadataTestUsenet(t)
	outside := filepath.Join(filepath.Dir(u.metadataDir), "outside.xml")
	if err := os.WriteFile(outside, []byte("<nzb/>"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(u.metadataDir, "linked.nzb")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}

	pending, err := u.ClaimNewNZBs()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("ClaimNewNZBs returned %d symlink sources", len(pending))
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "<nzb/>" {
		t.Fatalf("outside target was changed: data=%q err=%v", data, err)
	}
}

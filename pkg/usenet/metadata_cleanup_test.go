package usenet

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestRemoveStagedNZBIsIdempotent(t *testing.T) {
	u := newMetadataTestUsenet(t)
	path, err := u.StageNZB(testNZBID, []byte("queued"))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := u.RemoveStagedNZB(testNZBID, path); err != nil {
			t.Fatalf("remove staged source: %v", err)
		}
	}
}

func TestDeleteNZBToleratesAbsentSourceArtifacts(t *testing.T) {
	suffixes := []metadataFileSuffix{nzbSourceSuffix, nzbProcessingSuffix, nzbProcessedSuffix, nzbFailedSuffix}
	for present := 0; present < 1<<len(suffixes); present++ {
		t.Run(fmt.Sprintf("present_%04b", present), func(t *testing.T) {
			u := newMetadataTestUsenet(t)
			source, err := metadataFilePath(u.metadataDir, testNZBID, nzbSourceSuffix)
			if err != nil {
				t.Fatal(err)
			}
			for index, suffix := range suffixes {
				if present&(1<<index) == 0 {
					continue
				}
				path, err := metadataFilePath(u.metadataDir, testNZBID, suffix)
				if err != nil {
					t.Fatal(err)
				}
				if err := writeMetadataFile(u.metadataDir, path, []byte("synthetic"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := u.nzbStorage.AddNZB(&storage.NZB{ID: testNZBID, Name: "release", Path: source}); err != nil {
				t.Fatal(err)
			}
			otherSource, err := u.saveNZBFile(otherTestNZBID, []byte("keep"))
			if err != nil {
				t.Fatal(err)
			}
			if err := u.nzbStorage.AddNZB(&storage.NZB{ID: otherTestNZBID, Name: "other", Path: otherSource}); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := u.Delete(testNZBID); err != nil {
					t.Fatalf("delete with absent source artifacts: %v", err)
				}
			}
			if err := u.nzbStorage.DeleteNZB(testNZBID); err != nil {
				t.Fatalf("repeat direct metadata deletion: %v", err)
			}
			if u.nzbStorage.metaCount != 1 {
				t.Fatalf("repeated deletion changed other metadata count: %d", u.nzbStorage.metaCount)
			}
			if u.nzbStorage.Exists(testNZBID) {
				t.Fatal("explicit deletion left metadata behind")
			}
			for _, suffix := range suffixes {
				if _, err := os.Lstat(filepath.Join(u.metadataDir, testNZBID+string(suffix))); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("artifact %q remains: %v", suffix, err)
				}
			}
			if data, err := os.ReadFile(otherSource); err != nil || string(data) != "keep" || !u.nzbStorage.Exists(otherTestNZBID) {
				t.Fatalf("another NZB was affected: data=%q err=%v", data, err)
			}
		})
	}
}

func TestCompletedNZBCleanupResumesWithAbsentArtifacts(t *testing.T) {
	for present := 0; present < 4; present++ {
		t.Run(fmt.Sprintf("present_%02b", present), func(t *testing.T) {
			u := newMetadataTestUsenet(t)
			source, err := metadataFilePath(u.metadataDir, testNZBID, nzbSourceSuffix)
			if err != nil {
				t.Fatal(err)
			}
			nzb := &storage.NZB{
				ID: testNZBID, Name: "release", Path: source, Status: NZBStatusCompleted, TotalSize: 1234,
				Files: []storage.NZBFile{{
					NzbID: testNZBID, Name: "video.mkv", Size: 1234, FileType: storage.NZBFileTypeMedia,
					Segments: []storage.NZBSegment{{Number: 1, MessageID: "synthetic@example.invalid", Bytes: 1234, EndOffset: 1234}},
				}},
			}
			if err := u.nzbStorage.AddNZB(nzb); err != nil {
				t.Fatal(err)
			}
			for index, suffix := range []metadataFileSuffix{nzbSourceSuffix, nzbProcessingSuffix} {
				if present&(1<<index) == 0 {
					continue
				}
				if err := writeMetadataFile(u.metadataDir, filepath.Join(u.metadataDir, testNZBID+string(suffix)), []byte("synthetic"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			// Reload the persisted completion with its stale path, as after a crash.
			reloaded, err := u.nzbStorage.GetNZB(testNZBID)
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := u.markAsCompleted(reloaded); err != nil {
					t.Fatalf("retry completed cleanup: %v", err)
				}
			}
			stored, err := u.nzbStorage.GetNZB(testNZBID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.Status != NZBStatusCompleted || stored.Path != "" || stored.TotalSize != 1234 {
				t.Fatalf("completed metadata changed or retained stale path: %+v", stored)
			}
			if !reflect.DeepEqual(stored.Files, nzb.Files) {
				t.Fatalf("completion changed the stream segment map: %+v", stored.Files)
			}
			for _, suffix := range []metadataFileSuffix{nzbSourceSuffix, nzbProcessingSuffix} {
				if _, err := os.Lstat(filepath.Join(u.metadataDir, testNZBID+string(suffix))); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("completed artifact %q remains: %v", suffix, err)
				}
			}
			pending, err := u.ClaimNewNZBs()
			if err != nil || len(pending) != 0 {
				t.Fatalf("completed NZB was rediscovered: pending=%v err=%v", pending, err)
			}
		})
	}
}

func TestMarkAsFailedCleanupIsIdempotent(t *testing.T) {
	u := newMetadataTestUsenet(t)
	source, err := u.saveNZBFile(testNZBID, []byte("synthetic"))
	if err != nil {
		t.Fatal(err)
	}
	nzb := &storage.NZB{ID: testNZBID, Name: "release", Path: source}
	for range 2 {
		if err := u.markAsFailed(nzb, errors.New("parse failed")); err != nil {
			t.Fatalf("failed-job cleanup: %v", err)
		}
	}
	stored, err := u.nzbStorage.GetNZBHeader(testNZBID)
	if err != nil || stored == nil || stored.Status != NZBStatusFailed || stored.FailMessage != "parse failed" {
		t.Fatalf("failed-job metadata not retained: stored=%v err=%v", stored, err)
	}
}

func TestFinishMetadataRemovalPreservesIndependentFailures(t *testing.T) {
	missing := &os.PathError{Op: "remove", Path: "source.nzb", Err: os.ErrNotExist}
	syncFailure := errors.New("directory sync failed")
	closeFailure := errors.New("root close failed")
	for _, test := range []struct {
		name         string
		operationErr error
		syncErr      error
		closeErr     error
		wantSync     bool
		wantErrors   []error
	}{
		{name: "removed", wantSync: true},
		{name: "already absent", operationErr: missing, wantSync: true},
		{name: "missing and sync failure", operationErr: missing, syncErr: syncFailure, wantSync: true, wantErrors: []error{syncFailure}},
		{name: "missing and close failure", operationErr: missing, closeErr: closeFailure, wantSync: true, wantErrors: []error{closeFailure}},
		{name: "missing and both failures", operationErr: missing, syncErr: syncFailure, closeErr: closeFailure, wantSync: true, wantErrors: []error{syncFailure, closeFailure}},
		{name: "sync reports missing", syncErr: missing, wantSync: true, wantErrors: []error{os.ErrNotExist}},
		{name: "close reports missing", closeErr: missing, wantSync: true, wantErrors: []error{os.ErrNotExist}},
		{name: "permission failure", operationErr: os.ErrPermission, closeErr: closeFailure, wantErrors: []error{os.ErrPermission, closeFailure}},
		{name: "joined operation failure", operationErr: errors.Join(missing, os.ErrPermission), wantErrors: []error{os.ErrNotExist, os.ErrPermission}},
	} {
		t.Run(test.name, func(t *testing.T) {
			synced, closed := false, false
			err := finishMetadataRemoval(test.operationErr, func() error {
				if closed {
					t.Fatal("synced root after closing it")
				}
				synced = true
				return test.syncErr
			}, func() error {
				closed = true
				return test.closeErr
			})
			if synced != test.wantSync || !closed {
				t.Fatalf("synced=%v closed=%v, want sync=%v and closed", synced, closed, test.wantSync)
			}
			if len(test.wantErrors) == 0 && err != nil {
				t.Fatalf("cleanup failed: %v", err)
			}
			for _, want := range test.wantErrors {
				if !errors.Is(err, want) {
					t.Fatalf("cleanup error %v lost %v", err, want)
				}
			}
		})
	}
}

func TestRemoveMetadataFileDoesNotIgnoreUnavailableRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "absent-root")
	if err := removeMetadataFileIfExists(root, filepath.Join(root, testNZBID+".queued")); err == nil {
		t.Fatal("unavailable root was treated as an absent leaf")
	}
}

func TestCompletedCleanupPreservesMetadataOnUnsafeMarker(t *testing.T) {
	u := newMetadataTestUsenet(t)
	source, err := u.saveNZBFile(testNZBID, []byte("synthetic"))
	if err != nil {
		t.Fatal(err)
	}
	marker := source + ".processing"
	if err := os.Mkdir(marker, 0o700); err != nil {
		t.Fatal(err)
	}
	nzb := &storage.NZB{ID: testNZBID, Name: "release", Path: source}
	if err := u.markAsCompleted(nzb); err == nil {
		t.Fatal("completion accepted a directory instead of a marker")
	}
	stored, err := u.nzbStorage.GetNZBHeader(testNZBID)
	if err != nil || stored == nil || stored.Status != NZBStatusCompleted || stored.Path != source {
		t.Fatalf("completion lost recoverable metadata: stored=%v err=%v", stored, err)
	}
	if info, err := os.Stat(marker); err != nil || !info.IsDir() {
		t.Fatalf("unsafe marker was changed: %v", err)
	}
}

func TestRemoveStagedNZBConcurrentCleanup(t *testing.T) {
	u := newMetadataTestUsenet(t)
	path, err := u.StageNZB(testNZBID, []byte("queued"))
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 8)
	var workers sync.WaitGroup
	for range cap(errs) {
		workers.Go(func() {
			<-start
			errs <- u.RemoveStagedNZB(testNZBID, path)
		})
	}
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent cleanup: %v", err)
		}
	}
}

func TestRemoveStagedNZBPreservesSymlinkTarget(t *testing.T) {
	u := newMetadataTestUsenet(t)
	outside := filepath.Join(t.TempDir(), "keep.nzb")
	if err := os.WriteFile(outside, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(u.metadataDir, testNZBID+".queued")
	if err := os.Symlink(outside, path); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if err := u.RemoveStagedNZB(testNZBID, path); err == nil {
		t.Fatal("accepted symlink source")
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "keep" {
		t.Fatalf("symlink target changed: data=%q err=%v", data, err)
	}
	if _, err := os.Readlink(path); err != nil {
		t.Fatalf("rejected symlink was removed: %v", err)
	}
}

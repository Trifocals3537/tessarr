package manager

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

const (
	legacyTestID       = "11111111-1111-4111-8111-111111111111"
	legacyTestName     = "Release.nzb"
	legacyTestCategory = "sonarr"
	legacyTestFile     = "episode.mkv"
)

type legacyAdoptionFixture struct {
	root      string
	mountRoot string
	entryPath string
	entry     *storage.Entry
	header    *storage.NZB
	adopter   *legacyUsenetAdopter
	manual    []*storage.Entry
}

func TestRequireSingleLegacyLinkUsesOpenedArtifact(t *testing.T) {
	rootPath := t.TempDir()
	artifactPath := filepath.Join(rootPath, "artifact.bin")
	linkedPath := filepath.Join(rootPath, "artifact-link.bin")
	if err := os.WriteFile(artifactPath, []byte("legacy"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(artifactPath, linkedPath); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	rooted, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rooted.Close()
	file, _, err := openStableLegacyRegularFile(rooted, "artifact.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	movedPath := filepath.Join(rootPath, "moved-artifact.bin")
	if err := os.Rename(artifactPath, movedPath); err != nil {
		t.Fatalf("rename opened artifact: %v", err)
	}
	if err := os.WriteFile(artifactPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}

	err = requireSingleLegacyLink(file)
	if err == nil || !strings.Contains(err.Error(), "2 hard links") {
		t.Fatalf("requireSingleLegacyLink() error = %v, want pinned artifact hard-link rejection", err)
	}
}

func newLegacyAdoptionFixture(t *testing.T, action config.DownloadAction) *legacyAdoptionFixture {
	t.Helper()
	root := t.TempDir()
	mountRoot := t.TempDir()
	savePath, entryPath, err := usenetEntryPaths(root, legacyTestCategory, legacyTestName)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(entryPath, 0o700); err != nil {
		t.Fatal(err)
	}
	entry := &storage.Entry{
		Protocol:         config.ProtocolNZB,
		InfoHash:         legacyTestID,
		Name:             legacyTestName,
		OriginalFilename: legacyTestName,
		Size:             100,
		Bytes:            100,
		ActiveProvider:   "usenet",
		Providers: map[string]*storage.ProviderEntry{
			"usenet": {
				Provider: "usenet",
				ID:       legacyTestID,
				Files: map[string]*storage.ProviderFile{
					legacyTestFile: {
						Id:   legacyTestFile,
						Path: legacyTestFile,
						Link: legacyTestFile,
					},
				},
			},
		},
		Files: map[string]*storage.File{
			legacyTestFile: {
				Name:     legacyTestFile,
				Size:     10,
				InfoHash: legacyTestID,
			},
		},
		Category:    legacyTestCategory,
		SavePath:    savePath,
		ContentPath: entryPath,
		Action:      action,
	}
	header := &storage.NZB{
		ID:        legacyTestID,
		Name:      legacyTestName,
		TotalSize: 100,
		Category:  legacyTestCategory,
		Files: []storage.NZBFile{{
			NzbID: legacyTestID,
			Name:  legacyTestFile,
			Size:  10,
		}},
	}
	fixture := &legacyAdoptionFixture{
		root:      root,
		mountRoot: mountRoot,
		entryPath: entryPath,
		entry:     entry,
		header:    header,
	}
	fixture.adopter = &legacyUsenetAdopter{
		downloadRoot: root,
		mountRoot:    mountRoot,
		strmURL:      "http://127.0.0.1:8282",
		folderNaming: config.WebDavUseFileNameNoExt,
		header: func(string) (*storage.NZB, error) {
			return fixture.header, nil
		},
		markManual: func(entry *storage.Entry) error {
			fixture.manual = append(fixture.manual, entry)
			return nil
		},
	}
	return fixture
}

func TestLegacyUsenetAdoptionAcceptsActionArtifacts(t *testing.T) {
	tests := []struct {
		name    string
		action  config.DownloadAction
		prepare func(*testing.T, *legacyAdoptionFixture)
	}{
		{
			name:   "empty none",
			action: config.DownloadActionNone,
		},
		{
			name:   "partial download",
			action: config.DownloadActionDownload,
			prepare: func(t *testing.T, fixture *legacyAdoptionFixture) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(fixture.entryPath, legacyTestFile), []byte("12345"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:   "exact strm",
			action: config.DownloadActionStrm,
			prepare: func(t *testing.T, fixture *legacyAdoptionFixture) {
				t.Helper()
				streamURL, err := url.JoinPath(
					"http://127.0.0.1:8282",
					"webdav",
					"stream",
					EntryAllFolder,
					url.PathEscape(legacyTestName),
					url.PathEscape(legacyTestFile),
				)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(fixture.entryPath, legacyTestFile+".strm"), []byte(streamURL), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:   "safe symlink",
			action: config.DownloadActionSymlink,
			prepare: func(t *testing.T, fixture *legacyAdoptionFixture) {
				t.Helper()
				target := filepath.Join(fixture.mountRoot, EntryAllFolder, legacyTestName, legacyTestFile)
				if err := os.Symlink(target, filepath.Join(fixture.entryPath, legacyTestFile)); err != nil {
					t.Skipf("symlink creation unavailable: %v", err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLegacyAdoptionFixture(t, test.action)
			if test.prepare != nil {
				test.prepare(t, fixture)
			}
			if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err != nil {
				t.Fatal(err)
			}
			if len(fixture.manual) != 0 {
				t.Fatalf("safe legacy directory was marked for manual review: %v", fixture.entry.LastError)
			}
			assertManagerFileContents(t, filepath.Join(fixture.entryPath, usenetOwnerMarkerName), legacyTestID+"\n")
			assertManagerFileContents(
				t,
				filepath.Join(fixture.root, usenetLegacyAdoptionCheckpointName),
				usenetLegacyAdoptionCheckpointData,
			)
		})
	}
}

func TestLegacyUsenetAdoptionRejectsAmbiguousArtifactsWithoutMutation(t *testing.T) {
	tests := []struct {
		name    string
		action  config.DownloadAction
		prepare func(*testing.T, *legacyAdoptionFixture)
	}{
		{
			name:   "extra file",
			action: config.DownloadActionNone,
			prepare: func(t *testing.T, fixture *legacyAdoptionFixture) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(fixture.entryPath, "extra.txt"), []byte("extra"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:   "portable file alias",
			action: config.DownloadActionDownload,
			prepare: func(t *testing.T, fixture *legacyAdoptionFixture) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(fixture.entryPath, strings.ToUpper(legacyTestFile)), []byte("123"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:   "nested directory",
			action: config.DownloadActionDownload,
			prepare: func(t *testing.T, fixture *legacyAdoptionFixture) {
				t.Helper()
				if err := os.Mkdir(filepath.Join(fixture.entryPath, legacyTestFile), 0o700); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:   "wrong strm contents",
			action: config.DownloadActionStrm,
			prepare: func(t *testing.T, fixture *legacyAdoptionFixture) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(fixture.entryPath, legacyTestFile+".strm"), []byte("http://attacker.invalid"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:   "unsafe symlink target",
			action: config.DownloadActionSymlink,
			prepare: func(t *testing.T, fixture *legacyAdoptionFixture) {
				t.Helper()
				if err := os.Symlink(t.TempDir(), filepath.Join(fixture.entryPath, legacyTestFile)); err != nil {
					t.Skipf("symlink creation unavailable: %v", err)
				}
			},
		},
		{
			name:   "completed short download",
			action: config.DownloadActionDownload,
			prepare: func(t *testing.T, fixture *legacyAdoptionFixture) {
				t.Helper()
				fixture.entry.IsComplete = true
				if err := os.WriteFile(filepath.Join(fixture.entryPath, legacyTestFile), []byte("short"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:   "hard linked download",
			action: config.DownloadActionDownload,
			prepare: func(t *testing.T, fixture *legacyAdoptionFixture) {
				t.Helper()
				source := filepath.Join(fixture.root, "outside-sentinel")
				if err := os.WriteFile(source, []byte("12345"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(source, filepath.Join(fixture.entryPath, legacyTestFile)); err != nil {
					t.Skipf("hard links unavailable: %v", err)
				}
			},
		},
		{
			name:   "special file",
			action: config.DownloadActionDownload,
			prepare: func(t *testing.T, fixture *legacyAdoptionFixture) {
				t.Helper()
				if err := createLegacySpecialArtifact(filepath.Join(fixture.entryPath, legacyTestFile)); err != nil {
					t.Skipf("special-file fixture unavailable: %v", err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLegacyAdoptionFixture(t, test.action)
			test.prepare(t, fixture)
			if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err != nil {
				t.Fatalf("durably recorded manual-review outcome should complete migration: %v", err)
			}
			if len(fixture.manual) != 1 {
				t.Fatalf("manual-review updates = %d, want 1", len(fixture.manual))
			}
			if fixture.entry.State != storage.EntryStateError ||
				!strings.Contains(fixture.entry.LastError, ErrLegacyUsenetManualReview.Error()) {
				t.Fatalf("entry was not placed in manual-review error state: %+v", fixture.entry)
			}
			if _, err := os.Lstat(filepath.Join(fixture.entryPath, usenetOwnerMarkerName)); !os.IsNotExist(err) {
				t.Fatalf("ambiguous legacy directory gained an owner marker: %v", err)
			}
			assertManagerFileContents(
				t,
				filepath.Join(fixture.root, usenetLegacyAdoptionCheckpointName),
				usenetLegacyAdoptionCheckpointData,
			)
		})
	}
}

func TestLegacyUsenetAdoptionRejectsMetadataMismatches(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*legacyAdoptionFixture)
	}{
		{
			name: "header id",
			mutate: func(fixture *legacyAdoptionFixture) {
				fixture.header.ID = "22222222-2222-4222-8222-222222222222"
			},
		},
		{
			name: "header name",
			mutate: func(fixture *legacyAdoptionFixture) {
				fixture.header.Name = "Other.nzb"
			},
		},
		{
			name: "header category",
			mutate: func(fixture *legacyAdoptionFixture) {
				fixture.header.Category = "radarr"
			},
		},
		{
			name: "header size",
			mutate: func(fixture *legacyAdoptionFixture) {
				fixture.header.TotalSize++
			},
		},
		{
			name: "queue files",
			mutate: func(fixture *legacyAdoptionFixture) {
				delete(fixture.entry.Files, legacyTestFile)
			},
		},
		{
			name: "provider id",
			mutate: func(fixture *legacyAdoptionFixture) {
				fixture.entry.Providers["usenet"].ID = "22222222-2222-4222-8222-222222222222"
			},
		},
		{
			name: "provider path",
			mutate: func(fixture *legacyAdoptionFixture) {
				fixture.entry.Providers["usenet"].Files[legacyTestFile].Path = "../escape"
			},
		},
		{
			name: "persisted content path",
			mutate: func(fixture *legacyAdoptionFixture) {
				fixture.entry.ContentPath = t.TempDir()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLegacyAdoptionFixture(t, config.DownloadActionNone)
			test.mutate(fixture)
			if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err != nil {
				t.Fatal(err)
			}
			if len(fixture.manual) != 1 {
				t.Fatalf("manual-review updates = %d, want 1", len(fixture.manual))
			}
			if _, err := os.Lstat(filepath.Join(fixture.entryPath, usenetOwnerMarkerName)); !os.IsNotExist(err) {
				t.Fatalf("metadata mismatch gained an owner marker: %v", err)
			}
		})
	}
}

func TestLegacyUsenetAdoptionMissingDirectoryIsSkipped(t *testing.T) {
	fixture := newLegacyAdoptionFixture(t, config.DownloadActionNone)
	if err := os.Remove(fixture.entryPath); err != nil {
		t.Fatal(err)
	}
	headerCalls := 0
	fixture.adopter.header = func(string) (*storage.NZB, error) {
		headerCalls++
		return nil, errors.New("must not be called")
	}
	if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err != nil {
		t.Fatal(err)
	}
	if headerCalls != 0 || len(fixture.manual) != 0 {
		t.Fatalf("absent directory was classified: header calls %d, manual %d", headerCalls, len(fixture.manual))
	}
	if _, err := os.Lstat(fixture.entryPath); !os.IsNotExist(err) {
		t.Fatalf("absent release path was mutated: %v", err)
	}
}

func TestLegacyUsenetAdoptionRejectsSymlinkedCategoryWithoutMutation(t *testing.T) {
	fixture := newLegacyAdoptionFixture(t, config.DownloadActionNone)
	categoryPath := filepath.Dir(fixture.entryPath)
	externalCategory := t.TempDir()
	externalRelease := filepath.Join(externalCategory, filepath.Base(fixture.entryPath))
	if err := os.Rename(fixture.entryPath, externalRelease); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(categoryPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(externalCategory, categoryPath); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}

	if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err != nil {
		t.Fatal(err)
	}
	if len(fixture.manual) != 1 {
		t.Fatalf("manual-review updates = %d, want 1", len(fixture.manual))
	}
	if _, err := os.Lstat(filepath.Join(externalRelease, usenetOwnerMarkerName)); !os.IsNotExist(err) {
		t.Fatalf("release reached through symlinked category gained a marker: %v", err)
	}
}

func TestLegacyUsenetAdoptionPortableReleaseAliasRequiresManualReview(t *testing.T) {
	fixture := newLegacyAdoptionFixture(t, config.DownloadActionNone)
	alias := filepath.Join(filepath.Dir(fixture.entryPath), strings.ToLower(filepath.Base(fixture.entryPath)))
	if alias == fixture.entryPath {
		t.Skip("fixture filesystem/path does not provide a distinct portable alias")
	}
	if err := os.Rename(fixture.entryPath, alias); err != nil {
		t.Fatal(err)
	}
	if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err != nil {
		t.Fatal(err)
	}
	if len(fixture.manual) != 1 {
		t.Fatalf("manual-review updates = %d, want 1", len(fixture.manual))
	}
	if _, err := os.Lstat(filepath.Join(alias, usenetOwnerMarkerName)); !os.IsNotExist(err) {
		t.Fatalf("portable alias directory gained a marker: %v", err)
	}
}

func TestLegacyUsenetAdoptionRecoversMarkerWithoutCheckpoint(t *testing.T) {
	fixture := newLegacyAdoptionFixture(t, config.DownloadActionNone)
	rooted, err := os.OpenRoot(fixture.entryPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeDurableExclusiveUsenetFile(rooted, usenetOwnerMarkerName, legacyTestID+"\n", 0o600); err != nil {
		_ = rooted.Close()
		t.Fatal(err)
	}
	if err := rooted.Close(); err != nil {
		t.Fatal(err)
	}
	headerCalls := 0
	fixture.adopter.header = func(string) (*storage.NZB, error) {
		headerCalls++
		return fixture.header, nil
	}

	if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err != nil {
		t.Fatal(err)
	}
	if headerCalls != 1 || len(fixture.manual) != 0 {
		t.Fatalf("same-owner marker was not recovered idempotently: calls %d, manual %d", headerCalls, len(fixture.manual))
	}
	assertManagerFileContents(
		t,
		filepath.Join(fixture.root, usenetLegacyAdoptionCheckpointName),
		usenetLegacyAdoptionCheckpointData,
	)
}

func TestLegacyUsenetAdoptionRecognizesPredecessorCheckpoint(t *testing.T) {
	rootPath := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(rootPath, legacyUsenetAdoptionCheckpointName),
		[]byte(usenetLegacyAdoptionCheckpointData),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	rooted, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rooted.Close()
	exists, err := legacyUsenetCheckpointExists(rooted)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("predecessor adoption checkpoint was not recognized")
	}
}

func TestLegacyUsenetAdoptionPreexistingMarkerIsRevalidated(t *testing.T) {
	fixture := newLegacyAdoptionFixture(t, config.DownloadActionNone)
	rooted, err := os.OpenRoot(fixture.entryPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeDurableExclusiveUsenetFile(rooted, usenetOwnerMarkerName, legacyTestID+"\n", 0o600); err != nil {
		_ = rooted.Close()
		t.Fatal(err)
	}
	if err := rooted.Close(); err != nil {
		t.Fatal(err)
	}
	tampered := filepath.Join(fixture.entryPath, "unexpected")
	if err := os.WriteFile(tampered, []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err != nil {
		t.Fatal(err)
	}
	if len(fixture.manual) != 1 {
		t.Fatalf("manual-review updates = %d, want 1", len(fixture.manual))
	}
	assertManagerFileContents(t, filepath.Join(fixture.entryPath, usenetOwnerMarkerName), legacyTestID+"\n")
	assertManagerFileContents(t, tampered, "tampered")
}

func TestLegacyUsenetAdoptionRejectsHardLinkedOwnerMarker(t *testing.T) {
	fixture := newLegacyAdoptionFixture(t, config.DownloadActionNone)
	rooted, err := os.OpenRoot(fixture.entryPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeDurableExclusiveUsenetFile(rooted, usenetOwnerMarkerName, legacyTestID+"\n", 0o600); err != nil {
		_ = rooted.Close()
		t.Fatal(err)
	}
	if err := rooted.Close(); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(fixture.entryPath, usenetOwnerMarkerName)
	hardLink := filepath.Join(fixture.root, "owner-marker-hardlink")
	if err := os.Link(marker, hardLink); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}

	if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err != nil {
		t.Fatal(err)
	}
	if len(fixture.manual) != 1 {
		t.Fatalf("manual-review updates = %d, want 1", len(fixture.manual))
	}
	assertManagerFileContents(t, marker, legacyTestID+"\n")
	assertManagerFileContents(t, hardLink, legacyTestID+"\n")
}

func TestLegacyUsenetAdoptionMalformedEntryRequiresManualReview(t *testing.T) {
	fixture := newLegacyAdoptionFixture(t, config.DownloadActionNone)
	fixture.entry.Name = "../malformed"
	if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err != nil {
		t.Fatal(err)
	}
	if len(fixture.manual) != 1 {
		t.Fatalf("manual-review updates = %d, want 1", len(fixture.manual))
	}
	if _, err := os.Lstat(filepath.Join(fixture.entryPath, usenetOwnerMarkerName)); !os.IsNotExist(err) {
		t.Fatalf("malformed entry mutated the legacy directory: %v", err)
	}
}

func TestLegacyUsenetAdoptionCheckpointMakesRerunNoOp(t *testing.T) {
	fixture := newLegacyAdoptionFixture(t, config.DownloadActionNone)
	if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.entryPath, "later-user-file"), []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	headerCalls := 0
	fixture.adopter.header = func(string) (*storage.NZB, error) {
		headerCalls++
		return nil, errors.New("must not be called after checkpoint")
	}
	if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err != nil {
		t.Fatal(err)
	}
	if headerCalls != 0 || len(fixture.manual) != 0 {
		t.Fatalf("checkpoint rerun performed work: header calls %d, manual %d", headerCalls, len(fixture.manual))
	}
	assertManagerFileContents(t, filepath.Join(fixture.entryPath, "later-user-file"), "preserve")
}

func TestLegacyUsenetAdoptionDoesNotCheckpointFailedManualReviewWrite(t *testing.T) {
	fixture := newLegacyAdoptionFixture(t, config.DownloadActionNone)
	if err := os.WriteFile(filepath.Join(fixture.entryPath, "extra"), []byte("extra"), 0o600); err != nil {
		t.Fatal(err)
	}
	fixture.adopter.markManual = func(*storage.Entry) error {
		return errors.New("injected queue write failure")
	}
	if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err == nil {
		t.Fatal("migration succeeded despite failed manual-review persistence")
	}
	if _, err := os.Lstat(filepath.Join(fixture.root, usenetLegacyAdoptionCheckpointName)); !os.IsNotExist(err) {
		t.Fatalf("checkpoint exists after unresolved candidate: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(fixture.entryPath, usenetOwnerMarkerName)); !os.IsNotExist(err) {
		t.Fatalf("ambiguous directory gained a marker: %v", err)
	}
}

func TestLegacyUsenetAdoptionRejectsSpoofedCheckpoint(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, string)
	}{
		{
			name: "symlink",
			prepare: func(t *testing.T, root string) {
				t.Helper()
				target := filepath.Join(root, "checkpoint-target")
				if err := os.WriteFile(target, []byte(usenetLegacyAdoptionCheckpointData), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Join(root, usenetLegacyAdoptionCheckpointName)); err != nil {
					t.Skipf("symlink creation unavailable: %v", err)
				}
			},
		},
		{
			name: "hardlink",
			prepare: func(t *testing.T, root string) {
				t.Helper()
				target := filepath.Join(root, "checkpoint-target")
				if err := os.WriteFile(target, []byte(usenetLegacyAdoptionCheckpointData), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(target, filepath.Join(root, usenetLegacyAdoptionCheckpointName)); err != nil {
					t.Skipf("hard links unavailable: %v", err)
				}
			},
		},
		{
			name: "oversized",
			prepare: func(t *testing.T, root string) {
				t.Helper()
				data := usenetLegacyAdoptionCheckpointData + "extra"
				if err := os.WriteFile(filepath.Join(root, usenetLegacyAdoptionCheckpointName), []byte(data), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "portable alias",
			prepare: func(t *testing.T, root string) {
				t.Helper()
				alias := strings.ToUpper(usenetLegacyAdoptionCheckpointName)
				if alias == usenetLegacyAdoptionCheckpointName {
					t.Skip("checkpoint name has no distinct case alias")
				}
				if err := os.WriteFile(filepath.Join(root, alias), []byte(usenetLegacyAdoptionCheckpointData), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			test.prepare(t, root)
			adopter := &legacyUsenetAdopter{downloadRoot: root}
			if err := adopter.run(nil); err == nil {
				t.Fatal("spoofed checkpoint was accepted")
			}
		})
	}
}

func TestLegacyCheckpointNameIsReservedIncludingPortableAliases(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{
		usenetLegacyAdoptionCheckpointName,
		strings.ToUpper(usenetLegacyAdoptionCheckpointName),
	} {
		if _, _, err := usenetEntryPaths(root, "", name); err == nil {
			t.Fatalf("usenetEntryPaths() accepted reserved checkpoint release %q", name)
		}
	}
}

func TestLegacyUsenetAdoptionRejectsMissingHeader(t *testing.T) {
	fixture := newLegacyAdoptionFixture(t, config.DownloadActionNone)
	fixture.adopter.header = func(string) (*storage.NZB, error) {
		return nil, fmt.Errorf("metadata missing")
	}
	if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err != nil {
		t.Fatal(err)
	}
	if len(fixture.manual) != 1 {
		t.Fatalf("manual-review updates = %d, want 1", len(fixture.manual))
	}
}

func TestLegacyUsenetAdoptionSymlinkTestRunsOnSupportedPlatform(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows symlink privilege is environment-specific")
	}
	// This small guard makes accidental removal of the symlink action row
	// visible without duplicating the full action test.
	fixture := newLegacyAdoptionFixture(t, config.DownloadActionSymlink)
	target := filepath.Join(fixture.mountRoot, EntryAllFolder, legacyTestName, legacyTestFile)
	if err := os.Symlink(target, filepath.Join(fixture.entryPath, legacyTestFile)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.adopter.run([]*storage.Entry{fixture.entry}); err != nil {
		t.Fatal(err)
	}
	if len(fixture.manual) != 0 {
		t.Fatal("safe symlink action unexpectedly required manual review")
	}
}

func TestManagerLegacyAdoptionPrecedesJobQueueAndRestore(t *testing.T) {
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	m := &Manager{
		config:         &config.Config{MaxActiveDownloads: 1},
		queue:          newQueue(store, "", lifecycle),
		entryLifecycle: lifecycle,
		logger:         zerolog.Nop(),
	}
	m.resetLifecycle()

	adoptionStarted := make(chan struct{})
	releaseAdoption := make(chan struct{})
	initializationDone := make(chan struct{})
	go func() {
		defer close(initializationDone)
		m.initializeActiveDownloads(func() error {
			close(adoptionStarted)
			<-releaseAdoption
			return nil
		})
	}()

	select {
	case <-adoptionStarted:
	case <-time.After(time.Second):
		t.Fatal("legacy adoption did not start")
	}
	if m.jobQueue != nil {
		t.Fatal("job queue was created before legacy adoption completed")
	}
	if err := m.SubmitJob(&Job{ID: "must-not-enter", Type: JobTypeNZB}); err == nil {
		t.Fatal("new intake was accepted before legacy adoption completed")
	}
	if err := m.waitForBackground(); err != nil {
		t.Fatalf("restore was registered before legacy adoption completed: %v", err)
	}

	close(releaseAdoption)
	select {
	case <-initializationDone:
	case <-time.After(time.Second):
		t.Fatal("active-download initialization did not complete")
	}
	if m.initializationErr != nil {
		t.Fatalf("active-download initialization failed: %v", m.initializationErr)
	}
	if m.jobQueue == nil {
		t.Fatal("job queue was not created after successful legacy adoption")
	}

	m.stopAcceptingBackgroundWork()
	m.cancel()
	m.jobQueue.Close()
	if err := m.waitForBackground(); err != nil {
		t.Fatalf("restore did not stop cleanly: %v", err)
	}
}

func TestManagerLegacyAdoptionFailurePreventsRestoreAndStart(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, *legacyAdoptionFixture, error) func()
	}{
		{
			name: "manual review persistence",
			prepare: func(t *testing.T, fixture *legacyAdoptionFixture, injected error) func() {
				t.Helper()
				if err := os.WriteFile(filepath.Join(fixture.entryPath, "ambiguous"), []byte("preserve"), 0o600); err != nil {
					t.Fatal(err)
				}
				fixture.adopter.markManual = func(*storage.Entry) error {
					return injected
				}
				return func() {}
			},
		},
		{
			name: "checkpoint durability",
			prepare: func(t *testing.T, fixture *legacyAdoptionFixture, injected error) func() {
				t.Helper()
				rooted, err := os.OpenRoot(fixture.entryPath)
				if err != nil {
					t.Fatal(err)
				}
				if err := writeDurableExclusiveUsenetFile(
					rooted,
					usenetOwnerMarkerName,
					legacyTestID+"\n",
					0o600,
				); err != nil {
					_ = rooted.Close()
					t.Fatal(err)
				}
				if err := rooted.Close(); err != nil {
					t.Fatal(err)
				}
				previousSync := usenetSyncFile
				usenetSyncFile = func(*os.File) error {
					return injected
				}
				return func() {
					usenetSyncFile = previousSync
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			injected := fmt.Errorf("injected %s failure", test.name)
			fixture := newLegacyAdoptionFixture(t, config.DownloadActionNone)
			restore := test.prepare(t, fixture, injected)
			defer restore()

			m := &Manager{logger: zerolog.Nop()}
			m.resetLifecycle()
			m.initializeActiveDownloads(func() error {
				return fixture.adopter.run([]*storage.Entry{fixture.entry})
			})

			if !errors.Is(m.initializationErr, injected) {
				t.Fatalf("initialization error = %v, want injected failure", m.initializationErr)
			}
			if m.jobQueue != nil {
				t.Fatal("job queue was created after failed legacy adoption")
			}
			if err := m.waitForBackground(); err != nil {
				t.Fatalf("restore was started after failed legacy adoption: %v", err)
			}
			if err := m.Start(context.Background()); !errors.Is(err, injected) {
				t.Fatalf("Start() error = %v, want injected failure", err)
			}
			if _, err := os.Lstat(filepath.Join(fixture.root, usenetLegacyAdoptionCheckpointName)); !os.IsNotExist(err) {
				t.Fatalf("checkpoint exists after failed startup: %v", err)
			}
			m.cancel()
		})
	}
}

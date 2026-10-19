package manager

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

func TestTorrentFileLayoutsPreserveUnambiguousNestedDuplicateBasenames(t *testing.T) {
	entry := torrentOwnershipTestEntry(t.TempDir(), "layout-owner", config.DownloadActionSymlink)
	entry.Files = map[string]*storage.File{
		"season-1": {
			Name: "episode.mkv",
			Path: "Release/Season 01/episode.mkv",
		},
		"season-2": {
			Name: "Season 02/episode.mkv",
			Path: "Release/Season 02/episode.mkv",
		},
	}
	layouts, err := torrentEntryFileLayouts(entry)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{layouts[0].relative, layouts[1].relative}
	want := []string{
		filepath.Join("Season 01", "episode.mkv"),
		filepath.Join("Season 02", "episode.mkv"),
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("layout %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestApplyDebridTorrentPreservesCollisionSafeLogicalNamesAndPaths(t *testing.T) {
	entry := &storage.Entry{
		Protocol:  config.ProtocolTorrent,
		InfoHash:  "torbox-logical-paths",
		Files:     make(map[string]*storage.File),
		Providers: make(map[string]*storage.ProviderEntry),
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "11", Path: "Release/Season 01/Episode.mkv", Link: "torbox://42/11"},
		{Id: "12", Path: "Release/Season 02/Episode.mkv", Link: "torbox://42/12"},
		{Id: "13", Path: "Release?/Extras/Why?.mkv", Link: "torbox://42/13"},
	})
	if err != nil {
		t.Fatal(err)
	}
	remote := &debridTypes.Torrent{
		Id:       "42",
		InfoHash: entry.InfoHash,
		Name:     "Release",
		Debrid:   "torbox",
		Files:    remoteFiles,
	}

	if err := applyDebridTorrentToEntry(entry, remote); err != nil {
		t.Fatal(err)
	}

	if len(entry.Files) != 3 {
		t.Fatalf("managed file count = %d, want 3", len(entry.Files))
	}
	placement := entry.Providers["torbox"]
	if placement == nil || len(placement.Files) != 3 {
		t.Fatalf("provider files = %#v, want 3", placement)
	}
	for name, remoteFile := range remote.Files {
		managed := entry.Files[name]
		provider := placement.Files[name]
		if managed == nil || managed.Path != remoteFile.LocalPath() {
			t.Fatalf("managed file %q = %#v", name, managed)
		}
		if provider == nil || provider.Path != remoteFile.Path || provider.Id != remoteFile.Id {
			t.Fatalf("provider file %q = %#v", name, provider)
		}
	}
	if _, err := torrentEntryFileLayouts(entry); err != nil {
		t.Fatalf("portable managed layout rejected: %v", err)
	}
}

func TestTorrentFileLayoutsAcceptSanitizedProviderOwnershipNames(t *testing.T) {
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "marker", Path: ".tessarr-torrent-owner-v1", Size: 1},
		{Id: "partial", Path: ".decypharr-torrent-part-provider/Episode.mkv", Size: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	entry := torrentOwnershipTestEntry(t.TempDir(), "reserved-provider-names", config.DownloadActionDownload)
	entry.Files = make(map[string]*storage.File, len(remoteFiles))
	for name, file := range remoteFiles {
		entry.Files[name] = &storage.File{Name: name, Path: file.LocalPath(), Size: file.Size}
	}
	if _, err := torrentEntryFileLayouts(entry); err != nil {
		t.Fatalf("sanitized provider ownership names were rejected: %v", err)
	}
}

func TestApplyCompletedTorrentFilesRecoversLateProviderLinks(t *testing.T) {
	entry := &storage.Entry{
		InfoHash:       "late-provider-links",
		Status:         debridTypes.TorrentStatusDownloaded,
		Files:          make(map[string]*storage.File),
		Providers:      make(map[string]*storage.ProviderEntry),
		ActiveProvider: "premiumize-main",
	}
	remote := &debridTypes.Torrent{
		Id:       "transfer-1",
		InfoHash: entry.InfoHash,
		Name:     "Movie",
		Debrid:   "premiumize-main",
		Status:   debridTypes.TorrentStatusDownloaded,
		Files:    make(map[string]debridTypes.File),
	}

	if ready, err := applyCompletedTorrentFiles(entry, remote); err != nil || ready {
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("applyCompletedTorrentFiles() = ready before provider links exist")
	}
	if entry.Status != debridTypes.TorrentStatusDownloading {
		t.Fatalf("entry status = %q, want downloading while provider links are pending", entry.Status)
	}

	pendingFile := debridTypes.File{
		Id:   "item-1",
		Name: "Movie.mkv",
		Path: "Movie.mkv",
		Size: 1234,
	}
	remote.Files["Movie.mkv"] = pendingFile
	if ready, err := applyCompletedTorrentFiles(entry, remote); err != nil || ready {
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("applyCompletedTorrentFiles() = ready for a provider file without a link")
	}
	if len(entry.Files) != 0 {
		t.Fatalf("canonical files = %#v, want partial provider tree kept out", entry.Files)
	}

	pendingFile.Link = "https://provider.example/media"
	remote.Files["Movie.mkv"] = pendingFile
	ready, err := applyCompletedTorrentFiles(entry, remote)
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatal("applyCompletedTorrentFiles() = not ready after provider link appeared")
	}
	managed := entry.Files["Movie.mkv"]
	placement := entry.Providers["premiumize-main"]
	if managed == nil || managed.Path != "Movie.mkv" || managed.Size != 1234 {
		t.Fatalf("managed file = %#v, want recovered file metadata", managed)
	}
	if placement == nil || placement.Files["Movie.mkv"] == nil || placement.Files["Movie.mkv"].Link == "" {
		t.Fatalf("provider placement = %#v, want recovered provider link", placement)
	}
}

func TestApplyCompletedTorrentFilesDoesNotPublishPartialTreeOverExistingFiles(t *testing.T) {
	existingFile := &storage.File{Name: "Movie.mkv", Path: "Movie.mkv", Size: 1234}
	existingProviderFile := &storage.ProviderFile{Id: "movie", Path: "Movie.mkv", Link: "provider://movie"}
	entry := &storage.Entry{
		InfoHash:       "existing-canonical-files",
		Status:         debridTypes.TorrentStatusDownloaded,
		ActiveProvider: "primary",
		Files:          map[string]*storage.File{"Movie.mkv": existingFile},
		Providers: map[string]*storage.ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*storage.ProviderFile{"Movie.mkv": existingProviderFile}},
		},
	}
	remote := &debridTypes.Torrent{
		Id: "transfer", InfoHash: entry.InfoHash, Debrid: "primary", Status: debridTypes.TorrentStatusDownloaded,
		Files: map[string]debridTypes.File{
			"Other.mkv": {Id: "other", Name: "Other.mkv", Path: "Other.mkv", Size: 55},
		},
	}
	ready, err := applyCompletedTorrentFiles(entry, remote)
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("partial provider tree reported ready")
	}
	if len(entry.Files) != 1 || entry.Files["Movie.mkv"] != existingFile {
		t.Fatalf("partial tree changed canonical files: %#v", entry.Files)
	}
	placement := entry.Providers["primary"]
	if len(placement.Files) != 1 || placement.Files["Movie.mkv"] != existingProviderFile {
		t.Fatalf("partial tree replaced provider identity: %#v", placement.Files)
	}
}

func TestTorrentFileLayoutsRejectTraversalAliasesAndAmbiguousBasenames(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]*storage.File
	}{
		{
			name: "traversal",
			files: map[string]*storage.File{
				"bad": {Name: "episode.mkv", Path: "../episode.mkv"},
			},
		},
		{
			name: "portable case collision",
			files: map[string]*storage.File{
				"a": {Name: "Season/Episode.mkv", Path: "Season/Episode.mkv"},
				"b": {Name: "season/episode.MKV", Path: "season/episode.MKV"},
			},
		},
		{
			name: "file directory prefix collision",
			files: map[string]*storage.File{
				"a": {Name: "Season", Path: "Season"},
				"b": {Name: "Season/Episode.mkv", Path: "Season/Episode.mkv"},
			},
		},
		{
			name: "duplicate logical key is ambiguous",
			files: map[string]*storage.File{
				"a": {Name: "episode.mkv", Path: "Season 01/episode.mkv"},
				"b": {Name: "episode.mkv", Path: "Season 02/episode.mkv"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entry := torrentOwnershipTestEntry(t.TempDir(), "layout-"+strings.ReplaceAll(test.name, " ", "-"), config.DownloadActionSymlink)
			entry.Files = test.files
			if _, err := torrentEntryFileLayouts(entry); err == nil {
				t.Fatal("unsafe torrent layout was accepted")
			}
		})
	}
}

func TestTorrentFileLayoutsAcceptFoldCollisionAddedAfterMaterialization(t *testing.T) {
	initial, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id: "nested", Path: "Release/movie.mkv/extra.srt", Size: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	completed := time.Now()
	entry := torrentOwnershipTestEntry(t.TempDir(), "folded-refresh", config.DownloadActionSymlink)
	entry.CompletedAt = &completed
	entry.Files = make(map[string]*storage.File)
	entry.Providers = map[string]*storage.ProviderEntry{
		"primary": {Provider: "primary", Files: make(map[string]*storage.ProviderFile)},
	}
	for name, file := range initial {
		entry.Files[name] = &storage.File{Name: name, Path: file.LocalPath(), Size: file.Size}
		entry.Providers["primary"].Files[name] = &storage.ProviderFile{Id: file.Id, Path: file.Path}
	}
	expanded, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "nested", Path: "Release/movie.mkv/extra.srt", Size: 1},
		{Id: "file", Path: "Release/Movie.mkv", Size: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.AddTorrentProvider(&debridTypes.Torrent{
		Debrid: "primary", Status: debridTypes.TorrentStatusDownloaded, Files: expanded,
	}); err != nil {
		t.Fatal(err)
	}
	layouts, err := torrentEntryFileLayouts(entry)
	if err != nil {
		t.Fatalf("folded-directory refresh produced an invalid materialized layout: %v", err)
	}
	if len(layouts) != 2 {
		t.Fatalf("folded-directory refresh layouts = %#v", layouts)
	}
}

func TestTorrentFileLayoutsRejectInvalidOrUnboundedDownloadSizes(t *testing.T) {
	tests := []struct {
		name  string
		size  int64
		bytes *[2]int64
	}{
		{name: "negative size", size: -1},
		{name: "negative range", size: 1, bytes: &[2]int64{-1, 0}},
		{name: "inverted range", size: 1, bytes: &[2]int64{2, 1}},
		{name: "overflowing range", size: 1, bytes: &[2]int64{0, math.MaxInt64}},
		{name: "unknown download size", size: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entry := torrentOwnershipTestEntry(t.TempDir(), "invalid-size-"+strings.ReplaceAll(test.name, " ", "-"), config.DownloadActionDownload)
			entry.Files["movie.mkv"].Size = test.size
			entry.Files["movie.mkv"].ByteRange = test.bytes
			if _, err := torrentEntryFileLayouts(entry); err == nil {
				t.Fatal("unsafe transfer metadata was accepted")
			}
		})
	}

	symlink := torrentOwnershipTestEntry(t.TempDir(), "unknown-symlink-size", config.DownloadActionSymlink)
	symlink.Files["movie.mkv"].Size = 0
	if _, err := torrentEntryFileLayouts(symlink); err != nil {
		t.Fatalf("symlink action rejected an unknown provider size: %v", err)
	}
}

func TestTorrentFileLayoutsEnforceEntryCountAndTotalTransferBounds(t *testing.T) {
	tooMany := torrentOwnershipTestEntry(t.TempDir(), "too-many-files", config.DownloadActionSymlink)
	tooMany.Files = make(map[string]*storage.File, torrentOwnershipMaxEntries+1)
	shared := &storage.File{Name: "movie.mkv", Path: "Release/movie.mkv"}
	for index := 0; index <= torrentOwnershipMaxEntries; index++ {
		tooMany.Files[strconv.Itoa(index)] = shared
	}
	if _, err := torrentEntryFileLayouts(tooMany); err == nil {
		t.Fatal("torrent file count above the bounded filesystem ceiling was accepted")
	}

	root := t.TempDir()
	overflow := torrentOwnershipTestEntry(root, "transfer-overflow", config.DownloadActionDownload)
	overflow.Files = map[string]*storage.File{
		"large.mkv": {Name: "large.mkv", Path: "Release/large.mkv", Size: math.MaxInt64},
		"extra.mkv": {Name: "extra.mkv", Path: "Release/extra.mkv", Size: 1},
	}
	downloader := &Downloader{dest: root, logger: zerolog.Nop()}
	err := downloader.processTorrentDownload(context.Background(), overflow)
	if err == nil || !strings.Contains(err.Error(), "total transfer size overflows") {
		t.Fatalf("overflow error = %v", err)
	}
	if _, err := os.Lstat(overflow.DownloadPath()); !os.IsNotExist(err) {
		t.Fatalf("overflowing transfer claimed an output directory: %v", err)
	}
}

func TestClaimTorrentEntryDirectoryWritesDurableOwnerAndRollsBackOnSyncFailure(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "durable-owner", config.DownloadActionDownload)
	path, newlyClaimed, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{})
	if err != nil {
		t.Fatal(err)
	}
	if !newlyClaimed {
		t.Fatal("new torrent release directory was not reported as newly claimed")
	}
	assertTorrentOwnerMarker(t, path, "durable-owner")

	failing := torrentOwnershipTestEntry(root, "sync-failure", config.DownloadActionDownload)
	failing.Name = "Other Release"
	originalSyncDirectory := torrentSyncDirectory
	torrentSyncDirectory = func(*os.File) error { return errors.New("injected directory sync failure") }
	t.Cleanup(func() { torrentSyncDirectory = originalSyncDirectory })
	if _, _, err := claimTorrentEntryDirectory(root, failing, torrentLegacyProof{}); err == nil {
		t.Fatal("ownership claim ignored directory sync failure")
	}
	if _, err := os.Lstat(filepath.Join(failing.DownloadPath(), torrentOwnerMarkerName)); !os.IsNotExist(err) {
		t.Fatalf("failed ownership marker remained after sync failure: %v", err)
	}
}

func TestClaimTorrentEntryDirectoryAcceptsLegacyOwnerMarker(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "legacy-marker-owner", config.DownloadActionDownload)
	path, newlyClaimed, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{})
	if err != nil {
		t.Fatal(err)
	}
	if !newlyClaimed {
		t.Fatal("initial torrent claim was not reported as new")
	}
	if err := os.Rename(
		filepath.Join(path, torrentOwnerMarkerName),
		filepath.Join(path, legacyTorrentOwnerMarkerName),
	); err != nil {
		t.Fatal(err)
	}

	retryPath, newlyClaimed, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{})
	if err != nil {
		t.Fatalf("legacy owner marker was not accepted: %v", err)
	}
	if newlyClaimed || retryPath != path {
		t.Fatalf("legacy marker retry = (%q, %t), want (%q, false)", retryPath, newlyClaimed, path)
	}
	assertTorrentTestContents(t, filepath.Join(path, legacyTorrentOwnerMarkerName), "legacy-marker-owner\n")
	if _, err := os.Lstat(filepath.Join(path, torrentOwnerMarkerName)); !os.IsNotExist(err) {
		t.Fatalf("retry rewrote the legacy marker: %v", err)
	}
}

func TestTorrentOwnerMarkerAliasesMustAgree(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "marker-conflict-owner", config.DownloadActionDownload)
	path, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, legacyTorrentOwnerMarkerName), []byte("different-owner\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err == nil ||
		!strings.Contains(err.Error(), "markers disagree") {
		t.Fatalf("conflicting torrent ownership markers error = %v", err)
	}
}

func TestClaimTorrentEntryDirectoryConservativelyAdoptsExactLegacySymlinks(t *testing.T) {
	root := t.TempDir()
	mount := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "legacy-symlink", config.DownloadActionSymlink)
	entry.Files = map[string]*storage.File{
		"nested": {
			Name: "Season 01/episode.mkv",
			Path: "Release/Season 01/episode.mkv",
		},
	}
	relative := filepath.Join("Season 01", "episode.mkv")
	target := filepath.Join(mount, relative)
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("video"), 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(entry.DownloadPath(), relative)
	if err := os.MkdirAll(filepath.Dir(output), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, output); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, newlyClaimed, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{mountPath: mount}); err != nil {
		t.Fatal(err)
	} else if !newlyClaimed {
		t.Fatal("exact legacy directory was not adopted")
	}
	assertTorrentOwnerMarker(t, entry.DownloadPath(), "legacy-symlink")

	wrong := torrentOwnershipTestEntry(root, "wrong-legacy", config.DownloadActionSymlink)
	wrong.Name = "Wrong Release"
	wrong.Files = map[string]*storage.File{
		"nested": {
			Name: "Season 01/episode.mkv",
			Path: "Wrong Release/Season 01/episode.mkv",
		},
	}
	wrongOutput := filepath.Join(wrong.DownloadPath(), relative)
	if err := os.MkdirAll(filepath.Dir(wrongOutput), 0o700); err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(t.TempDir(), "unrelated.mkv")
	if err := os.WriteFile(external, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, wrongOutput); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, _, err := claimTorrentEntryDirectory(root, wrong, torrentLegacyProof{mountPath: mount}); err == nil ||
		!strings.Contains(err.Error(), "manual review") {
		t.Fatalf("wrong-target legacy directory error = %v", err)
	}
	if got, err := os.Readlink(wrongOutput); err != nil || got != external {
		t.Fatalf("wrong-target legacy link changed: target=%q error=%v", got, err)
	}
}

func TestRemoveOwnedTorrentDirectoryRequiresMarkerAndRecoversQuarantine(t *testing.T) {
	root := t.TempDir()
	unowned := torrentOwnershipTestEntry(root, "unowned", config.DownloadActionDownload)
	if err := os.MkdirAll(unowned.DownloadPath(), 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(unowned.DownloadPath(), "keep")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeOwnedTorrentEntryDirectory(root, unowned); err == nil {
		t.Fatal("unowned torrent directory was accepted for deletion")
	}
	assertTorrentTestContents(t, sentinel, "keep")

	owned := torrentOwnershipTestEntry(root, "quarantine-owner", config.DownloadActionDownload)
	owned.Name = "Owned Release"
	ownedPath, _, err := claimTorrentEntryDirectory(root, owned, torrentLegacyProof{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ownedPath, "payload"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	originalHook := torrentAfterQuarantine
	torrentAfterQuarantine = func(string) error { return errors.New("simulated crash") }
	t.Cleanup(func() { torrentAfterQuarantine = originalHook })
	if err := removeOwnedTorrentEntryDirectory(root, owned); err == nil {
		t.Fatal("simulated crash did not interrupt quarantine cleanup")
	}
	if _, err := os.Lstat(ownedPath); !os.IsNotExist(err) {
		t.Fatalf("visible owned path remained after quarantine: %v", err)
	}
	torrentAfterQuarantine = nil
	if err := removeOwnedTorrentEntryDirectory(root, owned); err != nil {
		t.Fatalf("quarantine recovery failed: %v", err)
	}
	if matches, err := filepath.Glob(filepath.Join(filepath.Dir(ownedPath), torrentQuarantinePrefix+"*")); err != nil || len(matches) != 0 {
		t.Fatalf("quarantine remnants = %v, error=%v", matches, err)
	}
}

func TestRemoveOwnedTorrentDirectoryRecoversLegacyQuarantine(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "legacy-quarantine-owner", config.DownloadActionDownload)
	ownedPath, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ownedPath, "payload"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(root, ownedPath)
	if err != nil {
		t.Fatal(err)
	}
	legacyQuarantine := filepath.Join(
		filepath.Dir(ownedPath),
		legacyTorrentQuarantinePrefix+strings.TrimPrefix(
			torrentQuarantinePrefixForEntry("legacy-quarantine-owner", relative),
			torrentQuarantinePrefix,
		)+"crash",
	)
	if err := os.Rename(ownedPath, legacyQuarantine); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(
		filepath.Join(legacyQuarantine, torrentOwnerMarkerName),
		filepath.Join(legacyQuarantine, legacyTorrentOwnerMarkerName),
	); err != nil {
		t.Fatal(err)
	}

	if err := removeOwnedTorrentEntryDirectory(root, entry); err != nil {
		t.Fatalf("legacy torrent quarantine recovery failed: %v", err)
	}
	if _, err := os.Lstat(legacyQuarantine); !os.IsNotExist(err) {
		t.Fatalf("legacy torrent quarantine remained: %v", err)
	}
}

func TestRemoveOwnedTorrentDirectoryPreservesPathSwapReplacement(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "path-swap-owner", config.DownloadActionDownload)
	path, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "owned"), []byte("owned"), 0o600); err != nil {
		t.Fatal(err)
	}
	replacementSentinel := filepath.Join(path, "replacement")
	originalHook := torrentAfterQuarantine
	torrentAfterQuarantine = func(visible string) error {
		if err := os.MkdirAll(visible, 0o700); err != nil {
			return err
		}
		return os.WriteFile(replacementSentinel, []byte("preserve"), 0o600)
	}
	t.Cleanup(func() { torrentAfterQuarantine = originalHook })
	if err := removeOwnedTorrentEntryDirectory(root, entry); err != nil {
		t.Fatal(err)
	}
	assertTorrentTestContents(t, replacementSentinel, "preserve")
	if err := removeOwnedTorrentEntryDirectory(root, entry); err == nil {
		t.Fatal("unowned path-swap replacement was accepted on retry")
	}
	assertTorrentTestContents(t, replacementSentinel, "preserve")
}

func TestRemoveOwnedTorrentDirectoryNeverRecursesIntoVerifiedQuarantineReplacement(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "quarantine-swap-owner", config.DownloadActionDownload)
	ownedPath, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ownedPath, "owned"), []byte("owned"), 0o600); err != nil {
		t.Fatal(err)
	}

	var movedOwned string
	var replacementSentinel string
	originalHook := torrentAfterQuarantineVerified
	torrentAfterQuarantineVerified = func(quarantineRelative string) error {
		quarantine := filepath.Join(root, quarantineRelative)
		movedOwned = quarantine + "-moved-owned"
		if err := os.Rename(quarantine, movedOwned); err != nil {
			return err
		}
		if err := os.Mkdir(quarantine, 0o700); err != nil {
			return err
		}
		replacementSentinel = filepath.Join(quarantine, "preserve")
		return os.WriteFile(replacementSentinel, []byte("replacement"), 0o600)
	}
	t.Cleanup(func() { torrentAfterQuarantineVerified = originalHook })

	err = removeOwnedTorrentEntryDirectory(root, entry)
	if err == nil || !strings.Contains(err.Error(), "changed before marker removal") {
		t.Fatalf("quarantine replacement error = %v", err)
	}
	assertTorrentTestContents(t, replacementSentinel, "replacement")
	assertTorrentOwnerMarker(t, movedOwned, "quarantine-swap-owner")
	if _, err := os.Lstat(filepath.Join(movedOwned, "owned")); !os.IsNotExist(err) {
		t.Fatalf("pinned owned content remained after cleanup: %v", err)
	}
}

func TestTorrentOwnerMarkerSwapIsRejected(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "marker-swap-owner", config.DownloadActionDownload)
	path, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{})
	if err != nil {
		t.Fatal(err)
	}
	originalHook := torrentAfterMarkerLstat
	called := false
	torrentAfterMarkerLstat = func(root *os.Root) error {
		if called {
			return nil
		}
		called = true
		if err := root.Rename(torrentOwnerMarkerName, torrentOwnerMarkerName+".old"); err != nil {
			return err
		}
		return root.WriteFile(torrentOwnerMarkerName, []byte("marker-swap-owner\n"), 0o600)
	}
	t.Cleanup(func() { torrentAfterMarkerLstat = originalHook })
	if err := removeOwnedTorrentEntryDirectory(root, entry); err == nil ||
		!strings.Contains(err.Error(), "changed while opening") {
		t.Fatalf("marker swap error = %v", err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("release directory was removed after marker swap: %v", err)
	}
}

func TestOwnedTorrentPartRejectsHardlinkedRecovery(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "part-hardlink", config.DownloadActionDownload)
	if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
		t.Fatal(err)
	}
	part, err := openOwnedTorrentPart(root, entry, "movie.mkv", 4)
	if err != nil {
		t.Fatal(err)
	}
	partPath := part.partAbsolutePath
	if _, err := part.file.Write([]byte("da")); err != nil {
		t.Fatal(err)
	}
	if err := part.Close(); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside-link")
	if err := os.Link(partPath, outside); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	if _, err := openOwnedTorrentPart(root, entry, "movie.mkv", 4); err == nil {
		t.Fatal("hardlinked recovered partial file was accepted")
	}
	assertTorrentTestContents(t, outside, "da")
}

func TestOwnedTorrentPartResumesLegacyPartial(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "legacy-part-owner", config.DownloadActionDownload)
	if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
		t.Fatal(err)
	}
	part, err := openOwnedTorrentPart(root, entry, "movie.mkv", 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.file.Write([]byte("ab")); err != nil {
		t.Fatal(err)
	}
	currentPath := part.partAbsolutePath
	if err := part.Close(); err != nil {
		t.Fatal(err)
	}
	legacyPath := filepath.Join(
		filepath.Dir(currentPath),
		strings.Replace(filepath.Base(currentPath), torrentPartialPrefix, legacyTorrentPartialPrefix, 1),
	)
	if err := os.Rename(currentPath, legacyPath); err != nil {
		t.Fatal(err)
	}

	recovered, err := openOwnedTorrentPart(root, entry, "movie.mkv", 4)
	if err != nil {
		t.Fatalf("legacy torrent partial was not resumed: %v", err)
	}
	defer recovered.Close()
	if recovered.partAbsolutePath != legacyPath {
		t.Fatalf("resumed partial = %q, want %q", recovered.partAbsolutePath, legacyPath)
	}
	if size, err := recovered.Size(); err != nil || size != 2 {
		t.Fatalf("legacy partial size = %d, error=%v", size, err)
	}
}

func TestOwnedTorrentPartRecoversPublishedCrashHardLinks(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "part-published-crash", config.DownloadActionDownload)
	if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
		t.Fatal(err)
	}
	part, err := openOwnedTorrentPart(root, entry, "movie.mkv", 4)
	if err != nil {
		t.Fatal(err)
	}
	partPath := part.partAbsolutePath
	if _, err := part.file.Write([]byte("good")); err != nil {
		t.Fatal(err)
	}
	if err := part.Close(); err != nil {
		t.Fatal(err)
	}
	finalPath := filepath.Join(entry.DownloadPath(), "movie.mkv")
	if err := os.Link(partPath, finalPath); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}

	recovered, err := openOwnedTorrentPart(root, entry, "movie.mkv", 4)
	if err != nil {
		t.Fatalf("valid two-link published crash state was rejected: %v", err)
	}
	if err := recovered.Commit(); err != nil {
		_ = recovered.Close()
		t.Fatalf("commit recovered published crash state: %v", err)
	}
	if _, err := os.Lstat(partPath); !os.IsNotExist(err) {
		t.Fatalf("published crash partial link remained: %v", err)
	}
	assertTorrentTestContents(t, finalPath, "good")
}

func TestOwnedTorrentPartCommitNeverOverwritesWrongFinal(t *testing.T) {
	tests := []string{"regular", "symlink", "directory"}
	for _, kind := range tests {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			entry := torrentOwnershipTestEntry(root, "publish-"+kind, config.DownloadActionDownload)
			if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
				t.Fatal(err)
			}
			part, err := openOwnedTorrentPart(root, entry, "movie.mkv", 4)
			if err != nil {
				t.Fatal(err)
			}
			defer part.Close()
			if _, err := part.file.Write([]byte("good")); err != nil {
				t.Fatal(err)
			}
			final := filepath.Join(entry.DownloadPath(), "movie.mkv")
			switch kind {
			case "regular":
				if err := os.WriteFile(final, []byte("wrong"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				external := filepath.Join(t.TempDir(), "external")
				if err := os.WriteFile(external, []byte("wrong"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(external, final); err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
			case "directory":
				if err := os.Mkdir(final, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if err := part.Commit(); err == nil {
				t.Fatal("wrong existing final artifact was overwritten")
			}
			info, err := os.Lstat(final)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "regular":
				assertTorrentTestContents(t, final, "wrong")
			case "symlink":
				if info.Mode()&os.ModeSymlink == 0 {
					t.Fatal("existing final symlink was replaced")
				}
			case "directory":
				if !info.IsDir() {
					t.Fatal("existing final directory was replaced")
				}
			}
		})
	}
}

func TestWriteOwnedTorrentFileUsesNoOverwriteIdempotence(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "strm-no-overwrite", config.DownloadActionStrm)
	if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
		t.Fatal(err)
	}
	relative := filepath.Join("Season 01", "episode.strm")
	if err := writeOwnedTorrentFile(root, entry, relative, []byte("same"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeOwnedTorrentFile(root, entry, relative, []byte("same"), 0o600); err != nil {
		t.Fatalf("exact idempotent STRM retry failed: %v", err)
	}
	if err := writeOwnedTorrentFile(root, entry, relative, []byte("wrong"), 0o600); err == nil {
		t.Fatal("wrong existing STRM artifact was replaced")
	}
	assertTorrentTestContents(t, filepath.Join(entry.DownloadPath(), relative), "same")
}

func torrentOwnershipTestEntry(root, owner string, action config.DownloadAction) *storage.Entry {
	return &storage.Entry{
		Protocol: config.ProtocolTorrent,
		InfoHash: owner,
		Name:     "Release",
		SavePath: filepath.Join(root, "radarr"),
		Action:   action,
		Files: map[string]*storage.File{
			"movie.mkv": {
				Name: "movie.mkv",
				Path: "Release/movie.mkv",
				Size: 4,
			},
		},
	}
}

func assertTorrentOwnerMarker(t *testing.T, entryPath, want string) {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(entryPath, torrentOwnerMarkerName))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != want+"\n" {
		t.Fatalf("owner marker = %q, want %q", contents, want+"\n")
	}
}

func assertTorrentTestContents(t *testing.T, path, want string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != want {
		t.Fatalf("%q contents = %q, want %q", path, contents, want)
	}
}

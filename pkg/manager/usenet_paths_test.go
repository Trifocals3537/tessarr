package manager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

func TestScanBoundedUsenetDirectoryAdvancesAcrossBatches(t *testing.T) {
	root := t.TempDir()
	const entries = usenetDirectoryReadBatch*2 + 7
	for i := range entries {
		path := filepath.Join(root, fmt.Sprintf("entry-%04d", i))
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	dir, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	scanErr := scanBoundedUsenetDirectory(dir, func(os.DirEntry) (bool, error) {
		seen++
		return false, nil
	})
	closeErr := dir.Close()
	if scanErr != nil {
		t.Fatal(scanErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if seen != entries {
		t.Fatalf("scanned entries = %d, want %d", seen, entries)
	}
}

func TestUsenetEntryPathsAcceptNormalRelease(t *testing.T) {
	root := t.TempDir()
	name := "[Group] Show Name - S01E02 [1080p].mkv"
	savePath, downloadPath, err := usenetEntryPaths(root, "sonarr", name)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(root, "sonarr"); savePath != want {
		t.Fatalf("savePath = %q, want %q", savePath, want)
	}
	if want := filepath.Join(root, "sonarr", "[Group] Show Name - S01E02 [1080p]"); downloadPath != want {
		t.Fatalf("downloadPath = %q, want %q", downloadPath, want)
	}
}

func TestUsenetEntryPathsAcceptEmptyCategoryAtConfiguredRoot(t *testing.T) {
	root := t.TempDir()
	savePath, downloadPath, err := usenetEntryPaths(root, "", "Release.mkv")
	if err != nil {
		t.Fatal(err)
	}
	if savePath != root {
		t.Fatalf("savePath = %q, want configured root %q", savePath, root)
	}
	if want := filepath.Join(root, "Release"); downloadPath != want {
		t.Fatalf("downloadPath = %q, want %q", downloadPath, want)
	}
}

func TestUsenetEntryPathsCompactOversizedNZBName(t *testing.T) {
	root := t.TempDir()
	name := "My Status as an Assassin Obviously Exceeds the Heros S01E01 The Assassin Easts Bread 1080p CR WEB-DL DUAL AAC2.0 H 264-VARYG (Ansatsusha de Aru Ore no Status ga Yuusha yori mo Akiraka ni Tsuyoi no da ga, Dual-Audio, Multi-Subs) [1+9] - My.Status.as.an.As.nzb"
	if len(name) <= safepath.PortableIdentifierMaxBytes {
		t.Fatalf("live regression fixture is only %d bytes", len(name))
	}
	savePath, firstPath, err := usenetEntryPaths(root, "sonarr", name)
	if err != nil {
		t.Fatal(err)
	}
	_, secondPath, err := usenetEntryPaths(root, "sonarr", name)
	if err != nil {
		t.Fatal(err)
	}
	if firstPath != secondPath {
		t.Fatalf("compacted path is not deterministic: %q != %q", firstPath, secondPath)
	}
	component := filepath.Base(firstPath)
	if len(component) > safepath.PortableIdentifierMaxBytes {
		t.Fatalf("release component length = %d, want <= %d", len(component), safepath.PortableIdentifierMaxBytes)
	}
	if !strings.HasSuffix(component, ".nzb") {
		t.Fatalf("release component %q did not preserve .nzb", component)
	}
	if strings.ContainsRune(component, utf8.RuneError) {
		t.Fatalf("release component contains a broken UTF-8 rune: %q", component)
	}

	entry := normalNZBEntry(root)
	entry.Name = name
	entry.OriginalFilename = name
	entry.SavePath = savePath
	entry.OutputName = component
	claimedPath, newlyClaimed, err := claimUsenetEntryDirectory(root, entry)
	if err != nil {
		t.Fatal(err)
	}
	if !newlyClaimed || claimedPath != firstPath {
		t.Fatalf("claim = (%q, %t), want (%q, true)", claimedPath, newlyClaimed, firstPath)
	}
	if info, err := os.Stat(claimedPath); err != nil || !info.IsDir() {
		t.Fatalf("compacted release directory was not created: info=%v err=%v", info, err)
	}
}

func TestRequireConfiguredUsenetDownloadRootRejectsCustomRoot(t *testing.T) {
	configured := t.TempDir()
	custom := t.TempDir()
	if _, err := requireConfiguredUsenetDownloadRoot(configured, custom); err == nil {
		t.Fatal("requireConfiguredUsenetDownloadRoot() accepted a custom root")
	}
	got, err := requireConfiguredUsenetDownloadRoot(configured, configured)
	if err != nil {
		t.Fatal(err)
	}
	if got != configured {
		t.Fatalf("configured root = %q, want %q", got, configured)
	}
}

func TestUsenetEntryPathsRejectUnsafeCategoryAndName(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name     string
		category string
		nzbName  string
	}{
		{name: "category traversal", category: "..", nzbName: "release"},
		{name: "nested category", category: "tv/../../outside", nzbName: "release"},
		{name: "absolute category", category: string(filepath.Separator) + "outside", nzbName: "release"},
		{name: "reserved lock category", category: usenetOwnershipLockName, nzbName: "release"},
		{name: "reserved quarantine category", category: usenetQuarantinePrefix + "spoof", nzbName: "release"},
		{name: "name traversal", category: "sonarr", nzbName: "../outside"},
		{name: "nested name", category: "sonarr", nzbName: "season/release"},
		{name: "windows name", category: "sonarr", nzbName: `C:\outside`},
		{name: "control character", category: "sonarr", nzbName: "bad\nname"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := usenetEntryPaths(root, test.category, test.nzbName); err == nil {
				t.Fatalf("usenetEntryPaths(%q, %q) error = nil", test.category, test.nzbName)
			}
		})
	}
}

func TestUsenetEntryPathsRejectSymlinkParentAndTarget(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	categoryLink := filepath.Join(root, "sonarr")
	if err := os.Symlink(outside, categoryLink); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, _, err := usenetEntryPaths(root, "sonarr", "release"); err == nil {
		t.Fatal("usenetEntryPaths() accepted a symlink category")
	}

	if err := os.Remove(categoryLink); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(categoryLink, 0o700); err != nil {
		t.Fatal(err)
	}
	targetLink := filepath.Join(categoryLink, "release")
	if err := os.Symlink(outside, targetLink); err != nil {
		t.Fatal(err)
	}
	if _, _, err := usenetEntryPaths(root, "sonarr", "release"); err == nil {
		t.Fatal("usenetEntryPaths() accepted a symlink release target")
	}
}

func TestSafeUsenetEntryDownloadPathRejectsTamperedSavePath(t *testing.T) {
	root := t.TempDir()
	entry := normalNZBEntry(root)
	entry.SavePath = filepath.Join(filepath.Dir(root), "outside")
	if _, err := safeUsenetEntryDownloadPath(root, entry); err == nil {
		t.Fatal("safeUsenetEntryDownloadPath() accepted a tampered SavePath")
	}
}

func TestPrepareUsenetDownloadDirectorySucceedsForNormalNZB(t *testing.T) {
	root := t.TempDir()
	entry := normalNZBEntry(root)
	downloader := &Downloader{dest: root}

	got, err := downloader.prepareUsenetDownloadDirectory(entry)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "sonarr", "[Group] Show S01E01 [1080p]")
	if got != want {
		t.Fatalf("prepareUsenetDownloadDirectory() = %q, want %q", got, want)
	}
	info, err := os.Stat(got)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("%q is not a directory", got)
	}
	if _, err := safeUsenetFilePath(root, entry, "Show S01E01.mkv"); err != nil {
		t.Fatalf("normal NZB file path rejected: %v", err)
	}
}

func TestClaimUsenetEntryDirectoryIsIdempotentForSameOwner(t *testing.T) {
	root := t.TempDir()
	entry := normalNZBEntry(root)

	firstPath, newlyClaimed, err := claimUsenetEntryDirectory(root, entry)
	if err != nil {
		t.Fatal(err)
	}
	if !newlyClaimed {
		t.Fatal("first claim was not reported as new")
	}
	secondPath, newlyClaimed, err := claimUsenetEntryDirectory(root, entry)
	if err != nil {
		t.Fatalf("same-owner retry failed: %v", err)
	}
	if newlyClaimed {
		t.Fatal("same-owner retry was reported as a new claim")
	}
	if firstPath != secondPath {
		t.Fatalf("retry path = %q, want %q", secondPath, firstPath)
	}
	assertManagerFileContents(t, filepath.Join(firstPath, usenetOwnerMarkerName), strings.ToLower(entry.InfoHash)+"\n")
}

func TestClaimUsenetEntryDirectoryAcceptsLegacyOwnerMarker(t *testing.T) {
	root := t.TempDir()
	entry := normalNZBEntry(root)
	path, newlyClaimed, err := claimUsenetEntryDirectory(root, entry)
	if err != nil {
		t.Fatal(err)
	}
	if !newlyClaimed {
		t.Fatal("initial NZB claim was not reported as new")
	}
	if err := os.Rename(
		filepath.Join(path, usenetOwnerMarkerName),
		filepath.Join(path, legacyUsenetOwnerMarkerName),
	); err != nil {
		t.Fatal(err)
	}

	retryPath, newlyClaimed, err := claimUsenetEntryDirectory(root, entry)
	if err != nil {
		t.Fatalf("legacy NZB owner marker was not accepted: %v", err)
	}
	if newlyClaimed || retryPath != path {
		t.Fatalf("legacy marker retry = (%q, %t), want (%q, false)", retryPath, newlyClaimed, path)
	}
	assertManagerFileContents(t, filepath.Join(path, legacyUsenetOwnerMarkerName), strings.ToLower(entry.InfoHash)+"\n")
	if _, err := os.Lstat(filepath.Join(path, usenetOwnerMarkerName)); !os.IsNotExist(err) {
		t.Fatalf("retry rewrote the legacy NZB marker: %v", err)
	}
}

func TestUsenetOwnerMarkerAliasesMustAgree(t *testing.T) {
	root := t.TempDir()
	entry := normalNZBEntry(root)
	path, _, err := claimUsenetEntryDirectory(root, entry)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, legacyUsenetOwnerMarkerName), []byte("different-owner\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := claimUsenetEntryDirectory(root, entry); err == nil || !strings.Contains(err.Error(), "markers disagree") {
		t.Fatalf("conflicting NZB ownership markers error = %v", err)
	}
}

func TestRollbackUsenetEntryClaimAcceptsLegacyMarkerOnlyDirectory(t *testing.T) {
	root := t.TempDir()
	entry := normalNZBEntry(root)
	path, _, err := claimUsenetEntryDirectory(root, entry)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(
		filepath.Join(path, usenetOwnerMarkerName),
		filepath.Join(path, legacyUsenetOwnerMarkerName),
	); err != nil {
		t.Fatal(err)
	}

	if err := rollbackUsenetEntryClaim(root, entry); err != nil {
		t.Fatalf("legacy marker-only rollback failed: %v", err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("legacy marker-only directory remained after rollback: %v", err)
	}
}

func TestClaimUsenetOwnerMarkerRemovesMarkerWhenFileSyncFails(t *testing.T) {
	rootPath := t.TempDir()
	rooted, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rooted.Close()

	previousSync := usenetSyncFile
	usenetSyncFile = func(*os.File) error {
		return errors.New("injected file sync failure")
	}
	t.Cleanup(func() {
		usenetSyncFile = previousSync
	})

	if _, err := claimUsenetOwnerMarker(rooted, "owner-id"); err == nil {
		t.Fatal("claimUsenetOwnerMarker() succeeded after file sync failure")
	}
	if _, err := rooted.Lstat(usenetOwnerMarkerName); !os.IsNotExist(err) {
		t.Fatalf("ownership marker survived failed file sync: %v", err)
	}
}

func TestClaimUsenetOwnerMarkerRemovesMarkerWhenDirectorySyncFails(t *testing.T) {
	rootPath := t.TempDir()
	rooted, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rooted.Close()

	previousSync := usenetSyncDirectory
	usenetSyncDirectory = func(*os.Root) error {
		return errors.New("injected directory sync failure")
	}
	t.Cleanup(func() {
		usenetSyncDirectory = previousSync
	})

	if _, err := claimUsenetOwnerMarker(rooted, "owner-id"); err == nil {
		t.Fatal("claimUsenetOwnerMarker() succeeded after directory sync failure")
	}
	if _, err := rooted.Lstat(usenetOwnerMarkerName); !os.IsNotExist(err) {
		t.Fatalf("ownership marker survived failed directory sync: %v", err)
	}
}

func TestClaimUsenetEntryDirectoryRejectsDifferentOwnerWithoutTouchingFirst(t *testing.T) {
	root := t.TempDir()
	first := normalNZBEntry(root)
	first.InfoHash = "first-owner"
	firstPath, _, err := claimUsenetEntryDirectory(root, first)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(firstPath, "first-owner.mkv")
	if err := os.WriteFile(sentinel, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}

	second := normalNZBEntry(root)
	second.InfoHash = "second-owner"
	if _, _, err := claimUsenetEntryDirectory(root, second); err == nil {
		t.Fatal("different owner claimed the same visible release path")
	}
	assertManagerFileContents(t, sentinel, "first")
	assertManagerFileContents(t, filepath.Join(firstPath, usenetOwnerMarkerName), "first-owner\n")
}

func TestClaimUsenetEntryDirectoryRejectsPortableCaseAlias(t *testing.T) {
	root := t.TempDir()
	first := normalNZBEntry(root)
	first.InfoHash = "first-owner"
	firstPath, _, err := claimUsenetEntryDirectory(root, first)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(firstPath, "first-owner.mkv")
	if err := os.WriteFile(sentinel, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}

	second := normalNZBEntry(root)
	second.InfoHash = "second-owner"
	second.Name = strings.ToLower(first.Name)
	if _, _, err := claimUsenetEntryDirectory(root, second); err == nil {
		t.Fatal("different owner claimed a portable case alias")
	}
	assertManagerFileContents(t, sentinel, "first")
	if runtime.GOOS != "windows" {
		if _, err := os.Stat(second.DownloadPath()); !os.IsNotExist(err) {
			t.Fatalf("portable alias directory was created: %v", err)
		}
	}
}

func TestClaimUsenetEntryDirectoryRefusesNonEmptyUnownedDirectory(t *testing.T) {
	root := t.TempDir()
	entry := normalNZBEntry(root)
	if err := os.MkdirAll(entry.DownloadPath(), 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(entry.DownloadPath(), "pre-existing.mkv")
	if err := os.WriteFile(sentinel, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, _, err := claimUsenetEntryDirectory(root, entry); err == nil {
		t.Fatal("claimed a non-empty unowned release directory")
	}
	assertManagerFileContents(t, sentinel, "preserve")
	if _, err := os.Stat(filepath.Join(entry.DownloadPath(), usenetOwnerMarkerName)); !os.IsNotExist(err) {
		t.Fatalf("ownership marker was left behind: %v", err)
	}
}

func TestSafeUsenetFilePathRejectsOwnerMarkerAlias(t *testing.T) {
	root := t.TempDir()
	entry := normalNZBEntry(root)
	for _, reserved := range []string{
		strings.ToUpper(usenetOwnerMarkerName),
		strings.ToUpper(usenetOwnershipLockName),
		strings.ToUpper(usenetQuarantinePrefix) + "spoof",
	} {
		if _, err := safeUsenetFilePath(root, entry, reserved); err == nil {
			t.Fatalf("safeUsenetFilePath() accepted reserved name %q", reserved)
		}
	}
}

func TestClaimUsenetEntryDirectoryRejectsSpoofedQuarantine(t *testing.T) {
	root := t.TempDir()
	entry := normalNZBEntry(root)
	ownerID, err := canonicalUsenetOwnerID(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(root, entry.DownloadPath())
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Dir(entry.DownloadPath())
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	spoof := filepath.Join(parent, usenetQuarantinePrefixForEntry(ownerID, relative)+"spoof")
	if err := os.Mkdir(spoof, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(spoof, usenetOwnerMarkerName), []byte("wrong-owner\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(spoof, "keep")
	if err := os.WriteFile(sentinel, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, _, err := claimUsenetEntryDirectory(root, entry); err == nil {
		t.Fatal("claimUsenetEntryDirectory() accepted a spoofed quarantine")
	}
	assertManagerFileContents(t, sentinel, "preserve")
	if _, err := os.Stat(entry.DownloadPath()); !os.IsNotExist(err) {
		t.Fatalf("spoofed quarantine was moved into visible path: %v", err)
	}
}

func TestRemoveOwnedUsenetEntryRefusesVisibleAndQuarantineAmbiguity(t *testing.T) {
	root := t.TempDir()
	entry := normalNZBEntry(root)
	entryPath, _, err := claimUsenetEntryDirectory(root, entry)
	if err != nil {
		t.Fatal(err)
	}
	ownerID, err := canonicalUsenetOwnerID(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(root, entryPath)
	if err != nil {
		t.Fatal(err)
	}
	quarantine := filepath.Join(filepath.Dir(entryPath), usenetQuarantinePrefixForEntry(ownerID, relative)+"crash")
	if err := os.Rename(entryPath, quarantine); err != nil {
		t.Fatal(err)
	}
	quarantineSentinel := filepath.Join(quarantine, "original")
	if err := os.WriteFile(quarantineSentinel, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(entryPath, 0o700); err != nil {
		t.Fatal(err)
	}
	replacementSentinel := filepath.Join(entryPath, "replacement")
	if err := os.WriteFile(replacementSentinel, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := removeOwnedUsenetEntryDirectory(root, entry); err == nil {
		t.Fatal("removeOwnedUsenetEntryDirectory() accepted ambiguous replacement state")
	}
	assertManagerFileContents(t, quarantineSentinel, "original")
	assertManagerFileContents(t, replacementSentinel, "replacement")
}

func TestRemoveOwnedUsenetEntryCleansMatchingCrashQuarantine(t *testing.T) {
	root := t.TempDir()
	entry := normalNZBEntry(root)
	entryPath, _, err := claimUsenetEntryDirectory(root, entry)
	if err != nil {
		t.Fatal(err)
	}
	ownerID, err := canonicalUsenetOwnerID(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(root, entryPath)
	if err != nil {
		t.Fatal(err)
	}
	quarantine := filepath.Join(filepath.Dir(entryPath), usenetQuarantinePrefixForEntry(ownerID, relative)+"crash")
	if err := os.Rename(entryPath, quarantine); err != nil {
		t.Fatal(err)
	}

	if err := removeOwnedUsenetEntryDirectory(root, entry); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(quarantine); !os.IsNotExist(err) {
		t.Fatalf("matching crash quarantine still exists: %v", err)
	}
}

func TestRemoveOwnedUsenetEntryCleansLegacyCrashQuarantine(t *testing.T) {
	root := t.TempDir()
	entry := normalNZBEntry(root)
	entryPath, _, err := claimUsenetEntryDirectory(root, entry)
	if err != nil {
		t.Fatal(err)
	}
	ownerID, err := canonicalUsenetOwnerID(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(root, entryPath)
	if err != nil {
		t.Fatal(err)
	}
	legacyQuarantine := filepath.Join(
		filepath.Dir(entryPath),
		legacyUsenetQuarantinePrefix+strings.TrimPrefix(
			usenetQuarantinePrefixForEntry(ownerID, relative),
			usenetQuarantinePrefix,
		)+"crash",
	)
	if err := os.Rename(entryPath, legacyQuarantine); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(
		filepath.Join(legacyQuarantine, usenetOwnerMarkerName),
		filepath.Join(legacyQuarantine, legacyUsenetOwnerMarkerName),
	); err != nil {
		t.Fatal(err)
	}

	if err := removeOwnedUsenetEntryDirectory(root, entry); err != nil {
		t.Fatalf("legacy NZB quarantine recovery failed: %v", err)
	}
	if _, err := os.Lstat(legacyQuarantine); !os.IsNotExist(err) {
		t.Fatalf("legacy NZB quarantine remained: %v", err)
	}
}

func TestUsenetOwnershipLockTimesOutUnderContention(t *testing.T) {
	root := t.TempDir()
	first, _, err := acquireUsenetOwnershipLock(root, true)
	if err != nil {
		t.Fatal(err)
	}
	defer first.close()

	previousTimeout := usenetOwnershipLockTimeout
	previousDelay := usenetOwnershipRetryDelay
	usenetOwnershipLockTimeout = 75 * time.Millisecond
	usenetOwnershipRetryDelay = 5 * time.Millisecond
	t.Cleanup(func() {
		usenetOwnershipLockTimeout = previousTimeout
		usenetOwnershipRetryDelay = previousDelay
	})
	started := time.Now()
	if second, _, err := acquireUsenetOwnershipLock(root, true); err == nil {
		_ = second.close()
		t.Fatal("second NZB ownership lock unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("NZB ownership lock timeout took %s", elapsed)
	}
}

func TestProcessTorrentDownloadRejectsPathOutsideConfiguredRoot(t *testing.T) {
	root := t.TempDir()
	entry := &storage.Entry{
		Protocol: config.ProtocolTorrent,
		InfoHash: "outside-configured-root",
		Name:     "[Group] Movie [1080p]",
		SavePath: filepath.Join(root, "radarr"),
		Action:   config.DownloadActionDownload,
		Files:    map[string]*storage.File{},
	}
	downloader := &Downloader{
		dest:   filepath.Join(root, "different-configured-root"),
		logger: zerolog.Nop(),
	}

	err := downloader.processTorrentDownload(context.Background(), entry)
	if err == nil || !strings.Contains(err.Error(), "outside configured root") {
		t.Fatalf("processTorrentDownload() error = %v, want outside-root rejection", err)
	}
	if _, err := os.Stat(entry.DownloadPath()); !os.IsNotExist(err) {
		t.Fatalf("unsafe torrent download path was created: %v", err)
	}
}

func TestDeleteEntryFilesPropagatesTorrentRemoveFailure(t *testing.T) {
	root := t.TempDir()
	entry := &storage.Entry{
		Protocol: config.ProtocolTorrent,
		Name:     "release",
		SavePath: filepath.Join(root, "invalid\x00path"),
	}
	if err := (&Queue{}).deleteEntryFiles(entry); err == nil {
		t.Fatal("deleteEntryFiles() swallowed torrent removal failure")
	}
}

func TestWriteStrmFileRejectsUnownedTorrentPath(t *testing.T) {
	root := t.TempDir()
	outsideConfiguredRoot := t.TempDir()
	target := filepath.Join(root, "movie.strm")
	entry := &storage.Entry{Protocol: config.ProtocolTorrent}
	downloader := &Downloader{dest: outsideConfiguredRoot}

	if err := downloader.writeStrmFile(entry, target, "http://example.invalid/movie"); err == nil {
		t.Fatal("writeStrmFile() accepted an unowned torrent path")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("unowned torrent path was created: %v", err)
	}
}

func TestWriteStrmFileDoesNotTruncateOutsideHardLinkForNZB(t *testing.T) {
	root := t.TempDir()
	entry := normalNZBEntry(root)
	downloader := &Downloader{dest: root}
	entryPath, err := downloader.prepareUsenetDownloadDirectory(entry)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(entryPath, "episode.strm")
	outside := filepath.Join(root, "outside-sentinel")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(outside, target); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}

	if err := downloader.writeStrmFile(entry, target, "http://example.invalid/episode"); err != nil {
		t.Fatal(err)
	}
	assertManagerFileContents(t, outside, "outside")
	assertManagerFileContents(t, target, "http://example.invalid/episode")
}

func TestDeleteNZBEntryFilesRemovesOnlyContainedPaths(t *testing.T) {
	downloadRoot := t.TempDir()
	metadataRoot := t.TempDir()
	entry := normalNZBEntry(downloadRoot)
	entryPath, _, err := claimUsenetEntryDirectory(downloadRoot, entry)
	if err != nil {
		t.Fatal(err)
	}
	downloadedFile := filepath.Join(entryPath, "episode.mkv")
	if err := os.WriteFile(downloadedFile, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	stagedFile := filepath.Join(metadataRoot, entry.InfoHash+".queued")
	if err := os.WriteFile(stagedFile, []byte("nzb"), 0o600); err != nil {
		t.Fatal(err)
	}
	entry.Magnet = stagedFile

	queue := &Queue{}
	if err := queue.deleteNZBEntryFiles(downloadRoot, metadataRoot, entry); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(entryPath); !os.IsNotExist(err) {
		t.Fatalf("download path still exists: %v", err)
	}
	if _, err := os.Stat(stagedFile); !os.IsNotExist(err) {
		t.Fatalf("staged NZB still exists: %v", err)
	}
}

func TestDeleteNZBEntryFilesRefusesWrongOwner(t *testing.T) {
	downloadRoot := t.TempDir()
	metadataRoot := t.TempDir()
	owner := normalNZBEntry(downloadRoot)
	owner.InfoHash = "actual-owner"
	entryPath, _, err := claimUsenetEntryDirectory(downloadRoot, owner)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(entryPath, "keep.mkv")
	if err := os.WriteFile(sentinel, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}

	wrongOwner := normalNZBEntry(downloadRoot)
	wrongOwner.InfoHash = "wrong-owner"
	if err := (&Queue{}).deleteNZBEntryFiles(downloadRoot, metadataRoot, wrongOwner); err == nil {
		t.Fatal("deleteNZBEntryFiles() accepted a wrong owner")
	}
	assertManagerFileContents(t, sentinel, "preserve")
}

func TestDeleteNZBEntryFilesRefusesMissingOwnerMarker(t *testing.T) {
	downloadRoot := t.TempDir()
	metadataRoot := t.TempDir()
	entry := normalNZBEntry(downloadRoot)
	if err := os.MkdirAll(entry.DownloadPath(), 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(entry.DownloadPath(), "keep.mkv")
	if err := os.WriteFile(sentinel, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := (&Queue{}).deleteNZBEntryFiles(downloadRoot, metadataRoot, entry); err == nil {
		t.Fatal("deleteNZBEntryFiles() accepted a missing owner marker")
	}
	assertManagerFileContents(t, sentinel, "preserve")
}

func TestDeleteNZBEntryFilesAllowsMissingReleaseDirectory(t *testing.T) {
	downloadRoot := t.TempDir()
	metadataRoot := t.TempDir()
	entry := normalNZBEntry(downloadRoot)
	stagedFile := filepath.Join(metadataRoot, entry.InfoHash+".queued")
	if err := os.WriteFile(stagedFile, []byte("nzb"), 0o600); err != nil {
		t.Fatal(err)
	}
	entry.Magnet = stagedFile

	if err := (&Queue{}).deleteNZBEntryFiles(downloadRoot, metadataRoot, entry); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stagedFile); !os.IsNotExist(err) {
		t.Fatalf("staged NZB still exists: %v", err)
	}
}

func TestDeleteNZBEntryFilesFailsClosedOnUnsafePath(t *testing.T) {
	downloadRoot := t.TempDir()
	metadataRoot := t.TempDir()
	outside := t.TempDir()
	outsideSentinel := filepath.Join(outside, "keep")
	if err := os.WriteFile(outsideSentinel, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	stagedFile := filepath.Join(metadataRoot, "queued.nzb")
	if err := os.WriteFile(stagedFile, []byte("nzb"), 0o600); err != nil {
		t.Fatal(err)
	}

	entry := normalNZBEntry(downloadRoot)
	entry.SavePath = outside
	entry.Magnet = stagedFile
	if err := (&Queue{}).deleteNZBEntryFiles(downloadRoot, metadataRoot, entry); err == nil {
		t.Fatal("deleteNZBEntryFiles() accepted an outside SavePath")
	}
	assertManagerFileContents(t, outsideSentinel, "outside")
	assertManagerFileContents(t, stagedFile, "nzb")
}

func TestDeleteNZBEntryFilesRejectsSymlinkTarget(t *testing.T) {
	downloadRoot := t.TempDir()
	metadataRoot := t.TempDir()
	outside := t.TempDir()
	outsideSentinel := filepath.Join(outside, "keep")
	if err := os.WriteFile(outsideSentinel, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}

	categoryPath := filepath.Join(downloadRoot, "sonarr")
	if err := os.Mkdir(categoryPath, 0o700); err != nil {
		t.Fatal(err)
	}
	entry := normalNZBEntry(downloadRoot)
	if err := os.Symlink(outside, entry.DownloadPath()); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := (&Queue{}).deleteNZBEntryFiles(downloadRoot, metadataRoot, entry); err == nil {
		t.Fatal("deleteNZBEntryFiles() accepted a symlink target")
	}
	assertManagerFileContents(t, outsideSentinel, "outside")
}

func normalNZBEntry(root string) *storage.Entry {
	return &storage.Entry{
		Protocol: config.ProtocolNZB,
		InfoHash: "33333333-3333-4333-8333-333333333333",
		Name:     "[Group] Show S01E01 [1080p].mkv",
		Category: "sonarr",
		SavePath: filepath.Join(root, "sonarr"),
		Files: map[string]*storage.File{
			"Show S01E01.mkv": {
				Name: "Show S01E01.mkv",
			},
		},
	}
}

func assertManagerFileContents(t *testing.T, path, want string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	if string(contents) != want {
		t.Fatalf("%q contents = %q, want %q", path, contents, want)
	}
}

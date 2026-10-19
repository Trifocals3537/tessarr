package manager

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestTorrentOutputPathMountSourceRemainsPortable(t *testing.T) {
	for _, naming := range []config.WebDavFolderNaming{
		"", config.WebDavUseFileName, config.WebDavUseOriginalName,
		config.WebDavUseFileNameNoExt, config.WebDavUseOriginalNameNoExt,
	} {
		t.Run(string(naming), func(t *testing.T) {
			for _, name := range []string{"Movie: Part Two", "CON", "movie?", "movie<", "movie>", "movie\"", "movie|", "movie*", "trailing.", "trailing ", "a/../movie", `a\movie`} {
				entry := &storage.Entry{
					Name: name, OriginalFilename: name, InfoHash: "safe-hash",
					OutputName: storage.NewTorrentOutputName(name, "safe-hash"),
				}
				for _, action := range []config.DownloadAction{config.DownloadActionSymlink, "", "unknown"} {
					entry.Action = action
					if err := validateTorrentSourceFolder(entry, naming, false); err == nil {
						t.Errorf("accepted unsafe %q mount source %q with action %q", naming, name, action)
					}
				}
			}
			entry := &storage.Entry{Name: "Movie.mkv", OriginalFilename: "Original.mkv", Action: config.DownloadActionSymlink}
			if err := validateTorrentSourceFolder(entry, naming, false); err != nil {
				t.Fatalf("portable source rejected: %v", err)
			}
		})
	}
	entry := &storage.Entry{Name: "Movie: Part Two", OriginalFilename: "Original?", InfoHash: "safe-hash", Action: config.DownloadActionSymlink}
	if err := validateTorrentSourceFolder(entry, config.WebdavUseHash, false); err != nil {
		t.Fatalf("safe hash source rejected because of unused titles: %v", err)
	}
	entry.InfoHash = "../escape"
	if err := validateTorrentSourceFolder(entry, config.WebdavUseHash, false); err == nil {
		t.Fatal("unsafe hash source accepted")
	}
	for _, action := range []config.DownloadAction{config.DownloadActionDownload, config.DownloadActionStrm, config.DownloadActionNone} {
		entry.Action = action
		if err := validateTorrentSourceFolder(entry, config.WebDavUseFileName, false); err != nil {
			t.Fatalf("mount-free action %q rejected a display title: %v", action, err)
		}
	}
}

func TestTorrentOutputPathValidatesSourceAtAdmissionAndProviderUpdate(t *testing.T) {
	request := asyncAdmissionTestRequest(t.TempDir(), "source-check", nil)
	request.Action = config.DownloadActionSymlink
	request.Magnet.Name = "Movie: Part Two"
	manager := &Manager{config: &config.Config{DownloadFolder: request.DownloadFolder}}
	if err := manager.validateTorrentImportRequest(request); err == nil {
		t.Fatal("unsafe title-based symlink source admitted")
	}
	request.Action = config.DownloadActionDownload
	if err := manager.validateTorrentImportRequest(request); err != nil {
		t.Fatalf("mount-free download rejected: %v", err)
	}
	request.Action = config.DownloadActionSymlink
	request.Magnet.Name = ""
	if err := manager.validateTorrentImportRequest(request); err != nil {
		t.Fatalf("nameless magnet rejected before provider resolution: %v", err)
	}
	resolved := &debridTypes.Torrent{Name: "Movie: Part Two", OriginalFilename: "Original.mkv", InfoHash: "safe-hash"}
	if err := manager.validateResolvedTorrentNames(resolved, request.Action); err == nil {
		t.Fatal("unsafe provider-updated symlink source accepted")
	}
	manager.config.FolderNaming = config.WebDavUseOriginalName
	if err := manager.validateResolvedTorrentNames(resolved, request.Action); err != nil {
		t.Fatalf("portable selected original filename rejected: %v", err)
	}
	resolved.Name, resolved.OriginalFilename = "Movie.mkv", "CON"
	if err := manager.validateResolvedTorrentNames(resolved, request.Action); err == nil {
		t.Fatal("unsafe provider original filename accepted")
	}
	resolved.OriginalFilename = ""
	if err := manager.validateResolvedTorrentNames(resolved, request.Action); err == nil {
		t.Fatal("missing resolved source accepted")
	}
	manager.config.FolderNaming = config.WebdavUseHash
	if err := manager.validateResolvedTorrentNames(resolved, request.Action); err != nil {
		t.Fatalf("hash source incorrectly required an original filename: %v", err)
	}
}

func TestTorrentOutputPathRechecksSourceBeforeLocalWork(t *testing.T) {
	entry := torrentOwnershipTestEntry(t.TempDir(), "../unsafe-hash", config.DownloadActionSymlink)
	entry.Name, entry.OriginalFilename = "CON", "CON"
	entry.OutputName = storage.NewTorrentOutputName(entry.Name, entry.InfoHash)
	// No manager/queue is needed: rejection must happen before any queue or
	// filesystem side effects, for every current mount-naming policy.
	downloader := &Downloader{}
	if err := downloader.download(context.Background(), entry); err == nil {
		t.Fatal("unsafe persisted source reached local processing")
	}
	if entry.IsDownloading {
		t.Fatal("source validation changed queue state")
	}
}

func TestTorrentOutputPathPreservesLegacyLeadingWhitespace(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "legacy-spaces", config.DownloadActionDownload)
	entry.Name = " Release.mkv"
	want := filepath.Join(entry.SavePath, " Release")
	got, err := safeTorrentEntryDownloadPath(root, entry)
	if err != nil {
		t.Fatal(err)
	}
	if got != want || got != entry.DownloadPath() {
		t.Fatalf("legacy path = %q, want unchanged %q", got, want)
	}
	if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
		t.Fatal(err)
	}
	if err := writeOwnedTorrentFile(root, entry, "movie.mkv", []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	assertTorrentTestContents(t, filepath.Join(want, "movie.mkv"), "data")
	if err := removeOwnedTorrentEntryDirectory(root, entry); err != nil {
		t.Fatal(err)
	}
}

func TestTorrentOutputPathAdmissionKeepsDisplayTitleAndStableProviderIdentity(t *testing.T) {
	request := asyncAdmissionTestRequest(t.TempDir(), "first", nil)
	request.Magnet.Name = " Movie: Part Two "
	entry := newTorrentQueueEntry(request, debridTypes.TorrentStatusQueued)
	if entry.OutputName == "" || entry.Name != request.Magnet.Name || entry.ContentPath != entry.DownloadPath() {
		t.Fatal("admission did not preserve title and assign output identity")
	}
	outputPath := entry.DownloadPath()
	remote := &debridTypes.Torrent{Id: "provider-id", Debrid: "torbox", Name: "Provider: Renamed", InfoHash: entry.InfoHash}
	if !entry.CanApplyTorrentTitle(remote) {
		t.Fatalf("newly admitted queue entry cannot accept provider title: save_path=%q completed=%v downloading=%t size_downloaded=%d files=%d", entry.SavePath, entry.CompletedAt, entry.IsDownloading, entry.SizeDownloaded, len(entry.Files))
	}
	if err := applyDebridTorrentState(entry, remote); err != nil {
		t.Fatal(err)
	}
	if entry.Name != "Provider: Renamed" || entry.DownloadPath() != outputPath {
		t.Fatal("provider result changed local output instead of only the display title")
	}
}

func TestTorrentOutputPathRestartKeepsPartAndExactCleanupTarget(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "stable-restart", config.DownloadActionDownload)
	entry.Name = "Movie: Part Two"
	entry.OutputName = storage.NewTorrentOutputName(entry.Name, entry.InfoHash)
	if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
		t.Fatal(err)
	}
	originalPath := entry.DownloadPath()
	part, err := openOwnedTorrentPart(root, entry, "movie.mkv", 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.file.Write([]byte("ab")); err != nil {
		t.Fatal(err)
	}
	if err := part.Close(); err != nil {
		t.Fatal(err)
	}
	store := newLifecycleTestStorage(t)
	if err := store.AddQueue(entry); err != nil {
		t.Fatal(err)
	}
	entry, err = store.GetQueued(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	entry.Name = "A new provider title"
	part, err = openOwnedTorrentPart(root, entry, "movie.mkv", 4)
	if err != nil {
		t.Fatal(err)
	}
	defer part.Close()
	if size, err := part.Size(); err != nil || size != 2 {
		t.Fatalf("recovered partial size = %d, err=%v", size, err)
	}
	if _, err := part.file.WriteAt([]byte("cd"), 2); err != nil {
		t.Fatal(err)
	}
	if err := part.Commit(); err != nil {
		t.Fatal(err)
	}
	assertTorrentTestContents(t, filepath.Join(originalPath, "movie.mkv"), "abcd")
	foreign := *entry
	foreign.InfoHash = "different-owner"
	if err := removeOwnedTorrentEntryDirectory(root, &foreign); err == nil {
		t.Fatal("stored output name bypassed the ownership check")
	}
	assertTorrentTestContents(t, filepath.Join(originalPath, "movie.mkv"), "abcd")
	if err := removeOwnedTorrentEntryDirectory(root, entry); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(originalPath); !os.IsNotExist(err) {
		t.Fatalf("cleanup did not remove the original output: %v", err)
	}
}

func TestTorrentOutputPathSymlinkAndStrmUseStoredRoot(t *testing.T) {
	for _, action := range []config.DownloadAction{config.DownloadActionSymlink, config.DownloadActionStrm} {
		t.Run(string(action), func(t *testing.T) {
			if action == config.DownloadActionSymlink && runtime.GOOS == "windows" {
				t.Skip("symlink creation may require privileges")
			}
			root := t.TempDir()
			entry := torrentOwnershipTestEntry(root, "stable-links", action)
			entry.Name = "Movie: Part Two"
			entry.OutputName = storage.NewTorrentOutputName(entry.Name, entry.InfoHash)
			if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
				t.Fatal(err)
			}
			mountFile := filepath.Join(t.TempDir(), "movie.mkv")
			if err := os.WriteFile(mountFile, []byte("media"), 0o600); err != nil {
				t.Fatal(err)
			}
			if action == config.DownloadActionSymlink {
				link, err := createOwnedTorrentSymlink(root, entry, "movie.mkv", mountFile, false)
				if err != nil {
					t.Fatal(err)
				}
				if link != filepath.Join(entry.DownloadPath(), "movie.mkv") {
					t.Fatal("symlink used a title-derived output root")
				}
				assertTorrentTestContents(t, link, "media")
			} else {
				if err := writeOwnedTorrentFile(root, entry, "movie.strm", []byte("https://example.invalid/stream\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				assertTorrentTestContents(t, filepath.Join(entry.DownloadPath(), "movie.strm"), "https://example.invalid/stream\n")
			}
			if err := removeOwnedTorrentEntryDirectory(root, entry); err != nil {
				t.Fatal(err)
			}
			assertTorrentTestContents(t, mountFile, "media")
		})
	}
}

func TestTorrentOutputPathRejectsUnsafePersistedComponents(t *testing.T) {
	root := t.TempDir()
	for _, component := range []string{"../escape", `/absolute`, `C:\escape`, "CON", "name:stream", "bad\x00name", "trailing ", torrentOwnerMarkerName, torrentPartialPrefix + "fake"} {
		entry := torrentOwnershipTestEntry(root, "unsafe-output", config.DownloadActionDownload)
		entry.OutputName = component
		if _, err := safeTorrentEntryDownloadPath(root, entry); err == nil {
			t.Errorf("accepted unsafe persisted component %q", component)
		}
	}
	entry := torrentOwnershipTestEntry(root, "unsafe-title", config.DownloadActionSymlink)
	entry.OutputName = storage.NewTorrentOutputName("safe", entry.InfoHash)
	entry.Name = "../../outside"
	if _, err := safeTorrentEntryDownloadPath(root, entry); err == nil {
		t.Fatal("safe output identity allowed an unsafe mount/source title")
	}
}

func TestTorrentOutputPathStripsOnlyRecognizedDisplayRoot(t *testing.T) {
	entry := torrentOwnershipTestEntry(t.TempDir(), "provider-root", config.DownloadActionSymlink)
	entry.Name = "Movie: Part Two"
	entry.OutputName = storage.NewTorrentOutputName(entry.Name, entry.InfoHash)
	for _, path := range []string{"Movie: Part Two/Season 01/movie.mkv", `Movie: Part Two\Season 01\movie.mkv`, " Movie: Part Two /Season 01/movie.mkv"} {
		entry.Files = map[string]*storage.File{"file": {Name: path, Path: path, Size: 4}}
		layouts, err := torrentEntryFileLayouts(entry)
		if err != nil || len(layouts) != 1 || layouts[0].relative != filepath.Join("Season 01", "movie.mkv") {
			t.Fatalf("display-root layout = %v, err=%v", layouts, err)
		}
	}
	for _, path := range []string{"Other: Root/movie.mkv", "Movie: Part Two/../escape.mkv", "Movie: Part Two/CON/movie.mkv", "Movie: Part Two/movie:stream.mkv", "/Movie: Part Two/movie.mkv"} {
		entry.Files = map[string]*storage.File{"file": {Name: "movie.mkv", Path: path, Size: 4}}
		if _, err := torrentEntryFileLayouts(entry); err == nil {
			t.Errorf("accepted unsafe nested path %q", path)
		}
	}
	entry.Name = ".."
	entry.Files = map[string]*storage.File{"file": {Name: "movie.mkv", Path: "../movie.mkv", Size: 4}}
	if _, err := torrentEntryFileLayouts(entry); err == nil {
		t.Fatal("untrusted entry title authorized stripping traversal")
	}
}

func TestMaterializedOutputReservationUsesStrippedReleaseRoot(t *testing.T) {
	for _, newPath := range []string{"Movie.mkv", "Release/Movie.mkv"} {
		t.Run(newPath, func(t *testing.T) {
			completed := time.Now()
			entry := &storage.Entry{
				Protocol: config.ProtocolTorrent, InfoHash: "root-conflict", Name: "Release",
				OutputName:  storage.NewTorrentOutputName("Release", "root-conflict"),
				CompletedAt: &completed, ActiveProvider: "primary",
				Files: map[string]*storage.File{
					"extra.srt": {Name: "extra.srt", Path: "Release/movie.mkv/extra.srt", Size: 1},
				},
				Providers: map[string]*storage.ProviderEntry{
					"primary": {Provider: "primary", Files: map[string]*storage.ProviderFile{
						"extra.srt": {Id: "existing", Path: "Release/movie.mkv/extra.srt"},
					}},
				},
			}
			remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
				{Id: "existing", Path: "Release/movie.mkv/extra.srt", Size: 1},
				{Id: "new", Path: newPath, Size: 2},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "primary", Files: remoteFiles}); err != nil {
				t.Fatal(err)
			}
			layouts, err := torrentEntryFileLayouts(entry)
			if err != nil {
				t.Fatalf("provider refresh created an unusable layout: %v", err)
			}
			if len(layouts) != 2 {
				t.Fatalf("layout count = %d, want 2", len(layouts))
			}
			var existing, added string
			for _, layout := range layouts {
				if layout.file.Size == 1 {
					existing = filepath.ToSlash(layout.relative)
				} else {
					added = filepath.ToSlash(layout.relative)
				}
			}
			if existing != "movie.mkv/extra.srt" || added == "" || strings.EqualFold(added, "movie.mkv") ||
				strings.HasPrefix(existing, strings.ToLower(added)+"/") || filepath.Ext(added) != ".mkv" {
				t.Fatalf("existing=%q added=%q, want stable nested file and distinct .mkv", existing, added)
			}
		})
	}
}

func TestMaterializedOutputReservationPreservesReleaseRootAcrossTitleChange(t *testing.T) {
	completed := time.Now()
	entry := &storage.Entry{
		Protocol: config.ProtocolTorrent, InfoHash: "root-rename", Name: "Old",
		OutputName:  storage.NewTorrentOutputName("Old", "root-rename"),
		CompletedAt: &completed, ActiveProvider: "primary",
		Files: map[string]*storage.File{
			"extra.srt": {Name: "extra.srt", Path: "Release/movie.mkv/extra.srt", Size: 1},
		},
		Providers: map[string]*storage.ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*storage.ProviderFile{
				"extra.srt": {Id: "existing", Path: "Release/movie.mkv/extra.srt"},
			}},
		},
	}
	before, err := torrentEntryFileLayouts(entry)
	if err != nil || len(before) != 1 {
		t.Fatalf("initial materialized layout = %v, err = %v", before, err)
	}
	oldRelative := before[0].relative
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "existing", Path: "Release/movie.mkv/extra.srt", Size: 1},
		{Id: "new", Path: "Movie.mkv", Size: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := applyDebridTorrentState(entry, &debridTypes.Torrent{
		Debrid: "primary", Name: "Release", Files: remoteFiles,
	}); err != nil {
		t.Fatal(err)
	}
	if entry.Name != "Old" {
		t.Fatalf("provider title reinterpreted a materialized output path: %q", entry.Name)
	}
	layouts, err := torrentEntryFileLayouts(entry)
	if err != nil {
		t.Fatalf("incoming title made the reserved layout invalid: %v", err)
	}
	if len(layouts) != 2 || entry.Files["extra.srt"].Path != "Release/movie.mkv/extra.srt" {
		t.Fatalf("materialized layout changed: %#v", entry.Files)
	}
	for _, layout := range layouts {
		if layout.file == entry.Files["extra.srt"] && layout.relative != oldRelative {
			t.Fatalf("materialized path moved from %q to %q", oldRelative, layout.relative)
		}
	}
}

func TestMaterializedOutputKeepsWebDAVFolderAcrossProviderRename(t *testing.T) {
	completed := time.Now()
	entry := &storage.Entry{
		Protocol: config.ProtocolTorrent, InfoHash: "strm-title", Name: "Old.mkv",
		OriginalFilename: "Original.mkv",
		OutputName:       storage.NewTorrentOutputName("Old.mkv", "strm-title"),
		CompletedAt:      &completed,
		ActiveProvider:   "primary",
		Files: map[string]*storage.File{
			"movie.mkv": {Name: "movie.mkv", Path: "movie.mkv", Size: 1},
		},
		Providers: map[string]*storage.ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*storage.ProviderFile{
				"movie.mkv": {Id: "existing", Path: "movie.mkv"},
			}},
		},
	}
	namingModes := []config.WebDavFolderNaming{
		config.WebDavUseFileName, config.WebDavUseOriginalName,
		config.WebDavUseFileNameNoExt, config.WebDavUseOriginalNameNoExt,
		config.WebdavUseHash,
	}
	before := make(map[config.WebDavFolderNaming]string, len(namingModes))
	for _, mode := range namingModes {
		before[mode] = storage.GetTorrentFolder(mode, entry)
	}
	remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id: "existing", Path: "movie.mkv", Size: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := applyDebridTorrentState(entry, &debridTypes.Torrent{
		Id: "transfer", Debrid: "primary", Name: "New.mkv",
		OriginalFilename: "New Original.mkv", Files: remoteFiles,
	}); err != nil {
		t.Fatal(err)
	}
	if entry.Name != "Old.mkv" || entry.OriginalFilename != "Original.mkv" {
		t.Fatalf("provider rename changed exposed folder identity: name=%q original=%q", entry.Name, entry.OriginalFilename)
	}
	for _, mode := range namingModes {
		if got := storage.GetTorrentFolder(mode, entry); got != before[mode] {
			t.Errorf("WebDAV folder mode %q changed from %q to %q", mode, before[mode], got)
		}
	}
}

func TestLegacyUnknownSavePathKeepsTitleAcrossProviderRename(t *testing.T) {
	for _, tc := range []struct{ name, savePath string }{
		{name: "empty"},
		{name: "relative", savePath: "relative-legacy-root"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := &storage.Entry{
				Protocol: config.ProtocolTorrent, InfoHash: "legacy-title", Name: "Old",
				OriginalFilename: "Old Original", SavePath: tc.savePath,
				ActiveProvider: "primary",
				Files: map[string]*storage.File{
					"movie.mkv": {Name: "movie.mkv", Path: "Release/movie.mkv", Size: 1},
				},
				Providers: map[string]*storage.ProviderEntry{
					"primary": {Provider: "primary", Files: map[string]*storage.ProviderFile{
						"movie.mkv": {Id: "existing", Path: "Release/movie.mkv"},
					}},
				},
			}
			remoteFiles, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
				Id: "existing", Path: "Release/movie.mkv", Size: 1,
			}})
			if err != nil {
				t.Fatal(err)
			}
			if err := applyDebridTorrentState(entry, &debridTypes.Torrent{
				Id: "transfer", Debrid: "primary", Name: "Release",
				OriginalFilename: "Release Original", Files: remoteFiles,
			}); err != nil {
				t.Fatal(err)
			}
			if entry.Name != "Old" || entry.OriginalFilename != "Old Original" ||
				entry.Files["movie.mkv"].Path != "Release/movie.mkv" {
				t.Fatalf("unknown-root legacy identity changed: %#v", entry)
			}
		})
	}
}

func TestMaterializedDirectoryOwnerIsNotMergedWithLiteralGeneratedName(t *testing.T) {
	initial, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id: "old", Path: "A?/old.mkv", Size: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	old := initial["old.mkv"]
	oldPath := filepath.ToSlash(old.LocalPath())
	oldDirectory := strings.Split(oldPath, "/")[0]
	if oldDirectory == "A?" || !strings.Contains(oldDirectory, "~") {
		t.Fatalf("test does not have a generated lossy directory: %q", oldPath)
	}
	completed := time.Now()
	entry := &storage.Entry{
		Protocol: config.ProtocolTorrent, InfoHash: "directory-owner", Name: "Release",
		OutputName:  storage.NewTorrentOutputName("Release", "directory-owner"),
		CompletedAt: &completed, ActiveProvider: "primary",
		Files: map[string]*storage.File{
			old.Name: {Name: old.Name, Path: old.LocalPath(), Size: old.Size},
		},
		Providers: map[string]*storage.ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*storage.ProviderFile{
				old.Name: {Id: old.Id, Path: old.Path},
			}},
		},
	}
	expanded, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "old", Path: "A?/old.mkv", Size: 1},
		{Id: "new", Path: oldDirectory + "/new.mkv", Size: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "primary", Files: expanded}); err != nil {
		t.Fatal(err)
	}
	if got := filepath.ToSlash(entry.Files[old.Name].Path); got != oldPath {
		t.Fatalf("materialized directory moved from %q to %q", oldPath, got)
	}
	var newPath string
	for _, file := range entry.Files {
		if file.Size == 2 {
			newPath = filepath.ToSlash(file.Path)
		}
	}
	if newPath == "" || strings.HasPrefix(strings.ToLower(newPath), strings.ToLower(oldDirectory)+"/") {
		t.Fatalf("new literal directory merged into existing native directory: %q", newPath)
	}
	if _, err := torrentEntryFileLayouts(entry); err != nil {
		t.Fatalf("directory-owner repair created invalid final layout: %v", err)
	}
}

func TestMaterializedLossyReleaseRootKeepsNativeDirectoryOwner(t *testing.T) {
	initial, err := debridTypes.FilesByLogicalName([]debridTypes.File{{
		Id: "old", Path: "Release?/old.mkv", Size: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	old := initial["old.mkv"]
	oldDirectory := strings.Split(filepath.ToSlash(old.LocalPath()), "/")[0]
	completed := time.Now()
	entry := &storage.Entry{
		Protocol: config.ProtocolTorrent, InfoHash: "lossy-root", Name: "Release?",
		OutputName:  storage.NewTorrentOutputName("Release?", "lossy-root"),
		CompletedAt: &completed, ActiveProvider: "primary",
		Files: map[string]*storage.File{
			old.Name: {Name: old.Name, Path: old.LocalPath(), Size: old.Size},
		},
		Providers: map[string]*storage.ProviderEntry{
			"primary": {Provider: "primary", Files: map[string]*storage.ProviderFile{
				old.Name: {Id: old.Id, Path: old.Path},
			}},
		},
	}
	expanded, err := debridTypes.FilesByLogicalName([]debridTypes.File{
		{Id: "old", Path: "Release?/old.mkv", Size: 1},
		{Id: "new", Path: "Release?/new.mkv", Size: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.AddTorrentProvider(&debridTypes.Torrent{Debrid: "primary", Files: expanded}); err != nil {
		t.Fatal(err)
	}
	newFile := entry.Files["new.mkv"]
	if newFile == nil || strings.Split(filepath.ToSlash(newFile.Path), "/")[0] != oldDirectory {
		t.Fatalf("same native directory was unnecessarily split: old=%q new=%#v", old.LocalPath(), newFile)
	}
	if _, err := torrentEntryFileLayouts(entry); err != nil {
		t.Fatalf("lossy-root refresh created invalid final layout: %v", err)
	}
}

func TestTorrentOutputPathCollisionsAndSeasonSplits(t *testing.T) {
	root := t.TempDir()
	first := torrentOwnershipTestEntry(root, "first-torrent", config.DownloadActionDownload)
	first.Name = "Movie: Part Two"
	first.OutputName = storage.NewTorrentOutputName(first.Name, first.InfoHash)
	second := *first
	second.InfoHash = "second-torrent"
	second.Name = "Movie? Part Two"
	second.OutputName = storage.NewTorrentOutputName(second.Name, second.InfoHash)
	for _, entry := range []*storage.Entry{first, &second} {
		if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
			t.Fatal(err)
		}
	}
	if strings.EqualFold(first.DownloadPath(), second.DownloadPath()) {
		t.Fatal("sanitized titles collided")
	}
	seasons := []SeasonInfo{{InfoHash: "season-one", Name: "Series: S01"}, {InfoHash: "season-two", Name: "Series: S02"}}
	children := convertToMultiSeason(first, seasons)
	if children[0].OutputName == "" || children[0].OutputName == children[1].OutputName || children[0].OutputName == first.OutputName {
		t.Fatal("season split did not assign separate stable output identities")
	}
	first.OutputName = ""
	legacy := convertToMultiSeason(first, seasons)
	if legacy[0].OutputName != "" || legacy[1].OutputName != "" {
		t.Fatal("legacy season split silently opted into renamed output paths")
	}
}

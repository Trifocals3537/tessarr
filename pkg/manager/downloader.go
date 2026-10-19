package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trifocals3537/tessarr/internal/cdntraffic"
	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/notifications"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
	"github.com/sourcegraph/conc/pool"
)

type Downloader struct {
	manager        *Manager
	strmURL        string
	mountPath      string
	dest           string
	relativeLinks  bool
	logger         zerolog.Logger
	usenetDownload func(context.Context, string, string, io.Writer, func(int64, int64)) error
	torrentLink    func(context.Context, *storage.Entry, string) (string, error)
}

const (
	symlinkMountWaitTimeout     = 30 * time.Minute
	symlinkScanInitialInterval  = 100 * time.Millisecond
	symlinkScanMaxInterval      = 2 * time.Second
	symlinkReadyTimeout         = 2 * time.Minute
	symlinkReadyInitialInterval = 200 * time.Millisecond
	symlinkReadyMaxInterval     = 2 * time.Second
	symlinkLogEveryAttempts     = 10
	symlinkLogSampleSize        = 8
	symlinkScanReadBatch        = 256
	symlinkScanMaxEntries       = 100_000
	symlinkScanMaxDepth         = 64
)

type downloadLogMeta struct {
	requestHost     string
	finalHost       string
	requestRange    string
	contentRange    string
	responseProto   string
	contentEncoding string
	statusCode      int
	transferMode    string
	parts           int
}

// NewDownloadManager creates a new strm manager
func NewDownloadManager(manager *Manager) *Downloader {
	cfg := config.Get()
	strmURL := cfg.AppURL
	if strmURL == "" {
		bindAddress := cfg.BindAddress
		if bindAddress == "" {
			bindAddress = "localhost"
		}

		strmURL = fmt.Sprintf("http://%s:%s", bindAddress, cfg.Port)
	}
	return &Downloader{
		manager:       manager,
		strmURL:       strmURL,
		mountPath:     cfg.Mount.MountPath,
		relativeLinks: cfg.RelativeSymlinks,
		logger:        manager.logger.With().Str("component", "downloader").Logger(),
		dest:          cfg.DownloadFolder,
	}
}

func (d *Downloader) relativeSymlinksEnabled() bool {
	if d.manager != nil {
		return config.Get().RelativeSymlinks
	}
	return d.relativeLinks
}

func (d *Downloader) download(ctx context.Context, torrent *storage.Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Recheck persisted entries against the current naming policy before
	// changing queue state or creating output, including multi-season parents.
	if torrent.IsTorrent() {
		if err := validateTorrentSourceFolder(torrent, config.Get().FolderNaming, false); err != nil {
			return err
		}
	}
	// Mark as in-flight up front so the queue scheduler skips this entry while
	// we're iterating seasons / creating symlinks (processSymlink only flips
	// this flag after its own directory scan, which is too late for the parent
	// of a multi-season torrent).
	torrent.IsDownloading = true
	if err := d.manager.queue.Update(torrent); err != nil {
		return err
	}

	var (
		isMultiSeason bool
		seasons       []SeasonInfo
	)
	if !torrent.SkipMultiSeason {
		isMultiSeason, seasons = d.detectMultiSeason(torrent)
	}
	torrentMountPath := d.manager.GetTorrentMountPath(torrent)
	if isMultiSeason {
		seasonResults := convertToMultiSeason(torrent, seasons)
		for _, result := range seasonResults {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := d.manager.queue.Add(result); err != nil {
				d.logger.Error().Err(err).Msgf("Failed to save season torrent")
				continue
			}
			childWork, err := d.manager.entryLifecycle.startWork(
				ctx,
				result.InfoHash,
				result.QueueGeneration,
			)
			if err != nil {
				d.markAsError(result, err)
				continue
			}
			childErr := func() error {
				defer childWork.Close()
				return d.process(childWork.Context(), result, torrentMountPath)
			}()
			if errors.Is(childErr, errDeleteQueueEntryOnJobFinish) {
				if err := d.manager.queue.Delete(result.InfoHash, nil); err != nil {
					d.markAsError(result, err)
				}
			} else if childErr != nil {
				d.markAsError(result, childErr)
			}
		}
		// Parent has been fanned out into season entries; mark it complete so
		// it leaves the downloading queue instead of getting re-processed.
		d.completeEntry(torrent)
		if torrent.Action == config.DownloadActionNone {
			return errDeleteQueueEntryOnJobFinish
		}
		return nil
	}
	return d.process(ctx, torrent, torrentMountPath)
}

func (d *Downloader) process(ctx context.Context, entry *storage.Entry, mountPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if entry.IsNZB() {
		if _, _, err := claimUsenetEntryDirectory(d.dest, entry); err != nil {
			return fmt.Errorf("claim usenet download path: %w", err)
		}
		for _, file := range entry.GetActiveFiles() {
			if _, err := safeUsenetFilePath(d.dest, entry, file.Name); err != nil {
				return err
			}
		}
	}

	switch entry.Action {
	case config.DownloadActionDownload:
		return d.processDownload(ctx, entry)
	case config.DownloadActionSymlink:
		return d.processSymlink(ctx, entry, mountPath)
	case config.DownloadActionStrm:
		return d.processStrm(ctx, entry)
	case config.DownloadActionNone:
		d.completeEntry(entry)
		// The lifecycle lease held by this worker must close before deletion
		// waits for active work, otherwise it would wait on itself.
		return errDeleteQueueEntryOnJobFinish
	default:
		return d.processSymlink(ctx, entry, mountPath)
	}
}

func (d *Downloader) completeEntry(entry *storage.Entry) {
	d.markAsCompleted(entry)
	d.notifyCompleted(entry)
	d.triggerArrRefresh(entry)
}

func (d *Downloader) markAsCompleted(entry *storage.Entry) {
	// Mark as completed
	entry.MarkAsCompleted(entry.DownloadPath())
	_ = d.manager.queue.Update(entry)
}

func (d *Downloader) notifyCompleted(entry *storage.Entry) {
	// Send notification
	msg := fmt.Sprintf("Download completed: %s [%s] -> %s", entry.Name, entry.Category, entry.DownloadPath())
	d.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventDownloadComplete,
		Status:  "success",
		Entry:   entry,
		Message: msg,
	})
}

func (d *Downloader) triggerArrRefresh(entry *storage.Entry) {
	d.manager.startBackground("Arr refresh", func() {
		a := d.manager.arr.GetOrCreate(entry.Category)
		if a == nil || a.Host == "" || a.Token == "" {
			return
		}
		if err := a.Refresh(); err != nil {
			d.logger.Debug().
				Err(err).
				Str("arr", a.Name).
				Str("entry", entry.Name).
				Msg("Failed to trigger Arr refresh")
		}
	})
}

func (d *Downloader) markAsError(entry *storage.Entry, err error) {
	d.logger.Error().Err(err).Str("name", entry.Name).Msg("Failed to process action")
	entry.MarkAsError(err)
	_ = d.manager.queue.Update(entry)

	// Send error notification
	msg := fmt.Sprintf("Download failed: %s [%s] - %s", entry.Name, entry.Category, err.Error())
	d.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventDownloadFailed,
		Status:  "error",
		Entry:   entry,
		Message: msg,
		Error:   err,
	})
}

// processSymlink creates symlinks for torrent files
func (d *Downloader) processSymlink(ctx context.Context, entry *storage.Entry, mountPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	files := entry.GetActiveFiles()
	var torrentSymlinkPath string
	if entry.IsNZB() {
		var err error
		torrentSymlinkPath, _, err = claimUsenetEntryDirectory(d.dest, entry)
		if err != nil {
			return err
		}
	} else {
		var err error
		torrentSymlinkPath, _, err = claimTorrentEntryDirectory(d.dest, entry, torrentLegacyProof{mountPath: mountPath})
		if err != nil {
			return fmt.Errorf("claim torrent symlink path: %w", err)
		}
	}
	d.logger.Info().Str("mount_path", mountPath).Msgf("Creating symlinks for %d files in %s", len(files), torrentSymlinkPath)

	filePaths, err := d.createSymlinksWhenMountFilesAppear(ctx, entry, files, mountPath, torrentSymlinkPath)
	if err != nil {
		return err
	}

	entry.IsDownloading = true
	_ = d.manager.queue.Update(entry)

	if err := d.waitForSymlinkFilesReady(ctx, filePaths, symlinkReadyTimeout); err != nil {
		return err
	}

	// Before reporting completion to the Arr, prove a deterministic sample of
	// media files can be opened and read at the head, middle, and tail with the
	// expected logical size. SkipPreCache remains the explicit compatibility
	// opt-out. Mounts without a context-aware opener retain legacy behavior.
	if !d.manager.config.SkipPreCache && len(filePaths) > 0 {
		probeFiles, err := d.importReadinessFiles(entry, filePaths, MaxImportReadinessFiles)
		if err != nil {
			return fmt.Errorf("prepare import readiness check: %w", err)
		}
		if len(probeFiles) > 0 {
			if err := d.manager.verifyImportReadiness(ctx, entry.InfoHash, probeFiles); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if errors.Is(err, ErrCacheWarmUnavailable) {
					d.logger.Warn().
						Str("entry", entry.Name).
						Int("sampled_files", len(probeFiles)).
						Msg("Import readiness verification unavailable for mount backend; retaining compatibility behavior")
				} else {
					return fmt.Errorf("import readiness verification failed: %w", err)
				}
			} else {
				d.logger.Info().
					Str("entry", entry.Name).
					Int("sampled_files", len(probeFiles)).
					Int("media_files", countMediaPaths(filePaths)).
					Msg("Import readiness verification passed")
			}
		}
	}

	d.completeEntry(entry)

	return nil
}

func (d *Downloader) createSymlinksWhenMountFilesAppear(ctx context.Context, entry *storage.Entry, files []*storage.File, mountPath string, symlinkDir string) ([]string, error) {
	if !entry.IsNZB() {
		return d.createTorrentSymlinksWhenMountFilesAppear(ctx, entry, mountPath, symlinkDir)
	}

	remainingFiles := make(map[string]*storage.File, len(files))
	for _, file := range files {
		remainingFiles[file.Name] = file
	}

	filePaths := make([]string, 0, len(remainingFiles))
	deadline := time.Now().Add(symlinkMountWaitTimeout)
	delay := symlinkScanInitialInterval
	attempt := 0
	var lastScanErr error
	var scanErr error

	checkDirectory := func(rootPath string) error {
		type pendingDirectory struct {
			path  string
			depth int
		}
		pending := []pendingDirectory{{path: rootPath}}
		scanned := 0

		for len(pending) > 0 && len(remainingFiles) > 0 {
			current := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			if current.depth > symlinkScanMaxDepth {
				return fmt.Errorf("mount file scan exceeds maximum depth %d", symlinkScanMaxDepth)
			}

			directory, err := os.Open(current.path)
			if err != nil {
				if scanErr == nil {
					scanErr = err
				}
				continue
			}
			for {
				entries, readErr := directory.ReadDir(symlinkScanReadBatch)
				for _, item := range entries {
					scanned++
					if scanned > symlinkScanMaxEntries {
						_ = directory.Close()
						return fmt.Errorf("mount file scan exceeds %d entries", symlinkScanMaxEntries)
					}
					entryName := item.Name()
					fullPath := filepath.Join(current.path, entryName)

					// A directory can legitimately have the same basename as a
					// requested media file. Recurse into it before matching names so
					// the visible output never becomes a symlink to a directory.
					if item.IsDir() {
						pending = append(pending, pendingDirectory{
							path:  fullPath,
							depth: current.depth + 1,
						})
						continue
					}
					// Do not turn device nodes, sockets, or source symlinks into
					// managed media files. A zero type is the regular-file case.
					if !item.Type().IsRegular() {
						continue
					}

					if file, exists := remainingFiles[entryName]; exists {
						fileSymlinkPath, pathErr := safeUsenetFilePath(d.dest, entry, file.Name)
						if pathErr != nil {
							_ = directory.Close()
							return pathErr
						}
						storedTarget, targetErr := symlinkTarget(fileSymlinkPath, fullPath, d.relativeSymlinksEnabled())
						if targetErr != nil {
							_ = directory.Close()
							return targetErr
						}
						linkErr := safepath.Symlink(d.dest, storedTarget, fileSymlinkPath)
						if linkErr != nil {
							_ = directory.Close()
							return fmt.Errorf(
								"failed to create symlink %s -> %s: %w",
								fileSymlinkPath,
								fullPath,
								linkErr,
							)
						}
						filePaths = append(filePaths, fileSymlinkPath)
						delete(remainingFiles, entryName)
						d.logger.Info().Msgf("File is ready: %s/%s", entry.GetFolder(), file.Name)
					}
				}
				if errors.Is(readErr, io.EOF) {
					break
				}
				if readErr != nil {
					if scanErr == nil {
						scanErr = readErr
					}
					break
				}
			}
			if closeErr := directory.Close(); closeErr != nil && scanErr == nil {
				scanErr = closeErr
			}
		}
		return nil
	}

	for len(remainingFiles) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		attempt++
		scanErr = nil
		if err := checkDirectory(mountPath); err != nil {
			return nil, err
		}
		lastScanErr = scanErr
		if len(remainingFiles) == 0 {
			break
		}

		if time.Now().After(deadline) {
			pending := pendingMountFileNames(remainingFiles, symlinkLogSampleSize)
			if lastScanErr != nil {
				return nil, fmt.Errorf("timeout waiting for mount files: %d files still pending (%s): last scan error: %w", len(remainingFiles), strings.Join(pending, ", "), lastScanErr)
			}
			return nil, fmt.Errorf("timeout waiting for mount files: %d files still pending (%s)", len(remainingFiles), strings.Join(pending, ", "))
		}

		if shouldLogSymlinkWaitAttempt(attempt) {
			d.logger.Debug().
				Err(lastScanErr).
				Str("entry", entry.Name).
				Str("mount_path", mountPath).
				Int("pending", len(remainingFiles)).
				Strs("sample", pendingMountFileNames(remainingFiles, symlinkLogSampleSize)).
				Msg("Waiting for mount files before creating symlinks")
		}

		if err := d.sleepUntilNextSymlinkAttempt(ctx, delay, deadline); err != nil {
			return nil, err
		}
		delay = nextSymlinkBackoff(delay, symlinkScanMaxInterval)
	}

	return filePaths, nil
}

func (d *Downloader) createTorrentSymlinksWhenMountFilesAppear(ctx context.Context, entry *storage.Entry, mountPath, symlinkDir string) ([]string, error) {
	entryPath, err := safeTorrentEntryDownloadPath(d.dest, entry)
	if err != nil {
		return nil, err
	}
	if !sameFilesystemPath(entryPath, symlinkDir) {
		return nil, fmt.Errorf("torrent symlink directory %q does not match owned output path %q", symlinkDir, entryPath)
	}
	layouts, err := torrentEntryFileLayouts(entry)
	if err != nil {
		return nil, err
	}
	remaining := make(map[string]torrentFileLayout, len(layouts))
	sourceKeys := make(map[string][]string, len(layouts))
	basenameCounts := make(map[string]int, len(layouts))
	activePlacement := entry.GetActiveProvider()
	for _, layout := range layouts {
		remaining[layout.key] = layout
		sourceKeys[layout.key] = []string{layout.key}
		if activePlacement != nil {
			if providerFile := activePlacement.Files[layout.file.Name]; providerFile != nil {
				providerRelative, providerErr := normalizeTorrentProviderSourcePath(providerFile.Path)
				if providerErr == nil {
					providerKey := torrentProviderSourceKey(providerRelative)
					if providerKey != layout.key {
						sourceKeys[layout.key] = append([]string{providerKey}, sourceKeys[layout.key]...)
					}
				}
			}
		}
		basenameCounts[strings.ToLower(filepath.Base(layout.relative))]++
	}

	filePaths := make([]string, 0, len(layouts))
	deadline := time.Now().Add(symlinkMountWaitTimeout)
	delay := symlinkScanInitialInterval
	attempt := 0
	var lastScanErr error

	for len(remaining) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		attempt++
		actualByPath, actualByBase, scanErr := scanTorrentMountFiles(mountPath)
		lastScanErr = scanErr
		if scanErr == nil {
			for key, layout := range remaining {
				var source string
				for _, sourceKey := range sourceKeys[key] {
					if source = actualByPath[sourceKey]; source != "" {
						break
					}
				}
				if source == "" {
					baseKey := strings.ToLower(filepath.Base(layout.relative))
					candidates := actualByBase[baseKey]
					if basenameCounts[baseKey] == 1 && len(candidates) == 1 {
						source = candidates[0]
					}
				}
				if source == "" {
					continue
				}
				if err := safepath.RejectSymlinks(source); err != nil {
					return nil, fmt.Errorf("refusing torrent mount source %q: %w", source, err)
				}
				destination, err := createOwnedTorrentSymlink(
					d.dest,
					entry,
					layout.relative,
					source,
					d.relativeSymlinksEnabled(),
				)
				if err != nil {
					return nil, err
				}
				filePaths = append(filePaths, destination)
				delete(remaining, key)
				d.logger.Info().Msgf("File is ready: %s/%s", entry.GetFolder(), layout.relative)
			}
		}
		if len(remaining) == 0 {
			break
		}
		if time.Now().After(deadline) {
			pending := pendingTorrentLayoutNames(remaining, symlinkLogSampleSize)
			if lastScanErr != nil {
				return nil, fmt.Errorf("timeout waiting for mount files: %d files still pending (%s): last scan error: %w", len(remaining), strings.Join(pending, ", "), lastScanErr)
			}
			return nil, fmt.Errorf("timeout waiting for mount files: %d files still pending (%s)", len(remaining), strings.Join(pending, ", "))
		}
		if shouldLogSymlinkWaitAttempt(attempt) {
			d.logger.Debug().
				Err(lastScanErr).
				Str("entry", entry.Name).
				Str("mount_path", mountPath).
				Int("pending", len(remaining)).
				Strs("sample", pendingTorrentLayoutNames(remaining, symlinkLogSampleSize)).
				Msg("Waiting for torrent mount files before creating symlinks")
		}
		if err := d.sleepUntilNextSymlinkAttempt(ctx, delay, deadline); err != nil {
			return nil, err
		}
		delay = nextSymlinkBackoff(delay, symlinkScanMaxInterval)
	}
	sort.Strings(filePaths)
	return filePaths, nil
}

func pendingTorrentLayoutNames(remaining map[string]torrentFileLayout, limit int) []string {
	names := make([]string, 0, len(remaining))
	for _, layout := range remaining {
		names = append(names, layout.relative)
	}
	sort.Strings(names)
	return limitedStringSample(names, limit)
}

func scanTorrentMountFiles(mountPath string) (map[string]string, map[string][]string, error) {
	rootInfo, err := os.Lstat(mountPath)
	if err != nil {
		return nil, nil, err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, nil, fmt.Errorf("torrent mount path %q is not a regular directory", mountPath)
	}
	root, err := os.OpenRoot(mountPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open torrent mount root: %w", err)
	}
	defer root.Close()
	pinnedRoot, err := root.Stat(".")
	if err != nil || !os.SameFile(rootInfo, pinnedRoot) {
		if err != nil {
			return nil, nil, fmt.Errorf("stat torrent mount root: %w", err)
		}
		return nil, nil, fmt.Errorf("torrent mount root changed while opening")
	}
	type pendingDir struct {
		relative string
		depth    int
	}
	pending := []pendingDir{{relative: ".", depth: 0}}
	byPath := make(map[string]string)
	byBase := make(map[string][]string)
	seen := 0
	for len(pending) > 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if current.depth > torrentOwnershipMaxDepth {
			return nil, nil, fmt.Errorf("torrent mount tree exceeds maximum depth")
		}
		before, err := root.Lstat(current.relative)
		if err != nil || before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
			if err != nil {
				return nil, nil, fmt.Errorf("inspect torrent mount directory %q: %w", current.relative, err)
			}
			return nil, nil, fmt.Errorf("torrent mount path %q is not a regular directory", current.relative)
		}
		dir, err := root.Open(current.relative)
		if err != nil {
			return nil, nil, err
		}
		opened, err := dir.Stat()
		if err != nil || !os.SameFile(before, opened) {
			_ = dir.Close()
			if err != nil {
				return nil, nil, fmt.Errorf("stat opened torrent mount directory %q: %w", current.relative, err)
			}
			return nil, nil, fmt.Errorf("torrent mount directory %q changed while opening", current.relative)
		}
		for {
			entries, readErr := dir.ReadDir(torrentOwnershipReadBatch)
			for _, item := range entries {
				seen++
				if seen > torrentOwnershipMaxEntries {
					_ = dir.Close()
					return nil, nil, fmt.Errorf("torrent mount tree exceeds %d entries", torrentOwnershipMaxEntries)
				}
				if err := validateTorrentProviderSourceIdentifier(item.Name()); err != nil {
					_ = dir.Close()
					return nil, nil, fmt.Errorf("invalid torrent mount name %q: %w", item.Name(), err)
				}
				relative := filepath.Join(current.relative, item.Name())
				relative = strings.TrimPrefix(relative, "."+string(filepath.Separator))
				info, err := root.Lstat(relative)
				if err != nil {
					_ = dir.Close()
					return nil, nil, err
				}
				if info.Mode()&os.ModeSymlink != 0 {
					continue
				}
				if info.IsDir() {
					pending = append(pending, pendingDir{
						relative: relative,
						depth:    current.depth + 1,
					})
					continue
				}
				if !info.Mode().IsRegular() {
					continue
				}
				normalized, err := normalizeTorrentProviderSourcePath(relative)
				if err != nil {
					_ = dir.Close()
					return nil, nil, err
				}
				key := torrentProviderSourceKey(normalized)
				absolute := filepath.Join(mountPath, relative)
				if previous, exists := byPath[key]; exists && !sameFilesystemPath(previous, absolute) {
					_ = dir.Close()
					return nil, nil, fmt.Errorf("torrent mount files collide at portable path %q", normalized)
				}
				byPath[key] = absolute
				baseKey := strings.ToLower(filepath.Base(normalized))
				byBase[baseKey] = append(byBase[baseKey], absolute)
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				_ = dir.Close()
				return nil, nil, readErr
			}
		}
		if err := dir.Close(); err != nil {
			return nil, nil, err
		}
		after, err := root.Lstat(current.relative)
		if err != nil || !os.SameFile(opened, after) {
			if err != nil {
				return nil, nil, fmt.Errorf("reinspect torrent mount directory %q: %w", current.relative, err)
			}
			return nil, nil, fmt.Errorf("torrent mount directory %q changed during scan", current.relative)
		}
	}
	for key := range byBase {
		sort.Strings(byBase[key])
	}
	return byPath, byBase, nil
}

func (d *Downloader) waitForSymlinkFilesReady(ctx context.Context, filePaths []string, timeout time.Duration) error {
	if len(filePaths) == 0 {
		return nil
	}

	pending := make(map[string]error, len(filePaths))
	for _, path := range filePaths {
		pending[path] = nil
	}

	deadline := time.Now().Add(timeout)
	delay := symlinkReadyInitialInterval
	attempt := 0

	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		attempt++
		for path := range pending {
			if err := verifySymlinkFileReady(path); err != nil {
				pending[path] = err
				continue
			}
			delete(pending, path)
		}
		if len(pending) == 0 {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for symlink files to be ready: %d files still pending (%s)", len(pending), strings.Join(pendingSymlinkFileStatuses(pending, symlinkLogSampleSize), ", "))
		}

		if shouldLogSymlinkWaitAttempt(attempt) {
			d.logger.Debug().
				Int("pending", len(pending)).
				Strs("sample", pendingSymlinkFileStatuses(pending, symlinkLogSampleSize)).
				Msg("Waiting for symlink files to resolve")
		}

		if err := d.sleepUntilNextSymlinkAttempt(ctx, delay, deadline); err != nil {
			return err
		}
		delay = nextSymlinkBackoff(delay, symlinkReadyMaxInterval)
	}

	return nil
}

func verifySymlinkFileReady(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("symlink not available: %w", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return fmt.Errorf("path is not a symlink")
	}

	target, err := os.Readlink(path)
	if err != nil {
		return fmt.Errorf("symlink target cannot be read: %w", err)
	}
	if _, err := resolveSymlinkTarget(path, target); err != nil {
		return fmt.Errorf("symlink target is invalid: %w", err)
	}
	return nil
}

func (d *Downloader) sleepUntilNextSymlinkAttempt(ctx context.Context, delay time.Duration, deadline time.Time) error {
	if remaining := time.Until(deadline); remaining < delay {
		delay = remaining
	}
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextSymlinkBackoff(current time.Duration, maxDelay time.Duration) time.Duration {
	current *= 2
	if current > maxDelay {
		return maxDelay
	}
	return current
}

func shouldLogSymlinkWaitAttempt(attempt int) bool {
	return attempt == 1 || attempt%symlinkLogEveryAttempts == 0
}

func pendingMountFileNames(files map[string]*storage.File, limit int) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	return limitedStringSample(names, limit)
}

func pendingSymlinkFileStatuses(files map[string]error, limit int) []string {
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	statuses := make([]string, 0, len(paths))
	for _, path := range paths {
		err := files[path]
		status := path
		if err != nil {
			status = fmt.Sprintf("%s: %s", path, err.Error())
		}
		statuses = append(statuses, status)
	}
	return limitedStringSample(statuses, limit)
}

func limitedStringSample(values []string, limit int) []string {
	if limit <= 0 || len(values) <= limit {
		return values
	}

	sample := append([]string(nil), values[:limit]...)
	sample = append(sample, fmt.Sprintf("... %d more", len(values)-limit))
	return sample
}

// processDownload downloads all files for an entry with progress tracking
// For torrents: uses HTTP download from debrid
// For NZBs: uses parallel NNTP segment download
func (d *Downloader) processDownload(ctx context.Context, entry *storage.Entry) error {
	// Check if this is a usenet entry
	if entry.IsNZB() {
		return d.processUsenetDownload(ctx, entry)
	}
	return d.processTorrentDownload(ctx, entry)
}

// processTorrentDownload downloads files from debrid via HTTP
func (d *Downloader) processTorrentDownload(ctx context.Context, entry *storage.Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	layouts, err := torrentEntryFileLayouts(entry)
	if err != nil {
		return err
	}
	d.logger.Info().Msgf("Downloading %d files...", len(layouts))

	totalSize := int64(0)
	for _, layout := range layouts {
		transferSize := torrentTransferSize(layout.file)
		if transferSize > math.MaxInt64-totalSize {
			return fmt.Errorf("torrent download total transfer size overflows")
		}
		totalSize += transferSize
	}
	if _, _, err := claimTorrentEntryDirectory(d.dest, entry, torrentLegacyProof{}); err != nil {
		return fmt.Errorf("claim torrent download path: %w", err)
	}
	entry.SizeDownloaded = 0
	entry.IsDownloading = true
	entry.Progress = 0

	var progressMu sync.Mutex
	progressCallback := func(downloaded int64, speed int64) {
		progressMu.Lock()
		defer progressMu.Unlock()

		entry.SizeDownloaded += downloaded
		entry.Speed = speed
		if totalSize > 0 {
			entry.Progress = float64(entry.SizeDownloaded) / float64(totalSize)
		}
		entry.UpdatedAt = time.Now()
		_ = d.manager.queue.Update(entry)
	}

	// Resolve download links before spawning goroutines
	type downloadTask struct {
		layout torrentFileLayout
		link   debridTypes.DownloadLink
	}
	downloadCtx := cdntraffic.WithPriority(ctx, cdntraffic.PriorityBackground)
	var tasks []downloadTask
	for _, layout := range layouts {
		if err := ctx.Err(); err != nil {
			return err
		}
		file := layout.file
		var downloadLink debridTypes.DownloadLink
		var err error
		if d.torrentLink != nil {
			downloadLink.DownloadLink, err = d.torrentLink(downloadCtx, entry, file.Name)
			downloadLink.Debrid = entry.ActiveProvider
			downloadLink.Filename = file.Name
		} else {
			downloadLink, err = d.manager.linkService.GetLink(downloadCtx, entry, file.Name)
		}
		if err != nil {
			return fmt.Errorf("resolve all torrent download links: file %q: %w", file.Name, err)
		}
		if strings.TrimSpace(downloadLink.DownloadLink) == "" {
			return fmt.Errorf("resolve all torrent download links: file %q returned an empty URL", file.Name)
		}
		tasks = append(tasks, downloadTask{layout: layout, link: downloadLink})
	}

	// If no valid download links were obtained, return error instead of panic
	if len(tasks) == 0 {
		return fmt.Errorf("no valid download links available for %s", entry.Name)
	}

	p := pool.New().WithMaxGoroutines(min(len(tasks), 4)).WithErrors().WithFirstError()
	for _, task := range tasks {
		p.Go(func() error {
			part, err := openOwnedTorrentPart(d.dest, entry, task.layout.relative, torrentTransferSize(task.layout.file))
			if err != nil {
				return err
			}
			defer part.Close()
			if err := d.localDownloaderWithLink(
				downloadCtx,
				task.link,
				entry.ActiveProvider,
				part,
				task.layout.file.ByteRange,
				progressCallback,
			); err != nil {
				d.logger.Error().Msgf("Failed to download %s: %v", task.layout.file.Name, err)
				return err
			}
			if err := part.Commit(); err != nil {
				return err
			}
			d.logger.Info().Msgf("Downloaded %s", task.layout.file.Name)
			return nil
		})
	}

	if err := p.Wait(); err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	d.completeEntry(entry)
	d.logger.Info().Msgf("Downloaded all files for %s", entry.Name)
	return nil
}

func torrentTransferSize(file *storage.File) int64 {
	if file == nil {
		return 0
	}
	if file.ByteRange != nil && file.ByteRange[0] >= 0 && file.ByteRange[1] >= file.ByteRange[0] {
		if file.ByteRange[1]-file.ByteRange[0] == math.MaxInt64 {
			return 0
		}
		return file.ByteRange[1] - file.ByteRange[0] + 1
	}
	return file.Size
}

// processUsenetDownload downloads NZB files via parallel NNTP segment fetching
func (d *Downloader) processUsenetDownload(ctx context.Context, entry *storage.Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	download := d.usenetDownload
	if download == nil && d.manager.usenet != nil {
		download = func(ctx context.Context, nzoID, filename string, writer io.Writer, progress func(int64, int64)) error {
			return d.manager.usenet.Download(ctx, nzoID, filename, writer, progress)
		}
	}
	if download == nil {
		return fmt.Errorf("usenet client not configured")
	}

	files := entry.GetActiveFiles()
	d.logger.Info().Msgf("Downloading %d NZB files via usenet...", len(files))

	if _, err := d.prepareUsenetDownloadDirectory(entry); err != nil {
		return err
	}

	totalSize := int64(0)
	for _, file := range files {
		totalSize += file.Size
	}

	entry.SizeDownloaded = 0
	entry.Progress = 0
	entry.IsDownloading = true
	_ = d.manager.queue.Update(entry)

	var progressMu sync.Mutex
	// Track per-file progress so we can compute the global total across all files
	fileProgress := make(map[string]int64)

	p := pool.New().WithErrors().WithFirstError()
	for _, file := range files {
		p.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			destPath, err := safeUsenetFilePath(d.dest, entry, file.Name)
			if err != nil {
				return err
			}
			destFile, err := safepath.OpenFile(d.dest, destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", file.Name, err)
			}
			defer destFile.Close()

			progressCallback := func(downloaded int64, speed int64) {
				progressMu.Lock()
				defer progressMu.Unlock()

				prev := fileProgress[file.Name]
				fileProgress[file.Name] = downloaded
				entry.SizeDownloaded += downloaded - prev
				entry.Speed = speed
				if totalSize > 0 {
					entry.Progress = float64(entry.SizeDownloaded) / float64(totalSize)
				}
				entry.UpdatedAt = time.Now()
				_ = d.manager.queue.Update(entry)
			}

			if err := download(ctx, entry.InfoHash, file.Name, destFile, progressCallback); err != nil {
				_ = safepath.Remove(d.dest, destPath)
				return fmt.Errorf("failed to download %s: %w", file.Name, err)
			}

			d.logger.Info().Msgf("Downloaded NZB file: %s", file.Name)
			return nil
		})
	}

	err := p.Wait()

	if err != nil {
		entry.MarkAsError(err)
		_ = d.manager.queue.Update(entry)
		return fmt.Errorf("NZB download failed: %w", err)
	}

	d.completeEntry(entry)
	d.logger.Info().Msgf("Downloaded all NZB files for %s", entry.Name)
	return nil
}

func (d *Downloader) prepareUsenetDownloadDirectory(entry *storage.Entry) (string, error) {
	downloadedFolder, _, err := claimUsenetEntryDirectory(d.dest, entry)
	if err != nil {
		return "", fmt.Errorf("claim usenet download path: %w", err)
	}
	return downloadedFolder, nil
}

// processStrm creates symlinks for torrent files
func (d *Downloader) processStrm(ctx context.Context, torrent *storage.Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	files := torrent.GetActiveFiles()
	d.logger.Info().Msgf("Creating .strm for %d files ...", len(files))

	var torrentSymlinkPath string
	if torrent.IsNZB() {
		var err error
		torrentSymlinkPath, _, err = claimUsenetEntryDirectory(d.dest, torrent)
		if err != nil {
			return err
		}
	} else {
		var err error
		torrentSymlinkPath, _, err = claimTorrentEntryDirectory(d.dest, torrent, torrentLegacyProof{strmURL: d.strmURL})
		if err != nil {
			return fmt.Errorf("claim torrent STRM path: %w", err)
		}
	}

	var torrentLayouts map[*storage.File]torrentFileLayout
	if torrent.IsTorrent() {
		layouts, layoutErr := torrentEntryFileLayouts(torrent)
		if layoutErr != nil {
			return layoutErr
		}
		torrentLayouts = make(map[*storage.File]torrentFileLayout, len(layouts))
		for _, layout := range layouts {
			torrentLayouts[layout.file] = layout
		}
	}
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		var strmFilePath string
		if torrent.IsNZB() {
			safePath, pathErr := safeUsenetFilePath(d.dest, torrent, file.Name+".strm")
			if pathErr != nil {
				return pathErr
			}
			strmFilePath = safePath
		} else {
			layout, ok := torrentLayouts[file]
			if !ok {
				return fmt.Errorf("torrent STRM layout is missing for %q", file.Name)
			}
			safePath, pathErr := safeTorrentFilePath(d.dest, torrent, layout.relative, ".strm")
			if pathErr != nil {
				return pathErr
			}
			strmFilePath = safePath
		}
		streamURL, err := torrentSTRMURL(d.strmURL, torrent, file)
		if err != nil {
			continue
		}
		if err := d.writeStrmFile(torrent, strmFilePath, streamURL); err != nil {
			return fmt.Errorf("failed to create .strm file: %s: %v", strmFilePath, err)
		}
	}
	d.completeEntry(torrent)
	d.logger.Info().Str("destination", torrentSymlinkPath).Msgf("Created .strm files for %s", torrent.Name)
	return nil
}

func (d *Downloader) writeStrmFile(entry *storage.Entry, path, contents string) error {
	if entry.IsTorrent() {
		entryPath, err := safeTorrentEntryDownloadPath(d.dest, entry)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(entryPath, path)
		if err != nil {
			return err
		}
		return writeOwnedTorrentFile(d.dest, entry, relative, []byte(contents), 0o644)
	}
	file, err := safepath.OpenFile(d.dest, path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(contents); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func (d *Downloader) detectMultiSeason(torrent *storage.Entry) (bool, []SeasonInfo) {
	torrentName := torrent.Name
	files := torrent.GetActiveFiles()

	// Find all seasons present in the files
	seasonsFound := findAllSeasons(files)

	// Check if this is actually a multi-season torrent
	isMultiSeason := len(seasonsFound) > 1 || hasMultiSeasonIndicators(torrentName)

	if !isMultiSeason {
		return false, nil
	}

	// Group files by season
	seasonGroups := groupFilesBySeason(files, seasonsFound)

	// Create SeasonInfo objects with proper naming
	var seasons []SeasonInfo
	for seasonNum, seasonFiles := range seasonGroups {
		if len(seasonFiles) == 0 {
			continue
		}

		// Generate season-specific name preserving all metadata
		seasonName := replaceMultiSeasonPattern(torrentName, seasonNum)

		seasons = append(seasons, SeasonInfo{
			SeasonNumber: seasonNum,
			Files:        seasonFiles,
			InfoHash:     generateSeasonHash(torrent.InfoHash, seasonNum),
			Name:         seasonName,
		})
	}

	// A name such as "Complete Series" is only a hint. Do not split the entry
	// unless its files actually produced multiple populated season groups.
	if len(seasons) <= 1 {
		return false, nil
	}

	d.logger.Info().Msgf("Multi-season torrent detected with seasons: %v", getSortedSeasons(seasonsFound))

	return true, seasons
}

// localDownloader streams into an already-open, rooted partial file. No HTTP
// client ever reopens a caller-derived final path, and the partial file becomes
// visible only after ownedTorrentPart.Commit performs a rooted no-overwrite
// publish.
func (d *Downloader) localDownloader(ctx context.Context, downloadURL string, part *ownedTorrentPart, byterange *[2]int64, progressCallback func(int64, int64)) error {
	return d.localDownloaderWithLink(
		ctx,
		debridTypes.DownloadLink{DownloadLink: downloadURL},
		"",
		part,
		byterange,
		progressCallback,
	)
}

func (d *Downloader) localDownloaderWithLink(ctx context.Context, downloadLink debridTypes.DownloadLink, fallbackProvider string, part *ownedTorrentPart, byterange *[2]int64, progressCallback func(int64, int64)) error {
	if part == nil || part.file == nil {
		return fmt.Errorf("rooted torrent partial file is required")
	}
	ctx = cdntraffic.WithPriority(ctx, cdntraffic.PriorityBackground)
	ctx = d.manager.withCDNIdentity(ctx, downloadLink, fallbackProvider)
	downloadURL := downloadLink.DownloadLink
	startTime := time.Now()
	requestedRange := "full"
	currentSize, err := part.Size()
	if err != nil {
		return err
	}
	expectedSize := part.expectedSize
	if expectedSize > 0 && currentSize == expectedSize {
		if progressCallback != nil && currentSize > 0 {
			progressCallback(currentSize, 0)
		}
		return nil
	}

	rangeStart := int64(-1)
	rangeEnd := int64(-1)
	if byterange != nil {
		if byterange[0] < 0 || byterange[1] < byterange[0] {
			return fmt.Errorf("invalid torrent byte range %d-%d", byterange[0], byterange[1])
		}
		rangeStart = byterange[0] + currentSize
		rangeEnd = byterange[1]
	} else if currentSize > 0 {
		if expectedSize <= 0 {
			if err := part.Reset(); err != nil {
				return fmt.Errorf("restart torrent download with unknown expected size: %w", err)
			}
			currentSize = 0
		} else {
			rangeStart = currentSize
			rangeEnd = expectedSize - 1
		}
	}
	if rangeStart >= 0 {
		requestedRange = fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return fmt.Errorf("invalid torrent download URL")
	}
	req.Header.Set("User-Agent", "Tessarr[QBitTorrent]")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Encoding", "identity")
	if rangeStart >= 0 {
		req.Header.Set("Range", requestedRange)
	}

	resp, err := d.manager.streamClient.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("torrent request to %s failed", safeTorrentHTTPOrigin(downloadURL))
	}
	if resp == nil {
		return fmt.Errorf("HTTP client returned a nil torrent response")
	}
	defer resp.Body.Close()

	filename := filepath.Join(part.entryPath, part.finalRelative)
	var downloaded atomic.Int64
	defer func() {
		meta := d.buildDownloadLogMeta(req, resp, requestedRange, "rooted", 1)
		d.logDownloadCompletion(filename, startTime, &downloaded, meta)
	}()

	if encoding := strings.TrimSpace(resp.Header.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return fmt.Errorf("unexpected encoded torrent response %q", encoding)
	}
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		size, statErr := part.Size()
		if statErr == nil && expectedSize > 0 && size == expectedSize {
			if progressCallback != nil {
				progressCallback(size, 0)
			}
			return nil
		}
		return fmt.Errorf("torrent server at %s rejected range %q with status 416", safeTorrentHTTPOrigin(downloadURL), requestedRange)
	}

	resumed := false
	switch resp.StatusCode {
	case http.StatusPartialContent:
		if rangeStart < 0 {
			return fmt.Errorf("torrent server returned 206 without a requested range")
		}
		actualStart, actualEnd, actualTotal, err := parseTorrentContentRange(resp.Header.Get("Content-Range"))
		if err != nil {
			return err
		}
		if actualStart != rangeStart || actualEnd != rangeEnd {
			return fmt.Errorf("torrent Content-Range %d-%d does not exactly match request %d-%d", actualStart, actualEnd, rangeStart, rangeEnd)
		}
		if byterange == nil && expectedSize > 0 && actualTotal != expectedSize {
			return fmt.Errorf("torrent Content-Range total %d does not match expected size %d", actualTotal, expectedSize)
		}
		resumed = currentSize > 0
	case http.StatusOK:
		if byterange != nil {
			return fmt.Errorf("torrent server ignored required byte range %q", requestedRange)
		}
		if err := part.Reset(); err != nil {
			return fmt.Errorf("restart torrent download after full response: %w", err)
		}
		currentSize = 0
	default:
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("unexpected torrent status %d from %s", resp.StatusCode, safeTorrentHTTPOrigin(downloadURL))
		}
		if rangeStart >= 0 {
			return fmt.Errorf("torrent server returned status %d for range %q", resp.StatusCode, requestedRange)
		}
	}
	if resumed && progressCallback != nil {
		progressCallback(currentSize, 0)
	}

	buffer := make([]byte, 1<<20)
	lastReport := time.Now()
	var sinceReport int64
	writtenSize := currentSize
	for {
		n, readErr := resp.Body.Read(buffer)
		if n > 0 {
			if expectedSize > 0 && writtenSize+int64(n) > expectedSize {
				_ = part.Reset()
				return fmt.Errorf("torrent response exceeds expected size %d", expectedSize)
			}
			written, writeErr := part.file.Write(buffer[:n])
			if writeErr != nil {
				return writeErr
			}
			if written != n {
				return io.ErrShortWrite
			}
			writtenSize += int64(written)
			downloaded.Add(int64(written))
			sinceReport += int64(written)
			elapsed := time.Since(lastReport)
			if progressCallback != nil && elapsed >= 500*time.Millisecond {
				speed := int64(float64(sinceReport) / elapsed.Seconds())
				progressCallback(sinceReport, speed)
				sinceReport = 0
				lastReport = time.Now()
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				return fmt.Errorf("torrent response body read failed for %s", safeTorrentHTTPOrigin(downloadURL))
			}
			if progressCallback != nil && sinceReport > 0 {
				elapsed := time.Since(lastReport)
				speed := int64(0)
				if elapsed > 0 {
					speed = int64(float64(sinceReport) / elapsed.Seconds())
				}
				progressCallback(sinceReport, speed)
			}
			finalSize, statErr := part.Size()
			if statErr != nil {
				return statErr
			}
			if expectedSize > 0 && finalSize != expectedSize {
				return fmt.Errorf("torrent response ended at %d bytes, expected %d", finalSize, expectedSize)
			}
			return nil
		}
	}
}

func safeTorrentHTTPOrigin(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return "remote host"
	}
	if parsed.Scheme == "" {
		return parsed.Host
	}
	return parsed.Scheme + "://" + parsed.Host
}

func parseTorrentContentRange(value string) (start, end, total int64, err error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(strings.ToLower(value), "bytes ") {
		return 0, 0, 0, fmt.Errorf("invalid torrent Content-Range %q", value)
	}
	rangeAndTotal := strings.SplitN(strings.TrimSpace(value[len("bytes "):]), "/", 2)
	if len(rangeAndTotal) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid torrent Content-Range %q", value)
	}
	bounds := strings.SplitN(rangeAndTotal[0], "-", 2)
	if len(bounds) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid torrent Content-Range %q", value)
	}
	start, err = strconv.ParseInt(bounds[0], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid torrent Content-Range start: %w", err)
	}
	end, err = strconv.ParseInt(bounds[1], 10, 64)
	if err != nil || end < start {
		if err == nil {
			err = fmt.Errorf("end precedes start")
		}
		return 0, 0, 0, fmt.Errorf("invalid torrent Content-Range end: %w", err)
	}
	if rangeAndTotal[1] == "*" {
		return start, end, -1, nil
	}
	total, err = strconv.ParseInt(rangeAndTotal[1], 10, 64)
	if err != nil || total <= end {
		if err == nil {
			err = fmt.Errorf("total does not exceed range end")
		}
		return 0, 0, 0, fmt.Errorf("invalid torrent Content-Range total: %w", err)
	}
	return start, end, total, nil
}

func (d *Downloader) buildDownloadLogMeta(req *http.Request, resp *http.Response, requestedRange, transferMode string, parts int) downloadLogMeta {
	meta := downloadLogMeta{
		requestHost:     req.URL.Host,
		requestRange:    requestedRange,
		contentRange:    "none",
		contentEncoding: "identity",
		responseProto:   "unknown",
		statusCode:      0,
		transferMode:    transferMode,
		parts:           parts,
	}

	if resp == nil {
		return meta
	}

	if resp.Request != nil && resp.Request.URL != nil {
		meta.finalHost = resp.Request.URL.Host
	}
	meta.responseProto = resp.Proto
	if resp.TLS != nil && resp.TLS.NegotiatedProtocol != "" {
		meta.responseProto = fmt.Sprintf("%s (alpn=%s)", resp.Proto, resp.TLS.NegotiatedProtocol)
	}
	if contentRange := resp.Header.Get("Content-Range"); contentRange != "" {
		meta.contentRange = contentRange
	}
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" {
		meta.contentEncoding = encoding
	}
	meta.statusCode = resp.StatusCode
	return meta
}

func (d *Downloader) logDownloadCompletion(filename string, startTime time.Time, downloaded *atomic.Int64, meta downloadLogMeta) {
	bytesDownloaded := downloaded.Load()
	elapsed := time.Since(startTime)
	speedMBps := float64(0)
	if elapsed > 0 {
		speedMBps = float64(bytesDownloaded) / elapsed.Seconds() / (1024 * 1024)
	}

	d.logger.Info().
		Str("file", filepath.Base(filename)).
		Str("request_host", meta.requestHost).
		Str("final_host", meta.finalHost).
		Str("request_range", meta.requestRange).
		Str("content_range", meta.contentRange).
		Str("response_proto", meta.responseProto).
		Str("content_encoding", meta.contentEncoding).
		Str("transfer_mode", meta.transferMode).
		Int("parts", meta.parts).
		Int64("status", int64(meta.statusCode)).
		Int64("bytes", bytesDownloaded).
		Dur("duration", elapsed).
		Float64("speed_mbps", speedMBps).
		Msg("download transfer completed")
}

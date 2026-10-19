package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

const (
	legacyCacheReadBatch              = 256
	legacyCacheMaxEntries             = 100_000
	legacyCacheMaxDebridFolders       = 100
	legacyCacheMaxFileBytes     int64 = 64 << 20
	legacyCacheMaxTotalBytes          = 256 << 20
)

// Migrator handles migration from cache JSON files to unified bbolt system
type Migrator struct {
	storage    *storage.Storage
	cacheDir   string
	backupPath string
	logger     zerolog.Logger
	mu         sync.RWMutex
	cancelFunc context.CancelFunc
	ctx        context.Context
	done       chan struct{}
}

// NewMigrator creates a new migrator
func NewMigrator(storage *storage.Storage) *Migrator {
	cacheDir := filepath.Join(config.GetMainPath(), "cache")
	backupPath := filepath.Join(config.GetMainPath(), "backups")

	return &Migrator{
		storage:    storage,
		cacheDir:   cacheDir,
		backupPath: backupPath,
		logger:     logger.New("migrator"),
	}
}

// Start starts the migration process from cache files
func (m *Migrator) Start(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})

	m.mu.Lock()
	if m.cancelFunc != nil {
		m.mu.Unlock()
		cancel()
		return fmt.Errorf("migration is already running")
	}
	m.ctx = ctx
	m.cancelFunc = cancel
	m.done = done
	m.mu.Unlock()

	defer func() {
		cancel()
		close(done)

		m.mu.Lock()
		m.ctx = nil
		m.cancelFunc = nil
		m.done = nil
		m.mu.Unlock()
	}()

	// Load cache torrents
	cachedTorrents, err := m.loadCacheTorrents()
	if err != nil {
		return fmt.Errorf("failed to load cache torrents: %w", err)
	}

	// Initialize migration status
	status := &storage.SystemMigrationStatus{
		Running:   true,
		Total:     len(cachedTorrents),
		Completed: 0,
		Errors:    0,
		StartedAt: time.Now(),
		UpdatedAt: time.Now(),
		ErrorList: []string{},
	}

	if err := m.storage.SaveMigrationStatus(status); err != nil {
		return fmt.Errorf("failed to save migration status: %w", err)
	}

	m.runMigration(ctx, cachedTorrents)

	return nil
}

// Stop stops the migration process
func (m *Migrator) Stop() error {
	m.mu.Lock()
	cancel := m.cancelFunc
	done := m.done
	m.mu.Unlock()

	if cancel == nil {
		return nil
	}

	cancel()
	if done != nil {
		<-done
	}
	return nil
}

// GetStatus returns the current migration status
func (m *Migrator) GetStatus() (*storage.SystemMigrationStatus, error) {
	return m.storage.GetMigrationStatus()
}

// GetStats returns migration statistics
func (m *Migrator) GetStats() (map[string]any, error) {
	cachedTorrents, err := m.loadCacheTorrents()
	if err != nil {
		return nil, err
	}

	managedCount, err := m.storage.Count()
	if err != nil {
		return nil, err
	}

	// Count total cache files
	totalCacheFiles := 0
	for _, list := range cachedTorrents {
		totalCacheFiles += len(list)
	}

	return map[string]any{
		"cache_torrents":     len(cachedTorrents),
		"cache_files":        totalCacheFiles,
		"managed_count":      managedCount,
		"multi_debrid_count": m.countMultiDebrid(cachedTorrents),
	}, nil
}

// countMultiDebrid counts how many torrents exist on multiple debrids
func (m *Migrator) countMultiDebrid(torrents map[string][]*storage.CachedTorrent) int {
	count := 0
	for _, list := range torrents {
		if len(list) > 1 {
			count++
		}
	}
	return count
}

// runMigration performs the actual migration
func (m *Migrator) runMigration(ctx context.Context, cachedTorrents map[string][]*storage.CachedTorrent) {
	m.logger.Info().Msg("Starting migration from cache files")

	status, err := m.storage.GetMigrationStatus()
	if err != nil || status == nil {
		m.logger.Error().Err(err).Msg("Failed to load migration status")
		return
	}

	for infohash, cachedList := range cachedTorrents {
		select {
		case <-ctx.Done():
			m.logger.Info().Msg("Migration stopped by user")
			status.Running = false
			status.UpdatedAt = time.Now()
			if err := m.storage.SaveMigrationStatus(status); err != nil {
				m.logger.Error().Err(err).Msg("Failed to save stopped migration status")
			}
			return
		default:
		}

		// Check if already migrated
		exists, err := m.storage.Exists(infohash)
		if err != nil {
			m.logger.Error().Err(err).Str("infohash", infohash).Msg("Failed to check existence")
			status.Errors++
			continue
		}

		if exists {
			status.Completed++
			status.UpdatedAt = time.Now()
			_ = m.storage.SaveMigrationStatus(status)
			continue
		}

		// Merge cache torrents from multiple debrids
		managed, err := m.mergeCachedTorrents(cachedList)
		if err != nil {
			m.logger.Error().Err(err).
				Str("infohash", infohash).
				Int("count", len(cachedList)).
				Msg("Failed to merge cached torrents")
			status.Errors++
			status.ErrorList = append(status.ErrorList, fmt.Sprintf("Failed to merge %s: %v", infohash, err))
			continue
		}

		// Save to new storage
		if err := m.storage.AddOrUpdate(managed); err != nil {
			m.logger.Error().Err(err).Str("infohash", infohash).Msg("Failed to add managed torrent")
			status.Errors++
			status.ErrorList = append(status.ErrorList, fmt.Sprintf("Failed to add %s: %v", managed.Name, err))
			continue
		}
		status.Completed++
		status.UpdatedAt = time.Now()

		// Update status every 10 torrents
		if status.Completed%10 == 0 {
			if err := m.storage.SaveMigrationStatus(status); err != nil {
				m.logger.Error().Err(err).Msg("Failed to update migration status")
			}
		}
	}

	// Final status update
	status.Running = false
	status.UpdatedAt = time.Now()
	_ = m.storage.SaveMigrationStatus(status)

	m.logger.Info().
		Int("total", status.Total).
		Int("completed", status.Completed).
		Int("errors", status.Errors).
		Msg("Migration completed")
}

// loadCacheTorrents loads all torrents from cache directories and groups by infohash
func (m *Migrator) loadCacheTorrents() (map[string][]*storage.CachedTorrent, error) {
	// Map: infohash -> []*CachedTorrent (multiple debrids)
	torrentsByHash := make(map[string][]*storage.CachedTorrent)

	rootInfo, err := os.Lstat(m.cacheDir)
	if err != nil {
		if os.IsNotExist(err) {
			return torrentsByHash, nil
		}
		return nil, fmt.Errorf("inspect legacy cache directory: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, fmt.Errorf("legacy cache root %q is not a regular directory", m.cacheDir)
	}
	rooted, err := os.OpenRoot(m.cacheDir)
	if err != nil {
		return nil, fmt.Errorf("open legacy cache directory: %w", err)
	}
	defer rooted.Close()
	openedRootInfo, err := rooted.Stat(".")
	if err != nil {
		return nil, fmt.Errorf("stat opened legacy cache directory: %w", err)
	}
	if !os.SameFile(rootInfo, openedRootInfo) {
		return nil, fmt.Errorf("legacy cache root changed while opening")
	}
	rootDirectory, err := rooted.Open(".")
	if err != nil {
		return nil, fmt.Errorf("read legacy cache directory: %w", err)
	}
	defer rootDirectory.Close()

	totalEntries := 0
	debridFolders := 0
	cacheFiles := 0
	var totalBytes int64
	for {
		debridDirs, readErr := rootDirectory.ReadDir(legacyCacheReadBatch)
		for _, debridDir := range debridDirs {
			totalEntries++
			if totalEntries > legacyCacheMaxEntries {
				return nil, fmt.Errorf(
					"legacy cache scan exceeds %d entries",
					legacyCacheMaxEntries,
				)
			}
			debridName := debridDir.Name()
			debridInfo, statErr := rooted.Lstat(debridName)
			if statErr != nil {
				return nil, fmt.Errorf("inspect legacy debrid cache %q: %w", debridName, statErr)
			}
			if debridInfo.Mode()&os.ModeSymlink != 0 || !debridInfo.IsDir() {
				continue
			}
			debridFolders++
			if debridFolders > legacyCacheMaxDebridFolders {
				return nil, fmt.Errorf(
					"legacy cache scan exceeds %d provider directories",
					legacyCacheMaxDebridFolders,
				)
			}

			debridDirectory, openErr := rooted.Open(debridName)
			if openErr != nil {
				return nil, fmt.Errorf("open legacy debrid cache %q: %w", debridName, openErr)
			}
			openedDebridInfo, statErr := debridDirectory.Stat()
			if statErr != nil || !os.SameFile(debridInfo, openedDebridInfo) {
				_ = debridDirectory.Close()
				if statErr != nil {
					return nil, fmt.Errorf("stat legacy debrid cache %q: %w", debridName, statErr)
				}
				return nil, fmt.Errorf("legacy debrid cache %q changed while opening", debridName)
			}

			for {
				files, filesReadErr := debridDirectory.ReadDir(legacyCacheReadBatch)
				for _, file := range files {
					totalEntries++
					if totalEntries > legacyCacheMaxEntries {
						_ = debridDirectory.Close()
						return nil, fmt.Errorf(
							"legacy cache scan exceeds %d entries",
							legacyCacheMaxEntries,
						)
					}
					if file.IsDir() || !strings.HasSuffix(file.Name(), ".json") {
						continue
					}
					relative := filepath.Join(debridName, file.Name())
					info, statErr := rooted.Lstat(relative)
					if statErr != nil {
						_ = debridDirectory.Close()
						return nil, fmt.Errorf("inspect legacy cache file %q: %w", relative, statErr)
					}
					if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
						continue
					}
					if info.Size() < 0 || info.Size() > legacyCacheMaxFileBytes {
						_ = debridDirectory.Close()
						return nil, fmt.Errorf(
							"legacy cache file %q has size %d; maximum is %d",
							relative,
							info.Size(),
							legacyCacheMaxFileBytes,
						)
					}
					if info.Size() > legacyCacheMaxTotalBytes-totalBytes {
						_ = debridDirectory.Close()
						return nil, fmt.Errorf(
							"legacy cache metadata exceeds aggregate limit %d",
							legacyCacheMaxTotalBytes,
						)
					}
					cacheFiles++
					if cacheFiles > legacyCacheMaxEntries {
						_ = debridDirectory.Close()
						return nil, fmt.Errorf(
							"legacy cache scan exceeds %d JSON files",
							legacyCacheMaxEntries,
						)
					}

					data, readFileErr := readStableLegacyCacheFile(rooted, relative, info)
					if readFileErr != nil {
						m.logger.Error().
							Err(readFileErr).
							Str("file", filepath.Join(m.cacheDir, relative)).
							Msg("Failed to read cache file")
						continue
					}
					totalBytes += int64(len(data))

					var cached storage.CachedTorrent
					if err := json.Unmarshal(data, &cached); err != nil {
						m.logger.Error().
							Err(err).
							Str("file", filepath.Join(m.cacheDir, relative)).
							Msg("Failed to unmarshal cache file")
						continue
					}
					if cached.InfoHash == "" {
						m.logger.Warn().
							Str("file", filepath.Join(m.cacheDir, relative)).
							Msg("Cache file missing info_hash, skipping")
						continue
					}
					if cached.Debrid == "" {
						cached.Debrid = debridName
					}
					torrentsByHash[cached.InfoHash] = append(
						torrentsByHash[cached.InfoHash],
						&cached,
					)
				}
				switch {
				case errors.Is(filesReadErr, io.EOF):
					if err := debridDirectory.Close(); err != nil {
						return nil, fmt.Errorf("close legacy debrid cache %q: %w", debridName, err)
					}
					debridDirectory = nil
				case filesReadErr != nil:
					_ = debridDirectory.Close()
					return nil, fmt.Errorf("read legacy debrid cache %q: %w", debridName, filesReadErr)
				case len(files) == 0:
					_ = debridDirectory.Close()
					return nil, fmt.Errorf("legacy debrid cache scan made no progress")
				}
				if debridDirectory == nil {
					break
				}
			}
		}
		switch {
		case errors.Is(readErr, io.EOF):
			return torrentsByHash, nil
		case readErr != nil:
			return nil, fmt.Errorf("read legacy cache directory: %w", readErr)
		case len(debridDirs) == 0:
			return nil, fmt.Errorf("legacy cache scan made no progress")
		}
	}
}

func readStableLegacyCacheFile(rooted *os.Root, relative string, before os.FileInfo) ([]byte, error) {
	file, err := rooted.Open(relative)
	if err != nil {
		return nil, err
	}
	opened, statErr := file.Stat()
	if statErr != nil || !os.SameFile(before, opened) || !opened.Mode().IsRegular() {
		_ = file.Close()
		if statErr != nil {
			return nil, statErr
		}
		return nil, fmt.Errorf("legacy cache file changed while opening")
	}
	data, readErr := io.ReadAll(io.LimitReader(file, legacyCacheMaxFileBytes+1))
	after, afterErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if afterErr != nil {
		return nil, afterErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	pathInfo, err := rooted.Lstat(relative)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > legacyCacheMaxFileBytes {
		return nil, fmt.Errorf("legacy cache file exceeds %d bytes", legacyCacheMaxFileBytes)
	}
	if !os.SameFile(before, after) ||
		!os.SameFile(before, pathInfo) ||
		before.Size() != after.Size() ||
		before.Size() != pathInfo.Size() ||
		before.Size() != int64(len(data)) ||
		!before.ModTime().Equal(after.ModTime()) ||
		!before.ModTime().Equal(pathInfo.ModTime()) {
		return nil, fmt.Errorf("legacy cache file changed while reading")
	}
	return data, nil
}

// mergeCachedTorrents merges multiple cache entries (from different debrids) into a single Entry
func (m *Migrator) mergeCachedTorrents(cachedList []*storage.CachedTorrent) (*storage.Entry, error) {
	if len(cachedList) == 0 {
		return nil, fmt.Errorf("empty cached list")
	}

	// Use first as base
	base := cachedList[0]
	managed := base.ToManagedTorrent()

	// AddOrUpdate placements from other debrids
	for i := 1; i < len(cachedList); i++ {
		other := cachedList[i]

		// Check if placement already exists for this debrid+infohash combo
		if _, exists := managed.Providers[other.Debrid]; exists {
			continue
		}

		// Parse timestamp
		addedAt, err := time.Parse(time.RFC3339, other.AddedOn)
		if err != nil {
			addedAt = time.Now()
		}

		// Determine placement status
		status := debridTypes.TorrentStatusDownloaded
		if other.Bad {
			status = debridTypes.TorrentStatusError
		} else if other.IsComplete {
			status = debridTypes.TorrentStatusDownloaded
		}

		// Create placement
		placement := &storage.ProviderEntry{
			Provider: other.Debrid,
			ID:       other.ID,
			AddedAt:  addedAt,
			Status:   status,
			Progress: other.Progress / 100.0,
			Files:    make(map[string]*storage.ProviderFile),
		}

		// Set downloaded timestamp if complete
		if other.IsComplete && other.Status == "downloaded" {
			downloadedAt := addedAt // Use added time as approximation
			placement.DownloadedAt = &downloadedAt
		}

		managed.Providers[other.Debrid] = placement

		// Merge files - add any files not in the base and populate placement files
		if other.Files != nil {
			for fileName, file := range other.Files {
				// AddOrUpdate to global files if not exists
				if _, exists := managed.Files[fileName]; !exists {
					managed.Files[fileName] = &storage.File{
						Name:      fileName,
						Path:      file.LocalPath(),
						Size:      file.Size,
						ByteRange: file.ByteRange,
						Deleted:   file.Deleted,
						InfoHash:  other.InfoHash, // Track which torrent this file came from
						AddedOn:   addedAt,
					}
				}

				// AddOrUpdate placement-specific file data
				placement.Files[fileName] = &storage.ProviderFile{
					Id:   file.Id,
					Link: file.Link,
					Path: file.Path,
				}
			}
		}

		// Update size if other has larger size
		if other.Bytes > managed.Bytes {
			managed.Bytes = other.Bytes
			managed.Size = other.Bytes
		}
	}

	// Activate the most complete placement
	m.activateBestPlacement(managed)

	return managed, nil
}

// activateBestPlacement finds and activates the first placement that is completed
func (m *Migrator) activateBestPlacement(torrent *storage.Entry) {
	for debrid, placement := range torrent.Providers {
		if placement.Status == debridTypes.TorrentStatusDownloaded {
			torrent.ActiveProvider = debrid
			return
		}
	}
}

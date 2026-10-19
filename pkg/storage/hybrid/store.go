// Package hybrid provides a high-performance append-only log storage engine
// with in-memory indexing, LRU caching, and secondary indexes.
//
// Architecture:
//   - Append-only log file for durability
//   - In-memory index with hot fields for O(1) lookups
//   - LRU cache for frequently accessed entries
//   - Secondary indexes for category/provider filtering
//   - Background compaction to reclaim deleted space
//
// Thread Safety:
//   - All operations are thread-safe via RWMutex
//   - Reads can proceed concurrently
//   - Writes are serialized
//
// Durability:
//   - All writes are appended to the log immediately
//   - Index is rebuilt from log on startup (crash recovery)
//   - Optional periodic sync for fsync guarantees
package hybrid

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/rs/zerolog"
)

// Common errors
var (
	ErrStoreClosed          = errors.New("store is closed")
	ErrCorruptedData        = errors.New("corrupted data detected")
	ErrCompactionInProgress = errors.New("compaction already in progress")
	errKeyNotFound          = errors.New("key not found")
)

// IsNotFound reports whether err means the requested key is authoritatively
// absent. Callers must not infer absence from other read failures.
func IsNotFound(err error) bool {
	return errors.Is(err, errKeyNotFound)
}

// Config holds store configuration
type Config struct {
	// DataPath is the append-log file path.
	DataPath string

	// CacheSize is the maximum number of entries to cache (default: 1000)
	CacheSize int

	// SyncInterval is how often to fsync (0 = every write, -1 = never)
	SyncInterval time.Duration

	// CompactionThreshold is the deleted entry ratio that triggers compaction (default: 0.2)
	CompactionThreshold float64

	// AutoCompact enables automatic background compaction
	AutoCompact bool
}

// Store is the main hybrid storage engine
type Store struct {
	mu sync.RWMutex

	// Core components
	log   *appendLog
	index *Index
	cache *lruCache

	// Configuration
	config Config
	logger zerolog.Logger

	// State
	closed     atomic.Bool
	compacting atomic.Bool

	// Background tasks
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Metrics
	stats Stats

	// syncForTest injects final-sync failures without replacing the append-log
	// file. Production instances leave it nil.
	syncForTest func() error

	// compactionPhaseForTest lets subprocess tests terminate at durable
	// replacement boundaries. Production instances leave it nil.
	compactionPhaseForTest func(compactionPhase)
}

// Stats holds operational statistics
type Stats struct {
	Writes      atomic.Int64
	Reads       atomic.Int64
	CacheHits   atomic.Int64
	CacheMisses atomic.Int64
	Deletes     atomic.Int64
	Compactions atomic.Int64
}

// StatsMeta holds operational statistics
type StatsMeta struct {
	Writes      int64
	Reads       int64
	CacheHits   int64
	CacheMisses int64
	Deletes     int64
	Compactions int64
}

// New creates a new Store with the given configuration
func New(config Config) (*Store, error) {
	if config.DataPath == "" {
		return nil, fmt.Errorf("DataPath is required")
	}

	compactionPaths, err := pathsForCompaction(config.DataPath)
	if err != nil {
		return nil, fmt.Errorf("invalid append log path: %w", err)
	}
	if err := recoverCompactionState(compactionPaths); err != nil {
		return nil, fmt.Errorf("recover interrupted compaction: %w", err)
	}
	config.DataPath = compactionPaths.canonical

	// Apply defaults
	if config.CacheSize <= 0 {
		config.CacheSize = 1000
	}
	if config.CompactionThreshold <= 0 {
		config.CompactionThreshold = 0.2
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &Store{
		config: config,
		logger: logger.New("store"),
		ctx:    ctx,
		cancel: cancel,
	}

	// Initialize components
	logPath := config.DataPath

	s.log, err = openAppendLog(logPath)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to open append log: %w", err)
	}

	s.index = newIndex()
	s.cache = newLRUCache(config.CacheSize)

	// Recover index from log
	if err := s.recover(); err != nil {
		closeErr := s.log.Close()
		cancel()
		return nil, errors.Join(
			fmt.Errorf("failed to recover from log: %w", err),
			wrapCompactionError("close append log after recovery failure", closeErr),
		)
	}

	// Auto-compact if log version is outdated (migrates to new format)
	if s.log.version < logVersion && s.index.Len() > 0 {
		if err := s.Compact(); err != nil {
			if s.closed.Load() {
				cancel()
				return nil, fmt.Errorf("failed to auto-compact version upgrade safely: %w", err)
			}
			s.logger.Warn().Err(err).Msg("Failed to auto-compact for version upgrade")
		}
	}

	// Start background tasks
	if config.SyncInterval > 0 {
		s.startSyncTask()
	}
	if config.AutoCompact {
		s.startCompactionTask()
	}
	return s, nil
}

// Close shuts down the store gracefully
func (s *Store) Close() error {
	if s.closed.Swap(true) {
		return nil // Already closed
	}

	// Stop background tasks
	s.cancel()
	s.wg.Wait()

	// Final sync
	s.mu.Lock()
	defer s.mu.Unlock()

	syncErr := s.syncLog()
	if syncErr != nil {
		s.logger.Warn().Err(syncErr).Msg("Failed to sync on close")
	}
	closeErr := s.log.Close()
	return errors.Join(syncErr, closeErr)
}

// Sync flushes all append-log writes completed before this call to durable
// storage. Callers use it at multi-store transaction boundaries; routine
// progress updates continue to use the configured periodic sync.
func (s *Store) Sync() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.syncLog()
}

func (s *Store) syncLog() error {
	if s.syncForTest != nil {
		return s.syncForTest()
	}
	return s.log.Sync()
}

// recover rebuilds the index from the append log
func (s *Store) recover() error {
	err := s.log.Iterate(func(record *LogRecord) error {
		if record.Deleted {
			s.index.Delete(record.Key)
			s.cache.Remove(record.Key)
		} else {
			s.index.Put(record.Key, &IndexEntry{
				Offset:    record.Offset,
				Size:      record.Size,
				Category:  record.Category,
				Provider:  record.Provider,
				Status:    record.Status,
				Name:      record.Name,
				TotalSize: record.TotalSize,
				Protocol:  record.Protocol,
				Bad:       record.Bad,
				AddedOn:   record.AddedOn,
			})
		}
		return nil
	})

	if err != nil {
		return err
	}
	return nil
}

// startSyncTask periodically syncs the log to disk
func (s *Store) startSyncTask() {
	s.wg.Go(func() {
		ticker := time.NewTicker(s.config.SyncInterval)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				s.mu.Lock()
				_ = s.syncLog()
				s.mu.Unlock()
			}
		}
	})
}

// startCompactionTask periodically checks if compaction is needed
func (s *Store) startCompactionTask() {
	s.wg.Go(func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				if s.NeedsCompaction() {
					if err := s.Compact(); err != nil {
						s.logger.Warn().Err(err).Msg("Auto-compaction failed")
					}
				}
			}
		}
	})
}

// Put stores a key-value pair with optional metadata
func (s *Store) Put(key string, value []byte, meta *EntryMeta) error {
	return s.put(key, value, meta, false)
}

// PutExisting updates an existing key without creating it. The existence
// check and append happen under the same store lock so a concurrent delete
// cannot turn a stale update into a resurrected row.
func (s *Store) PutExisting(key string, value []byte, meta *EntryMeta) error {
	return s.put(key, value, meta, true)
}

func (s *Store) put(key string, value []byte, meta *EntryMeta, requireExisting bool) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if requireExisting && s.index.Get(key) == nil {
		return fmt.Errorf("key %s: %w", key, errKeyNotFound)
	}

	// Prepare metadata
	var category, provider, name, status, protocol string
	var totalSize, addedOn int64
	var bad bool
	if meta != nil {
		category = meta.Category
		provider = meta.Provider
		status = meta.Status
		name = meta.Name
		totalSize = meta.TotalSize
		protocol = meta.Protocol
		bad = meta.Bad
		addedOn = meta.AddedOn
	}

	// Write to log
	offset, size, err := s.log.Append(key, value, false, category, provider, status, name, totalSize, protocol, bad, addedOn)
	if err != nil {
		return fmt.Errorf("failed to append to log: %w", err)
	}

	// Update index
	s.index.Put(key, &IndexEntry{
		Offset:    offset,
		Size:      size,
		Category:  category,
		Provider:  provider,
		Status:    status,
		Name:      name,
		TotalSize: totalSize,
		Protocol:  protocol,
		Bad:       bad,
		AddedOn:   addedOn,
	})

	// Invalidate cache (will be populated on next read)
	s.cache.Remove(key)

	s.stats.Writes.Add(1)
	if s.config.SyncInterval == 0 {
		if err := s.syncLog(); err != nil {
			return fmt.Errorf("sync appended record: %w", err)
		}
	}
	return nil
}

// Get retrieves a value by key
func (s *Store) Get(key string) ([]byte, error) {
	if s.closed.Load() {
		return nil, ErrStoreClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check cache first
	if value, ok := s.cache.Get(key); ok {
		s.stats.CacheHits.Add(1)
		return value, nil
	}

	// Look up in index
	entry := s.index.Get(key)
	if entry == nil {
		return nil, fmt.Errorf("key %s: %w", key, errKeyNotFound)
	}

	// Read from log
	value, err := s.log.ReadAt(entry.Offset, entry.Size)
	if err != nil {
		return nil, fmt.Errorf("failed to read from log: %w", err)
	}

	// Populate cache (upgrade to write lock)
	s.mu.RUnlock()
	s.mu.Lock()
	s.cache.Put(key, value)
	s.mu.Unlock()
	s.mu.RLock()

	s.stats.CacheMisses.Add(1)
	s.stats.Reads.Add(1)
	return value, nil
}

// GetMeta retrieves just the metadata without reading the full value
// This is O(1) from the in-memory index - no disk access
func (s *Store) GetMeta(key string) (*IndexEntry, error) {
	if s.closed.Load() {
		return nil, ErrStoreClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entry := s.index.Get(key)
	if entry == nil {
		return nil, fmt.Errorf("key %s: %w", key, errKeyNotFound)
	}

	return entry, nil
}

// Delete removes a key
func (s *Store) Delete(key string) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if key exists
	if s.index.Get(key) == nil {
		return fmt.Errorf("key %s: %w", key, errKeyNotFound)
	}

	// Write tombstone to log
	if _, _, err := s.log.Append(key, nil, true, "", "", "", "", 0, "", false, 0); err != nil {
		return fmt.Errorf("failed to write tombstone: %w", err)
	}

	// Remove from index and cache
	s.index.Delete(key)
	s.cache.Remove(key)

	s.stats.Deletes.Add(1)
	if s.config.SyncInterval == 0 {
		if err := s.syncLog(); err != nil {
			return fmt.Errorf("sync deletion tombstone: %w", err)
		}
	}
	return nil
}

// Exists checks if a key exists (O(1) from index)
func (s *Store) Exists(key string) bool {
	if s.closed.Load() {
		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.index.Get(key) != nil
}

// Len returns the number of entries
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.Len()
}

// Keys returns all keys (snapshot)
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.Keys()
}

// ForEach iterates over all entries in disk order (optimized for sequential
// I/O). The value passed to fn is owned by ForEach and only valid for the
// duration of that call — callers that retain it must copy.
//
// Scans deliberately bypass the LRU cache: a full pass touches every entry once
// and never reuses them, so caching would evict the genuinely-hot working set
// (active streams) and replace it with single-use scan data. Each value is read
// under a short RLock so writers interleave between keys and reads observe the
// current log even across a compaction swap. A single reusable buffer keeps the
// scan allocation-free per record.
func (s *Store) ForEach(fn func(key string, value []byte) error) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.mu.RLock()
	keys := s.index.KeysSortedByOffset()
	s.mu.RUnlock()

	var scratch []byte
	for _, key := range keys {
		s.mu.RLock()
		entry := s.index.Get(key)
		if entry == nil {
			s.mu.RUnlock()
			continue // deleted between the snapshot and now
		}
		value, err := s.log.ReadAtInto(entry.Offset, entry.Size, scratch)
		s.mu.RUnlock()
		if err != nil {
			return fmt.Errorf("failed to read from log: %w", err)
		}
		scratch = value // keep the (possibly grown) buffer for reuse
		s.stats.Reads.Add(1)

		if err := fn(key, value); err != nil {
			return err
		}
	}

	return nil
}

// ForEachGuarded is equivalent to ForEach, but acquires a caller-provided
// per-key guard before reading each current value. The guard is released after
// the value has been read and before fn is called. This lets higher layers bind
// optimistic snapshot tokens without holding their locks across user work.
//
// Returning include=false skips a key. A nil guard behaves like ForEach.
func (s *Store) ForEachGuarded(
	guard func(key string) (release func(), include bool, err error),
	fn func(key string, value []byte) error,
) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.mu.RLock()
	keys := s.index.KeysSortedByOffset()
	s.mu.RUnlock()

	var scratch []byte
	for _, key := range keys {
		var release func()
		if guard != nil {
			var include bool
			var err error
			release, include, err = guard(key)
			if err != nil {
				return err
			}
			if !include {
				if release != nil {
					release()
				}
				continue
			}
		}

		s.mu.RLock()
		entry := s.index.Get(key)
		if entry == nil {
			s.mu.RUnlock()
			if release != nil {
				release()
			}
			continue
		}
		value, err := s.log.ReadAtInto(entry.Offset, entry.Size, scratch)
		s.mu.RUnlock()
		if release != nil {
			release()
		}
		if err != nil {
			return fmt.Errorf("failed to read from log: %w", err)
		}
		scratch = value
		s.stats.Reads.Add(1)

		if err := fn(key, value); err != nil {
			return err
		}
	}

	return nil
}

// ForEachMeta iterates over metadata only (no disk reads)
func (s *Store) ForEachMeta(fn func(key string, meta *IndexEntry) error) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.index.ForEach(fn)
}

// FilterByCategory returns all keys matching a category (O(1) lookup)
func (s *Store) FilterByCategory(category string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.GetByCategory(category)
}

// FilterByProvider returns all keys matching a provider (O(1) lookup)
func (s *Store) FilterByProvider(provider string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.GetByProvider(provider)
}

// CountByCategory returns entry count for a category (O(1))
func (s *Store) CountByCategory(category string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.index.GetByCategory(category))
}

// Categories returns all known categories
func (s *Store) Categories() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.Categories()
}

// NeedsCompaction returns true if compaction would reclaim significant space
func (s *Store) NeedsCompaction() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	liveSize := s.index.TotalSize()
	logSize := s.log.Size()

	if logSize == 0 {
		return false
	}

	deadRatio := 1.0 - (float64(liveSize) / float64(logSize))
	return deadRatio > s.config.CompactionThreshold
}

// Compact removes deleted entries and rewrites the log
func (s *Store) Compact() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}

	if !s.compacting.CompareAndSwap(false, true) {
		return ErrCompactionInProgress
	}
	defer s.compacting.Store(false)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return ErrStoreClosed
	}

	paths, err := pathsForCompaction(s.log.path)
	if err != nil {
		return fmt.Errorf("resolve compaction paths: %w", err)
	}
	if err := ensureCleanCompactionWorkspace(paths); err != nil {
		return fmt.Errorf("prepare compaction workspace: %w", err)
	}
	if err := ensureCanonicalLogIdentity(s.log, paths.canonical); err != nil {
		return err
	}

	// The original canonical log remains the only authority until the compact
	// candidate has been fully written, flushed, and its directory entry made
	// durable.
	if err := s.syncLog(); err != nil {
		return fmt.Errorf("sync canonical log before compaction: %w", err)
	}

	newLog, err := createAppendLogExclusive(paths.compact)
	if err != nil {
		return fmt.Errorf("create compaction log: %w", err)
	}

	// Write all live entries to new log
	newIndex := newIndex()
	keys := s.index.KeysSortedByOffset()

	for _, key := range keys {
		entry := s.index.Get(key)
		if entry == nil {
			continue
		}

		// Read value from old log
		value, err := s.log.ReadAt(entry.Offset, entry.Size)
		if err != nil {
			cleanupErr := cleanupUncommittedCompact(newLog, paths)
			return errors.Join(
				fmt.Errorf("read during compaction: %w", err),
				cleanupErr,
			)
		}

		// Write to new log
		offset, size, err := newLog.Append(key, value, false, entry.Category, entry.Provider, entry.Status, entry.Name, entry.TotalSize, entry.Protocol, entry.Bad, entry.AddedOn)
		if err != nil {
			cleanupErr := cleanupUncommittedCompact(newLog, paths)
			return errors.Join(
				fmt.Errorf("write during compaction: %w", err),
				cleanupErr,
			)
		}

		// Update new index
		newIndex.Put(key, &IndexEntry{
			Offset:    offset,
			Size:      size,
			Category:  entry.Category,
			Provider:  entry.Provider,
			Status:    entry.Status,
			Name:      entry.Name,
			TotalSize: entry.TotalSize,
			Protocol:  entry.Protocol,
			Bad:       entry.Bad,
			AddedOn:   entry.AddedOn,
		})
	}

	// Sync new log
	if err := newLog.Sync(); err != nil {
		cleanupErr := cleanupUncommittedCompact(newLog, paths)
		return errors.Join(
			fmt.Errorf("sync compaction log: %w", err),
			cleanupErr,
		)
	}
	if err := syncParentDirectory(paths.parent); err != nil {
		cleanupErr := cleanupUncommittedCompact(newLog, paths)
		return errors.Join(
			fmt.Errorf("sync staged compaction directory: %w", err),
			cleanupErr,
		)
	}
	s.reachCompactionPhase(compactionPhaseStaged)

	oldLog := s.log
	result := s.installCompactedLog(oldLog, newLog, paths)
	if result.log == nil {
		// Replacement code could not reopen either authoritative generation.
		// Fail closed so callers never read with an index that may not match the
		// bytes currently named by the canonical path.
		s.closed.Store(true)
		s.cancel()
		return errors.Join(
			result.err,
			errors.New("compaction left no usable append-log handle; store closed"),
		)
	}
	s.log = result.log
	if !result.committed {
		return result.err
	}

	// The durable namespace replacement succeeded. Only now expose offsets from
	// the compacted file to readers.
	s.index = newIndex
	s.cache.Clear()
	s.stats.Compactions.Add(1)
	return result.err
}

func (s *Store) GetStats() StatsMeta {
	return StatsMeta{
		Writes:      s.stats.Writes.Load(),
		Reads:       s.stats.Reads.Load(),
		CacheHits:   s.stats.CacheHits.Load(),
		CacheMisses: s.stats.CacheMisses.Load(),
		Deletes:     s.stats.Deletes.Load(),
		Compactions: s.stats.Compactions.Load(),
	}
}

// DiskSize returns the current log file size
func (s *Store) DiskSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.log.Size()
}

// MemoryUsage returns approximate in-memory usage
func (s *Store) MemoryUsage() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.MemoryUsage() + s.cache.MemoryUsage()
}

// EntryMeta holds metadata for a stored entry
type EntryMeta struct {
	Category  string
	Provider  string
	Status    string
	Name      string
	TotalSize int64
	Protocol  string // "torrent" or "nzb"
	Bad       bool
	AddedOn   int64 // Unix timestamp
}

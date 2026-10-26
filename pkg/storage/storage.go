package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/internal/stateowner"
	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

var storeNames = []string{
	"entries",
	"queue",
	"items",
	"storage_state",
	"repair_state",
	"repair_runs",
	"repair_recoveries",
	"entry_tombstones",
	"queue_tombstones",
	"migration_cleanups",
}

// legacyStoreNames are buckets from the v1 repair system. They are removed
// on startup so they don't accumulate dead data.
var legacyStoreNames = []string{"repair_jobs", "repair_keys"}

// Storage handles persistence using HybridStore
type Storage struct {
	cleanupReceiptForTest func()
	owner                 *stateowner.Owner
	closeOnce             sync.Once
	closeErr              error
	entries               *hybrid.Store
	queue                 *hybrid.Store
	entryItems            *hybrid.Store
	storageState          *hybrid.Store
	// Durability hooks inject sync failures; production leaves them nil.
	handoffSyncForTest            func() error
	mainCleanupSyncForTest        func() error
	mainRetirementSyncForTest     func(string) error
	queueRowSyncForTest           func() error
	queueIntentSyncForTest        func() error
	submissionSyncForTest         func() error
	healthAliasSyncedForTest      func()
	healthRetirementForTest       func(string)
	healthAliasPhaseForTest       func(string)
	healthRenameForTest           func(string)
	healthOwnerArchiveForTest     func(string)
	healthOwnerArchiveSyncForTest func(string) error
	// Per-key durability latches survive lifecycle-state eviction. Operations
	// on each key are serialized by its lifecycle lock.
	handoffPending   sync.Map
	repairState      *hybrid.Store
	repairRuns       *hybrid.Store
	repairRecoveries *hybrid.Store
	entryTombstones  *hybrid.Store
	queueTombstones  *hybrid.Store
	migrationCleanup *hybrid.Store
	mainEntries      *mainEntryLifecycle
	dir              string
	logger           zerolog.Logger

	// Lock order: per-key lifecycle -> placementPublicationMu -> entryItemsMu
	// -> entryHealthMu -> hybrid store locks. Never take a lifecycle lock while holding this
	// mutex, and never retain it across provider/network work.
	placementPublicationMu sync.Mutex
	entryItemsMu           sync.Mutex
	entryItemsDirty        bool
	startupComplete        bool
	entryHealthMu          sync.Mutex
	healthIdentityIndexed  bool // Guarded by entryHealthMu; rebuilt on each open.
	healthCountsMu         sync.Mutex
	healthCounts           map[HealthStatus]int
	healthCountsBuiltAt    time.Time
	migrationCleanupMu     sync.Mutex
	torrentSourcesMu       sync.Mutex
	torrentSourceBytes     int64
	providerPlacementMu    sync.Mutex // Lifecycle gate precedes this mutex; never held across provider calls.
}

func createItemStores(baseDir string, baseConfig hybrid.Config) (map[string]*hybrid.Store, error) {
	items := make(map[string]*hybrid.Store)
	for _, name := range storeNames {
		config := baseConfig
		if name == "repair_recoveries" {
			// Arr mutations must never outrun their durable intent record.
			config.SyncInterval = 0
		}
		config.DataPath = filepath.Join(baseDir, name+".db")
		store, err := hybrid.New(config)
		if err != nil {
			for _, it := range items {
				_ = it.Close()
			}
			return nil, fmt.Errorf("failed to create %s store: %w", name, err)
		}
		items[name] = store
	}
	return items, nil
}

func dropLegacyStores(baseDir string, log zerolog.Logger) {
	rooted, absoluteRoot, err := safepath.OpenRoot(baseDir)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to open storage root for legacy cleanup")
		return
	}
	defer rooted.Close()
	for _, name := range legacyStoreNames {
		relative := name + ".db"
		if _, err := rooted.Lstat(relative); os.IsNotExist(err) {
			continue
		} else if err != nil {
			log.Warn().Err(err).Str("store", relative).Msg("Failed to inspect legacy repair bucket")
			continue
		}
		if err := rooted.RemoveAll(relative); err != nil {
			log.Warn().Err(err).Str("store", relative).Msg("Failed to remove legacy repair bucket")
		} else {
			log.Info().Str("path", filepath.Join(absoluteRoot, relative)).Msg("Removed legacy repair bucket")
		}
	}
}

func NewStorage(dbPath string) (*Storage, error) {
	var err error
	dbPath, err = safepath.EnsureRoot(dbPath, 0o755)
	if err != nil {
		return nil, fmt.Errorf("failed to create db directory: %w", err)
	}

	log := logger.New("storage")
	owner, err := stateowner.Acquire(dbPath, ".tessarr-storage.lock")
	if err != nil {
		return nil, fmt.Errorf("open storage writer: %w", err)
	}

	dropLegacyStores(dbPath, log)

	baseConfig := hybrid.Config{
		CacheSize:           5000,
		SyncInterval:        time.Second,
		CompactionThreshold: 0.5,
		AutoCompact:         true,
	}

	itemStores, err := createItemStores(dbPath, baseConfig)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to create item stores: %w", err), owner.Close())
	}

	s := &Storage{
		owner:            owner,
		entries:          itemStores["entries"],
		queue:            itemStores["queue"],
		entryItems:       itemStores["items"],
		storageState:     itemStores["storage_state"],
		repairState:      itemStores["repair_state"],
		repairRuns:       itemStores["repair_runs"],
		repairRecoveries: itemStores["repair_recoveries"],
		entryTombstones:  itemStores["entry_tombstones"],
		queueTombstones:  itemStores["queue_tombstones"],
		migrationCleanup: itemStores["migration_cleanups"],
		mainEntries:      newMainEntryLifecycle(),
		dir:              dbPath,
		logger:           log,
	}

	if err := s.recoverProviderPlacementAccounts(); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("recover provider placement accounts: %w", err)
	}

	needsEntryItemRecovery, err := s.beginEntryItemSession()
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("start entry-item recovery session: %w", err)
	}
	needsSharedFolderUpgrade, err := s.needsEntryItemSharedFolderUpgrade()
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("inspect entry-item integrity version: %w", err)
	}
	if needsEntryItemRecovery {
		if _, err := s.reconcileEntryItems(); err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("recover entry-item index: %w", err)
		}
	}

	if err := s.recoverMainEntryTombstones(); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("recover main entry deletions: %w", err)
	}

	if count, err := s.MigrateMetadata(); err != nil {
		log.Warn().Err(err).Msg("Metadata migration failed")
	} else if count > 0 {
		log.Info().Int("count", count).Msg("Migrated entry metadata to new format")
	}
	if needsSharedFolderUpgrade {
		if !needsEntryItemRecovery {
			repaired, err := s.reconcileEntryItems()
			if err != nil {
				_ = s.Close()
				return nil, fmt.Errorf("upgrade shared-folder entry-item index: %w", err)
			}
			if repaired > 0 {
				log.Info().Int("count", repaired).
					Msg("Repaired shared-folder entry-item index")
			}
		}
		if err := s.markEntryItemSharedFolderUpgrade(); err != nil {
			_ = s.Close()
			return nil, err
		}
	}
	if err := s.reconcileHealthRetirements(); err != nil {
		_ = s.Close()
		return nil, err
	}
	if err := s.PruneTorrentSources(); err != nil {
		log.Warn().Err(err).Msg("Failed to prune unreferenced torrent sources")
	}

	s.startupComplete = true
	return s, nil
}

func (s *Storage) Close() error {
	s.closeOnce.Do(func() { s.closeErr = s.closeStores() })
	return s.closeErr
}

func (s *Storage) closeStores() error {
	var errs []error
	if s.startupComplete {
		if err := s.markEntryItemsClean(); err != nil {
			errs = append(errs, err)
		}
	}
	stores := []*hybrid.Store{
		s.entries,
		s.queue,
		s.entryItems,
		s.storageState,
		s.repairState,
		s.repairRuns,
		s.repairRecoveries,
		s.entryTombstones,
		s.queueTombstones,
		s.migrationCleanup,
	}
	for _, store := range stores {
		if store == nil {
			continue
		}
		if err := store.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := s.owner.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing storage: %w", errors.Join(errs...))
	}
	return nil
}

// DurabilityError exposes component health without inferring it from row
// visibility. An indexed append may still be awaiting a successful fsync.
func (s *Storage) DurabilityError() error {
	for _, store := range []*hybrid.Store{s.entries, s.queue, s.entryItems, s.storageState,
		s.repairState, s.repairRuns, s.repairRecoveries, s.entryTombstones, s.queueTombstones, s.migrationCleanup} {
		if store != nil {
			if err := store.DurabilityError(); err != nil {
				return err
			}
		}
	}
	return nil
}

// DiskSize returns the total on-disk size of all stores (O(1), no filesystem walk).
func (s *Storage) DiskSize() int64 {
	var size int64
	for _, store := range []*hybrid.Store{
		s.entries,
		s.queue,
		s.entryItems,
		s.storageState,
		s.repairState,
		s.repairRuns,
		s.repairRecoveries,
		s.entryTombstones,
		s.queueTombstones,
		s.migrationCleanup,
	} {
		if store != nil {
			size += store.DiskSize()
		}
	}
	return size
}

// SaveMigrationStatus saves the system migration status
func (s *Storage) SaveMigrationStatus(status *SystemMigrationStatus) error {
	pb := SystemMigrationStatusToProto(status)
	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	return s.entries.Put("__migration_status__", data, nil)
}

// GetMigrationStatus retrieves the system migration status
func (s *Storage) GetMigrationStatus() (*SystemMigrationStatus, error) {
	data, err := s.entries.Get("__migration_status__")
	if err != nil {
		return nil, err
	}
	var pb SystemMigrationStatusProto
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, err
	}
	return ProtoToSystemMigrationStatus(&pb), nil
}

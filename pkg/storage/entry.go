package storage

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrEntryNotFound identifies an authoritative main-storage miss. Storage
	// I/O, decode, and closed-store errors remain distinct so cleanup does not
	// proceed after an indeterminate lookup.
	ErrEntryNotFound = errors.New("entry not found")

	// ErrEntryIdentityMismatch means a main-storage key and serialized entry
	// disagree. Provider and filesystem cleanup must fail closed.
	ErrEntryIdentityMismatch = errors.New("entry identity does not match storage key")

	// ErrQueuedEntryNotFound is the equivalent authoritative queue miss.
	ErrQueuedEntryNotFound = errors.New("queued entry not found")

	// ErrQueuedEntryIdentityMismatch means the durable queue key and serialized
	// entry disagree.
	ErrQueuedEntryIdentityMismatch = errors.New("queued entry identity does not match storage key")
)

func IsEntryNotFound(err error) bool {
	return errors.Is(err, ErrEntryNotFound)
}

func IsQueuedEntryNotFound(err error) bool {
	return errors.Is(err, ErrQueuedEntryNotFound)
}

func decodeEntryForKey(key string, data []byte, identityErr error, kind string) (*Entry, error) {
	var pb EntryProto
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, fmt.Errorf("decode %s entry %s: %w", kind, key, err)
	}
	entry := ProtoToEntry(&pb)
	if entry == nil || !strings.EqualFold(entry.InfoHash, key) {
		payloadID := ""
		if entry != nil {
			payloadID = entry.InfoHash
		}
		return nil, fmt.Errorf(
			"%w: key %q contains %q",
			identityErr,
			key,
			payloadID,
		)
	}
	// Bind downstream cleanup and lifecycle operations to the authoritative
	// durable key, not to casing supplied in the serialized payload.
	entry.InfoHash = key
	return entry, nil
}

func decodeMainEntry(key string, data []byte) (*Entry, error) {
	return decodeEntryForKey(key, data, ErrEntryIdentityMismatch, "main")
}

func decodeQueuedEntry(key string, data []byte) (*Entry, error) {
	return decodeEntryForKey(key, data, ErrQueuedEntryIdentityMismatch, "queued")
}

// AddOrUpdate adds or updates an entry
func (s *Storage) AddOrUpdate(entry *Entry) error {
	if entry == nil {
		return fmt.Errorf("main entry is nil")
	}
	entry.InfoHash = normalizeMainEntryKey(entry.InfoHash)
	ref, err := s.acquireMainEntryState(entry.InfoHash)
	if err != nil {
		return err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.deleting {
		return fmt.Errorf("%w: %s", ErrEntryDeleting, ref.key)
	}
	tombstone, _, err := s.loadMainEntryTombstone(ref.key)
	if err != nil {
		return err
	}
	if tombstone != nil && len(tombstone.PlacementSnapshot) != 0 {
		return fmt.Errorf("%w: administrative provider cleanup pending", ErrEntryDeleting)
	}
	exists := s.entries.Exists(entry.InfoHash)
	wasUnbound := entry.MainGeneration == 0
	if entry.MainGeneration == 0 {
		if state.seen || state.retired || exists {
			return staleMainEntryError(ref.key, 0, state.generation)
		}
		entry.MainGeneration = state.generation
	} else if entry.MainGeneration != state.generation {
		return staleMainEntryError(ref.key, entry.MainGeneration, state.generation)
	}

	if state.retired {
		provider := normalizeMainEntryProvider(entry.MainMutationProvider)
		absentAt := state.absentAt[provider]
		_, durableAbsence := state.durableAbsent[provider]
		providerAuthorized := durableAbsence &&
			(absentAt == 0 || entry.MainProviderSnapshot > absentAt)
		providerAuthorized = providerAuthorized &&
			provider != "" &&
			entry.MainProviderSnapshot != 0
		queueIncarnation := strings.TrimSpace(entry.MainReimportIncarnation)
		queueAuthorized := queueIncarnation != "" &&
			queueIncarnation == strings.TrimSpace(entry.QueueIncarnation) &&
			queueIncarnation == state.authorizedQueueIncarn
		if !providerAuthorized && !queueAuthorized {
			return fmt.Errorf(
				"%w: %s from provider %q at snapshot %d and queue incarnation %q",
				ErrEntryRediscoveryPending,
				ref.key,
				provider,
				entry.MainProviderSnapshot,
				queueIncarnation,
			)
		}
	}

	if state.retired {
		// Persist the transition before writing the replacement. Recovery can
		// then distinguish an interrupted replacement from an interrupted
		// deletion without inferring from row presence alone.
		if err := s.persistMainEntryTombstone(
			ref.key,
			state,
			"",
			mainEntryTombstoneReplacementPending,
		); err != nil {
			return fmt.Errorf("persist pending main entry rediscovery: %w", err)
		}
		if err := s.entryTombstones.Sync(); err != nil {
			return fmt.Errorf("sync pending main entry rediscovery: %w", err)
		}
	}

	if err := s.addOrUpdateMainRaw(entry); err != nil {
		if wasUnbound {
			entry.MainGeneration = 0
		}
		if state.retired {
			rollbackErr := s.restoreRetiredMainEntryTombstone(ref.key, state)
			return errors.Join(err, rollbackErr)
		}
		return err
	}

	if state.retired {
		// Make the authorized replacement durable before clearing the durable
		// retirement handshake. A crash can therefore leave a conservative
		// tombstone, never an unprotected provider resurrection.
		if err := s.entries.Sync(); err != nil {
			return fmt.Errorf("sync rediscovered main entry: %w", err)
		}
		if err := s.clearMainEntryTombstone(ref.key); err != nil {
			return fmt.Errorf("clear rediscovered main entry tombstone: %w", err)
		}
		if err := s.entryTombstones.Sync(); err != nil {
			return fmt.Errorf("sync cleared main entry tombstone: %w", err)
		}
	}

	state.seen = true
	state.retired = false
	state.retiredAt = 0
	clear(state.absentAt)
	clear(state.durableAbsent)
	state.authorizedQueueIncarn = ""
	state.generation = s.mainEntries.nextSequence()
	bindMainEntrySnapshot(entry, state.generation)
	return nil
}

func (s *Storage) addOrUpdateMainRaw(entry *Entry) error {
	s.placementPublicationMu.Lock()
	defer s.placementPublicationMu.Unlock()
	if err := s.validateMainPlacementPublication(entry); err != nil {
		return err
	}
	entry.UpdatedAt = time.Now()

	var previous *Entry
	var err error
	if s.entries.Exists(entry.InfoHash) {
		previous, err = s.getMainRaw(entry.InfoHash)
		if err != nil {
			return fmt.Errorf("load current main entry before update: %w", err)
		}
	}
	if err := assignFileIDs(entry, previous); err != nil {
		return err
	}

	// Serialize
	pb := EntryToProto(entry)
	data, err := proto.Marshal(pb)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	meta := &hybrid.EntryMeta{
		Category:  entry.Category,
		Provider:  entry.ActiveProvider,
		Status:    string(entry.Status),
		Name:      entry.GetFolder(), // Store computed folder name for fast listings
		TotalSize: entry.Size,
		Protocol:  string(entry.Protocol),
		Bad:       entry.Bad,
		AddedOn:   entry.AddedOn.Unix(),
	}

	s.entryItemsMu.Lock()
	defer s.entryItemsMu.Unlock()
	snapshots, err := s.mutateEntryItems(previous, entry)
	if err != nil {
		return err
	}
	if err := s.entries.Put(entry.InfoHash, data, meta); err != nil {
		restoreErr := s.restoreEntryItemSnapshots(snapshots)
		return errors.Join(
			fmt.Errorf("write main entry: %w", err),
			restoreErr,
		)
	}
	s.markEntryItemMutation(previous, entry)
	return nil
}

const fileIDBytes = 16

// NewFileID returns a collision-resistant stable file identity.
func NewFileID() (string, error) {
	bytes := make([]byte, fileIDBytes)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate file id: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

// assignFileIDs preserves IDs from the durable version of an entry and gives
// only genuinely new files a fresh identity. Duplicate IDs fail closed because
// an ambiguous identity would let one signed URL select the wrong file.
func assignFileIDs(entry, previous *Entry) error {
	if entry == nil {
		return fmt.Errorf("assign file ids: entry is nil")
	}
	used := make(map[string]string, len(entry.Files))
	for name, file := range entry.Files {
		if file == nil {
			return fmt.Errorf("assign file ids: file %q is nil", name)
		}
		if file.ID == "" && previous != nil {
			file.ID = previousFileID(entry, previous, name, file)
		}
		if file.ID == "" {
			for {
				id, err := NewFileID()
				if err != nil {
					return err
				}
				if _, exists := used[id]; !exists {
					file.ID = id
					break
				}
			}
		}
		if other, duplicate := used[file.ID]; duplicate {
			return fmt.Errorf("assign file ids: files %q and %q share id %q", other, name, file.ID)
		}
		used[file.ID] = name
	}
	return nil
}

// previousFileID carries identity through provider refreshes and safe renames.
// Exact map/name matches win; otherwise only a unique provider ID, path, or
// byte-range match is accepted. Ambiguity deliberately produces a new ID.
func previousFileID(entry, previous *Entry, name string, file *File) string {
	if old := previous.Files[name]; old != nil && old.ID != "" {
		return old.ID
	}
	if id := uniquePreviousFileID(previous, func(_ string, old *File) bool {
		return old.Name == file.Name
	}); id != "" {
		return id
	}
	if id := previousProviderFileID(entry, previous, name, file.Name); id != "" {
		return id
	}
	if file.Path != "" {
		pathKey := strings.ToLower(filepath.ToSlash(filepath.Clean(file.Path)))
		if id := uniquePreviousFileID(previous, func(_ string, old *File) bool {
			return old.Path != "" && old.Size == file.Size &&
				strings.ToLower(filepath.ToSlash(filepath.Clean(old.Path))) == pathKey
		}); id != "" {
			return id
		}
	}
	if file.ByteRange != nil {
		if id := uniquePreviousFileID(previous, func(_ string, old *File) bool {
			return old.ByteRange != nil && old.Size == file.Size && *old.ByteRange == *file.ByteRange
		}); id != "" {
			return id
		}
	}
	return ""
}

func uniquePreviousFileID(previous *Entry, match func(string, *File) bool) string {
	var found string
	for name, old := range previous.Files {
		if old == nil || old.ID == "" || !match(name, old) {
			continue
		}
		if found != "" && found != old.ID {
			return ""
		}
		found = old.ID
	}
	return found
}

func previousProviderFileID(entry, previous *Entry, mapName, fileName string) string {
	var found string
	for providerName, currentProvider := range entry.Providers {
		if currentProvider == nil {
			continue
		}
		currentFile := currentProvider.Files[mapName]
		if currentFile == nil {
			currentFile = currentProvider.Files[fileName]
		}
		if currentFile == nil || currentFile.Id == "" {
			continue
		}
		previousProvider := previous.Providers[providerName]
		if previousProvider == nil {
			for _, candidate := range previous.Providers {
				if candidate != nil && candidate.Provider == currentProvider.Provider {
					previousProvider = candidate
					break
				}
			}
		}
		if previousProvider == nil {
			continue
		}
		for oldName, oldProviderFile := range previousProvider.Files {
			if oldProviderFile == nil || oldProviderFile.Id != currentFile.Id {
				continue
			}
			old := previous.Files[oldName]
			if old == nil || old.ID == "" {
				continue
			}
			if found != "" && found != old.ID {
				return ""
			}
			found = old.ID
		}
	}
	return found
}

// BatchAddOrUpdate adds or updates multiple entries
func (s *Storage) BatchAddOrUpdate(entries []*Entry) error {
	if len(entries) == 0 {
		return nil
	}
	for _, entry := range entries {
		if err := s.AddOrUpdate(entry); err != nil {
			return err
		}
	}
	if err := s.entries.Sync(); err != nil {
		return fmt.Errorf("sync main entry batch: %w", err)
	}
	return nil
}

// AddOrUpdateDurable commits one main-entry mutation and flushes it before
// returning. Completion paths use this before removing their recoverable input.
func (s *Storage) AddOrUpdateDurable(entry *Entry) error {
	if err := s.AddOrUpdate(entry); err != nil {
		return err
	}
	if err := s.entries.Sync(); err != nil {
		return fmt.Errorf("sync main entry: %w", err)
	}
	return nil
}

// Exists checks if an entry exists
func (s *Storage) Exists(infohash string) (bool, error) {
	infohash = normalizeMainEntryKey(infohash)
	ref, err := s.acquireMainEntryState(infohash)
	if err != nil {
		return false, err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleting {
		return false, fmt.Errorf("%w: %s", ErrEntryDeleting, ref.key)
	}
	return s.entries.Exists(infohash), nil
}

// Get retrieves an entry by InfoHash
func (s *Storage) Get(infohash string) (*Entry, error) {
	infohash = normalizeMainEntryKey(infohash)
	ref, err := s.acquireMainEntryState(infohash)
	if err != nil {
		return nil, err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleting {
		return nil, fmt.Errorf("%w: %s", ErrEntryDeleting, ref.key)
	}

	entry, err := s.getMainRaw(infohash)
	if err != nil {
		return nil, err
	}
	state.seen = true
	bindMainEntrySnapshot(entry, state.generation)
	return entry, nil
}

func (s *Storage) getMainRaw(infohash string) (*Entry, error) {
	data, err := s.entries.Get(infohash)
	if err != nil {
		if hybrid.IsNotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrEntryNotFound, infohash)
		}
		return nil, err
	}
	return decodeMainEntry(infohash, data)
}

// List retrieves all cached entries with optional filtering
func (s *Storage) List(filter func(*Entry) bool) ([]*Entry, error) {
	var entries []*Entry

	err := s.forEachMainEntry(func(entry *Entry) error {
		if filter == nil || filter(entry) {
			entries = append(entries, entry)
		}
		return nil
	})

	return entries, err
}

// ForEach iterates over entries
func (s *Storage) ForEach(fn func(*Entry) error) error {
	return s.forEachMainEntry(fn)
}

func (s *Storage) forEachMainEntry(fn func(*Entry) error) error {
	if fn == nil {
		return nil
	}

	var generation uint64
	return s.entries.ForEachGuarded(
		func(key string) (func(), bool, error) {
			ref, err := s.acquireMainEntryState(key)
			if err != nil {
				return nil, false, err
			}
			state := ref.state
			state.mu.Lock()
			if state.deleting {
				state.mu.Unlock()
				ref.release()
				return nil, false, nil
			}
			state.seen = true
			generation = state.generation
			return func() {
				state.mu.Unlock()
				ref.release()
			}, true, nil
		},
		func(key string, value []byte) error {
			entry, err := decodeMainEntry(key, value)
			if err != nil {
				return err
			}
			bindMainEntrySnapshot(entry, generation)
			return fn(entry)
		},
	)
}

// ForEachBatch iterates over entries in batches
func (s *Storage) ForEachBatch(batchSize int, fn func([]*Entry) error) error {
	batch := make([]*Entry, 0, batchSize)

	err := s.forEachMainEntry(func(entry *Entry) error {
		batch = append(batch, entry)

		if len(batch) >= batchSize {
			if err := fn(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
		return nil
	})

	if err == nil && len(batch) > 0 {
		err = fn(batch)
	}
	return err
}

// EntryMetaInfo is a lightweight struct for folder listings (no disk reads)
type EntryMetaInfo struct {
	InfoHash string
	Name     string
	Size     int64
	AddedOn  time.Time
	Provider string
	Protocol string
	Bad      bool
}

// ForEachMeta iterates over entry metadata without reading full entries from disk.
// This is O(n) in-memory only - no disk reads, no protobuf deserialization.
func (s *Storage) ForEachMeta(fn func(*EntryMetaInfo) error) error {
	return s.entries.ForEachMeta(func(key string, meta *hybrid.IndexEntry) error {
		return fn(&EntryMetaInfo{
			InfoHash: key,
			Name:     meta.Name,
			Size:     meta.TotalSize,
			AddedOn:  time.Unix(meta.AddedOn, 0),
			Provider: meta.Provider,
			Protocol: meta.Protocol,
			Bad:      meta.Bad,
		})
	})
}

// MigrateMetadata re-saves all entries to populate the new metadata fields
// (Protocol, Bad, AddedOn, computed folder Name) in the index.
// This is a one-time migration for existing data.
// Returns the number of entries migrated and any error.
func (s *Storage) MigrateMetadata() (int, error) {
	// First, collect all keys that need migration
	// We check if Protocol is empty as indicator of unmigrated data
	var keysToMigrate []string
	_ = s.entries.ForEachMeta(func(key string, meta *hybrid.IndexEntry) error {
		// Skip special keys
		if strings.HasPrefix(key, "__") {
			return nil
		}
		// Check if metadata needs migration (Protocol empty = old format)
		if meta.Protocol == "" {
			keysToMigrate = append(keysToMigrate, key)
		}
		return nil
	})

	if len(keysToMigrate) == 0 {
		return 0, nil
	}

	// Migrate each entry by reading and re-saving
	migrated := 0
	for _, key := range keysToMigrate {
		entry, err := s.Get(key)
		if err != nil {
			continue // Skip entries that can't be read
		}

		// Re-save to update metadata
		if err := s.AddOrUpdate(entry); err != nil {
			continue
		}
		migrated++
	}

	return migrated, nil
}

// Delete removes an entry
func (s *Storage) Delete(infohash string) error {
	return s.DeleteWithCleanup(infohash, nil)
}

// DeleteWithCleanup durably records deletion intent before external cleanup,
// then releases the mutex while that cleanup runs. A crash during cleanup
// finishes retiring the local row on restart. If cleanup returns an error, the
// durable intent is rolled back before the unchanged generation is reopened.
func (s *Storage) DeleteWithCleanup(infohash string, cleanup func(*Entry) error) (err error) {
	infohash = normalizeMainEntryKey(infohash)
	ref, err := s.acquireMainEntryState(infohash)
	if err != nil {
		return err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	if state.deleting {
		state.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrEntryDeleting, ref.key)
	}
	entry, err := s.getMainRaw(infohash)
	if err != nil {
		state.mu.Unlock()
		return fmt.Errorf("load entry for deletion: %w", err)
	}
	state.seen = true
	bindMainEntrySnapshot(entry, state.generation)
	state.deleting = true
	if err := s.persistMainEntryTombstone(
		ref.key,
		state,
		"",
		mainEntryTombstoneDeleting,
	); err != nil {
		rollbackErr := s.rollbackMainEntryTombstone(ref.key)
		if rollbackErr == nil {
			state.deleting = false
		}
		state.mu.Unlock()
		return errors.Join(
			fmt.Errorf("persist main entry deletion tombstone: %w", err),
			rollbackErr,
		)
	}
	if err := s.entryTombstones.Sync(); err != nil {
		rollbackErr := s.rollbackMainEntryTombstone(ref.key)
		if rollbackErr == nil {
			state.deleting = false
		}
		state.mu.Unlock()
		return errors.Join(
			fmt.Errorf("sync main entry deletion tombstone: %w", err),
			rollbackErr,
		)
	}
	state.mu.Unlock()

	if cleanup != nil {
		if cleanupErr := cleanup(entry); cleanupErr != nil {
			state.mu.Lock()
			rollbackErr := s.rollbackMainEntryTombstone(ref.key)
			if rollbackErr == nil {
				state.deleting = false
			}
			state.mu.Unlock()
			return errors.Join(
				fmt.Errorf("cleanup main entry %s: %w", ref.key, cleanupErr),
				rollbackErr,
			)
		}
	}

	state.mu.Lock()
	if err := s.deleteMainRaw(entry); err != nil {
		rollbackErr := s.rollbackMainEntryTombstone(ref.key)
		if rollbackErr == nil {
			state.deleting = false
		}
		state.mu.Unlock()
		return errors.Join(err, rollbackErr)
	}
	if err := s.entries.Sync(); err != nil {
		restoreErr := s.restoreMainEntryAfterSyncFailure(entry)
		rollbackErr := s.rollbackMainEntryTombstone(ref.key)
		if restoreErr == nil && rollbackErr == nil {
			state.deleting = false
		}
		state.mu.Unlock()
		return errors.Join(
			fmt.Errorf("sync deleted main entry: %w", err),
			restoreErr,
			rollbackErr,
		)
	}
	clear(state.absentAt)
	clear(state.durableAbsent)
	state.authorizedQueueIncarn = ""
	if err := s.persistMainEntryTombstone(
		ref.key,
		state,
		"",
		mainEntryTombstoneRetired,
	); err != nil {
		restoreErr := s.restoreMainEntryAfterSyncFailure(entry)
		rollbackErr := s.rollbackMainEntryTombstone(ref.key)
		if restoreErr == nil && rollbackErr == nil {
			state.deleting = false
		}
		state.mu.Unlock()
		return errors.Join(
			fmt.Errorf("finalize main entry retirement: %w", err),
			restoreErr,
			rollbackErr,
		)
	}
	if err := s.entryTombstones.Sync(); err != nil {
		restoreErr := s.restoreMainEntryAfterSyncFailure(entry)
		rollbackErr := s.rollbackMainEntryTombstone(ref.key)
		if restoreErr == nil && rollbackErr == nil {
			state.deleting = false
		}
		state.mu.Unlock()
		return errors.Join(
			fmt.Errorf("sync finalized main entry retirement: %w", err),
			restoreErr,
			rollbackErr,
		)
	}
	state.generation = s.mainEntries.nextSequence()
	state.retiredAt = state.generation
	state.retired = true
	state.deleting = false
	state.mu.Unlock()
	return nil
}

// DeleteSnapshot deletes only the exact current generation represented by
// entry. Provider refresh uses this path so a stale "provider absent" decision
// cannot delete a row concurrently updated by another provider.
func (s *Storage) DeleteSnapshot(entry *Entry) error {
	if entry == nil {
		return fmt.Errorf("main entry is nil")
	}
	entry.InfoHash = normalizeMainEntryKey(entry.InfoHash)
	ref, err := s.acquireMainEntryState(entry.InfoHash)
	if err != nil {
		return err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleting {
		return fmt.Errorf("%w: %s", ErrEntryDeleting, ref.key)
	}
	if entry.MainGeneration == 0 || entry.MainGeneration != state.generation {
		return staleMainEntryError(ref.key, entry.MainGeneration, state.generation)
	}
	state.deleting = true
	if err := s.persistMainEntryTombstone(
		ref.key,
		state,
		"",
		mainEntryTombstoneDeleting,
	); err != nil {
		rollbackErr := s.rollbackMainEntryTombstone(ref.key)
		if rollbackErr == nil {
			state.deleting = false
		}
		return errors.Join(
			fmt.Errorf("persist main entry deletion tombstone: %w", err),
			rollbackErr,
		)
	}
	if err := s.entryTombstones.Sync(); err != nil {
		rollbackErr := s.rollbackMainEntryTombstone(ref.key)
		if rollbackErr == nil {
			state.deleting = false
		}
		return errors.Join(
			fmt.Errorf("sync main entry deletion tombstone: %w", err),
			rollbackErr,
		)
	}
	if err := s.deleteMainRaw(entry); err != nil {
		rollbackErr := s.rollbackMainEntryTombstone(ref.key)
		if rollbackErr == nil {
			state.deleting = false
		}
		return errors.Join(err, rollbackErr)
	}
	if err := s.entries.Sync(); err != nil {
		restoreErr := s.restoreMainEntryAfterSyncFailure(entry)
		rollbackErr := s.rollbackMainEntryTombstone(ref.key)
		if restoreErr == nil && rollbackErr == nil {
			state.deleting = false
		}
		return errors.Join(
			fmt.Errorf("sync deleted main entry: %w", err),
			restoreErr,
			rollbackErr,
		)
	}
	clear(state.absentAt)
	clear(state.durableAbsent)
	state.authorizedQueueIncarn = ""
	if err := s.persistMainEntryTombstone(
		ref.key,
		state,
		"",
		mainEntryTombstoneRetired,
	); err != nil {
		restoreErr := s.restoreMainEntryAfterSyncFailure(entry)
		rollbackErr := s.rollbackMainEntryTombstone(ref.key)
		if restoreErr == nil && rollbackErr == nil {
			state.deleting = false
		}
		return errors.Join(
			fmt.Errorf("finalize main entry retirement: %w", err),
			restoreErr,
			rollbackErr,
		)
	}
	if err := s.entryTombstones.Sync(); err != nil {
		restoreErr := s.restoreMainEntryAfterSyncFailure(entry)
		rollbackErr := s.rollbackMainEntryTombstone(ref.key)
		if restoreErr == nil && rollbackErr == nil {
			state.deleting = false
		}
		return errors.Join(
			fmt.Errorf("sync finalized main entry retirement: %w", err),
			restoreErr,
			rollbackErr,
		)
	}
	state.generation = s.mainEntries.nextSequence()
	state.retiredAt = state.generation
	state.retired = true
	state.deleting = false
	return nil
}

func (s *Storage) deleteMainRaw(entry *Entry) error {
	if entry == nil {
		return fmt.Errorf("main entry is nil")
	}
	current, err := s.getMainRaw(entry.InfoHash)
	if err != nil {
		return fmt.Errorf("load entry for deletion: %w", err)
	}
	s.entryItemsMu.Lock()
	defer s.entryItemsMu.Unlock()
	snapshots, err := s.mutateEntryItems(current, nil)
	if err != nil {
		return err
	}
	if err := s.entries.Delete(entry.InfoHash); err != nil {
		restoreErr := s.restoreEntryItemSnapshots(snapshots)
		if hybrid.IsNotFound(err) {
			err = fmt.Errorf("%w: %s", ErrEntryNotFound, entry.InfoHash)
		}
		return errors.Join(err, restoreErr)
	}
	s.markEntryItemMutation(current, nil)
	return nil
}

func (s *Storage) rollbackMainEntryTombstone(key string) error {
	tombstone, _, err := s.loadMainEntryTombstone(key)
	if err != nil {
		return err
	}
	if tombstone != nil && len(tombstone.PlacementSnapshot) != 0 {
		// Reopen same-process retries, but retain the durable independent admin
		// obligation. Startup still retires the main row and resumes providers.
		return s.entryTombstones.Sync()
	}
	if err := s.clearMainEntryTombstone(key); err != nil {
		return fmt.Errorf("rollback main entry tombstone: %w", err)
	}
	if err := s.entryTombstones.Sync(); err != nil {
		return fmt.Errorf("sync rolled back main entry tombstone: %w", err)
	}
	return nil
}

func (s *Storage) restoreRetiredMainEntryTombstone(
	key string,
	state *mainEntryState,
) error {
	if err := s.persistMainEntryTombstone(
		key,
		state,
		"",
		mainEntryTombstoneRetired,
	); err != nil {
		return fmt.Errorf("restore retired main entry tombstone: %w", err)
	}
	if err := s.entryTombstones.Sync(); err != nil {
		return fmt.Errorf("sync restored main entry tombstone: %w", err)
	}
	return nil
}

func (s *Storage) restoreMainEntryAfterSyncFailure(entry *Entry) error {
	if err := s.addOrUpdateMainRaw(entry); err != nil {
		return fmt.Errorf("restore main entry after sync failure: %w", err)
	}
	if err := s.entries.Sync(); err != nil {
		return fmt.Errorf("sync restored main entry after sync failure: %w", err)
	}
	return nil
}

// Count returns the number of entries
func (s *Storage) Count() (int, error) {
	return s.entries.Len(), nil
}

type entryItemSnapshot struct {
	name  string
	data  []byte
	found bool
}

func (s *Storage) snapshotEntryItem(name string) (entryItemSnapshot, error) {
	snapshot := entryItemSnapshot{name: name}
	data, err := s.entryItems.Get(name)
	if err != nil {
		if hybrid.IsNotFound(err) {
			return snapshot, nil
		}
		return snapshot, fmt.Errorf("load entry item %s: %w", name, err)
	}
	snapshot.data = append([]byte(nil), data...)
	snapshot.found = true
	return snapshot, nil
}

func (s *Storage) restoreEntryItemSnapshots(snapshots []entryItemSnapshot) error {
	var errs []error
	for _, snapshot := range snapshots {
		if snapshot.found {
			if err := s.entryItems.Put(snapshot.name, snapshot.data, nil); err != nil {
				errs = append(errs, fmt.Errorf("restore entry item %s: %w", snapshot.name, err))
			}
			continue
		}
		if err := s.entryItems.Delete(snapshot.name); err != nil && !hybrid.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("remove new entry item %s: %w", snapshot.name, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Storage) mutateEntryItems(previous, replacement *Entry) ([]entryItemSnapshot, error) {
	if err := s.markEntryItemsDirtyLocked(); err != nil {
		return nil, err
	}
	names := make(map[string]struct{}, 2)
	if previous != nil && previous.GetFolder() != "" {
		names[previous.GetFolder()] = struct{}{}
	}
	if replacement != nil && replacement.GetFolder() != "" {
		names[replacement.GetFolder()] = struct{}{}
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)

	snapshots := make([]entryItemSnapshot, 0, len(ordered))
	for _, name := range ordered {
		snapshot, err := s.snapshotEntryItem(name)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	rollback := func(err error) ([]entryItemSnapshot, error) {
		return nil, errors.Join(err, s.restoreEntryItemSnapshots(snapshots))
	}

	if previous != nil {
		if err := s.removeFromEntryItem(previous); err != nil {
			return rollback(err)
		}
	}
	if replacement != nil {
		if err := s.updateEntryItem(replacement); err != nil {
			return rollback(err)
		}
		if err := s.restoreEntryItemDeletedFlags(
			replacement.GetFolder(),
			snapshots,
		); err != nil {
			return rollback(err)
		}
	}
	return snapshots, nil
}

func (s *Storage) restoreEntryItemDeletedFlags(
	name string,
	snapshots []entryItemSnapshot,
) error {
	if name == "" {
		return nil
	}
	data, err := s.entryItems.Get(name)
	if err != nil {
		if hybrid.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("load replacement entry item %s: %w", name, err)
	}
	var currentPB EntryItemProto
	if err := proto.Unmarshal(data, &currentPB); err != nil {
		return fmt.Errorf("decode replacement entry item %s: %w", name, err)
	}
	current := ProtoToEntryItem(&currentPB)
	changed := false
	for _, snapshot := range snapshots {
		if !snapshot.found {
			continue
		}
		var previousPB EntryItemProto
		if err := proto.Unmarshal(snapshot.data, &previousPB); err != nil {
			return fmt.Errorf(
				"decode prior entry item %s: %w",
				snapshot.name,
				err,
			)
		}
		if preserveEntryItemDeletedFlags(
			current,
			ProtoToEntryItem(&previousPB),
		) {
			changed = true
		}
	}
	if !changed {
		return nil
	}
	current.Size = current.GetSize()
	updated, err := proto.MarshalOptions{Deterministic: true}.Marshal(
		EntryItemToProto(current),
	)
	if err != nil {
		return fmt.Errorf("encode replacement entry item %s: %w", name, err)
	}
	if err := s.entryItems.Put(name, updated, nil); err != nil {
		return fmt.Errorf("write replacement entry item %s: %w", name, err)
	}
	return nil
}

func (s *Storage) markEntryItemMutation(previous, replacement *Entry) {
	dirty := make(map[string]config.Protocol, 2)
	if previous != nil && previous.GetFolder() != "" {
		dirty[previous.GetFolder()] = previous.Protocol
	}
	if replacement != nil && replacement.GetFolder() != "" {
		dirty[replacement.GetFolder()] = replacement.Protocol
	}
	for name, protocol := range dirty {
		if !s.entryItems.Exists(name) {
			_ = s.DeleteEntryHealth(name)
			continue
		}
		s.MarkEntryDirty(name, protocol, "entry_item_changed")
	}
}

// updateEntryItem updates the name index. The caller owns entryItemsMu.
func (s *Storage) updateEntryItem(entry *Entry) error {
	name := entry.GetFolder()
	if name == "" {
		return nil
	}

	var item *EntryItem
	if data, err := s.entryItems.Get(name); err == nil {
		var pb EntryItemProto
		if err := proto.Unmarshal(data, &pb); err != nil {
			return fmt.Errorf("decode entry item %s: %w", name, err)
		}
		item = ProtoToEntryItem(&pb)
	} else if !hybrid.IsNotFound(err) {
		return fmt.Errorf("load entry item %s: %w", name, err)
	}

	if item == nil {
		item = &EntryItem{Name: name, Files: make(map[string]*File)}
	}

	mergeEntryItemFiles(item, entry)

	item.Size = item.GetSize()
	pb := EntryItemToProto(item)
	data, err := proto.Marshal(pb)
	if err != nil {
		return fmt.Errorf("encode entry item %s: %w", name, err)
	}
	if err := s.entryItems.Put(name, data, nil); err != nil {
		return fmt.Errorf("write entry item %s: %w", name, err)
	}
	return nil
}

// removeFromEntryItem removes an entry from the name index. The caller owns
// entryItemsMu.
func (s *Storage) removeFromEntryItem(entry *Entry) error {
	name := entry.GetFolder()
	if name == "" {
		return nil
	}

	data, err := s.entryItems.Get(name)
	if err != nil {
		if hybrid.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("load entry item %s: %w", name, err)
	}

	var pb EntryItemProto
	if err := proto.Unmarshal(data, &pb); err != nil {
		return fmt.Errorf("decode entry item %s: %w", name, err)
	}
	item := ProtoToEntryItem(&pb)

	for fileName := range entry.Files {
		if f, exists := item.Files[fileName]; exists && f.InfoHash == entry.InfoHash {
			delete(item.Files, fileName)
		}
	}
	if err := s.mergeRemainingFolderEntries(item, name, entry.InfoHash); err != nil {
		return err
	}

	if len(item.Files) == 0 {
		if err := s.entryItems.Delete(name); err != nil && !hybrid.IsNotFound(err) {
			return fmt.Errorf("delete entry item %s: %w", name, err)
		}
		return nil
	}

	item.Size = item.GetSize()
	updatedPb := EntryItemToProto(item)
	updatedData, err := proto.Marshal(updatedPb)
	if err != nil {
		return fmt.Errorf("encode entry item %s: %w", name, err)
	}
	if err := s.entryItems.Put(name, updatedData, nil); err != nil {
		return fmt.Errorf("write entry item %s: %w", name, err)
	}
	return nil
}

// mergeRemainingFolderEntries restores files supplied by other authoritative
// entries that render the same folder. A provider can expose one cloud
// transfer under more than one stable identity, and independent providers can
// also produce the same release folder. Deleting one identity must not remove
// the shared folder's only indexed copy of a file that another live entry still
// serves.
//
// The caller holds entryItemsMu. Main-entry mutations take that mutex before
// writing the authoritative store, so the metadata scan and reads below see a
// stable cross-entry snapshot. Sorting keys keeps equal-timestamp tie-breaking
// deterministic even though the in-memory metadata index is lock-free.
func (s *Storage) mergeRemainingFolderEntries(
	item *EntryItem,
	folder string,
	skipInfoHash string,
) error {
	if item == nil || folder == "" {
		return nil
	}
	skipInfoHash = normalizeMainEntryKey(skipInfoHash)
	keys := make([]string, 0)
	if err := s.entries.ForEachMeta(func(key string, meta *hybrid.IndexEntry) error {
		if strings.HasPrefix(key, "__") ||
			normalizeMainEntryKey(key) == skipInfoHash ||
			meta.Name != folder {
			return nil
		}
		keys = append(keys, key)
		return nil
	}); err != nil {
		return fmt.Errorf("scan entries for shared folder %q: %w", folder, err)
	}
	sort.Strings(keys)
	for _, key := range keys {
		data, err := s.entries.Get(key)
		if err != nil {
			return fmt.Errorf("read entry %q for shared folder %q: %w", key, folder, err)
		}
		remaining, err := decodeMainEntry(key, data)
		if err != nil {
			return fmt.Errorf("decode entry %q for shared folder %q: %w", key, folder, err)
		}
		// Metadata is an optimization, not authority. Fail closed if it is
		// stale rather than merging an unrelated entry into this folder.
		if remaining.GetFolder() != folder {
			return fmt.Errorf(
				"entry %q metadata names shared folder %q but payload renders %q",
				key,
				folder,
				remaining.GetFolder(),
			)
		}
		mergeEntryItemFiles(item, remaining)
	}
	return nil
}

// Queue operations

// AddQueue adds an entry to the queue
func (s *Storage) AddQueue(entry *Entry) error {
	if entry == nil {
		return fmt.Errorf("queued entry is nil")
	}
	entry.InfoHash = normalizeMainEntryKey(entry.InfoHash)
	ref, err := s.acquireMainEntryState(entry.InfoHash)
	if err != nil {
		return err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleting {
		return fmt.Errorf("%w: %s", ErrEntryDeleting, ref.key)
	}
	if state.queueDeleting {
		return fmt.Errorf("%w: %s", ErrQueuedEntryDeleting, ref.key)
	}

	current, err := s.getQueuedRaw(ref.key)
	switch {
	case err == nil:
		entry.QueueIncarnation = current.QueueIncarnation
		entry.CreatedAt = current.CreatedAt
		PreserveTorrentOutputPath(current, entry)
		if state.retired &&
			(entry.QueueIncarnation == "" ||
				entry.QueueIncarnation != state.authorizedQueueIncarn) {
			return fmt.Errorf(
				"%w: %s still has pre-retirement queue incarnation %q",
				ErrEntryRediscoveryPending,
				ref.key,
				entry.QueueIncarnation,
			)
		}
		if err := s.writeQueueRaw(entry, false); err != nil {
			return err
		}
		if err := s.queue.Sync(); err != nil {
			return fmt.Errorf("sync queued entry: %w", err)
		}
		return nil
	case !IsQueuedEntryNotFound(err):
		return fmt.Errorf("check existing queued entry: %w", err)
	}

	entry.CreatedAt = time.Now()
	entry.QueueIncarnation = uuid.NewString()
	if !state.retired {
		if err := s.writeQueueRaw(entry, false); err != nil {
			return err
		}
		if err := s.queue.Sync(); err != nil {
			return fmt.Errorf("sync queued entry: %w", err)
		}
		return nil
	}

	if err := s.persistMainEntryTombstone(
		ref.key,
		state,
		"",
		mainEntryTombstoneQueuePending,
		entry.QueueIncarnation,
	); err != nil {
		return fmt.Errorf("persist pending queued replacement: %w", err)
	}
	if err := s.entryTombstones.Sync(); err != nil {
		restoreErr := s.restoreRetiredMainEntryTombstone(ref.key, state)
		return errors.Join(
			fmt.Errorf("sync pending queued replacement: %w", err),
			restoreErr,
		)
	}
	if err := s.writeQueueRaw(entry, false); err != nil {
		restoreErr := s.restoreRetiredMainEntryTombstone(ref.key, state)
		return errors.Join(err, restoreErr)
	}
	if err := s.queue.Sync(); err != nil {
		removeErr := s.deleteQueuedAfterFailedReplacement(ref.key)
		restoreErr := s.restoreRetiredMainEntryTombstone(ref.key, state)
		return errors.Join(
			fmt.Errorf("sync queued replacement: %w", err),
			removeErr,
			restoreErr,
		)
	}
	if err := s.persistMainEntryTombstone(
		ref.key,
		state,
		"",
		mainEntryTombstoneRetired,
		entry.QueueIncarnation,
	); err != nil {
		return fmt.Errorf("authorize queued replacement: %w", err)
	}
	if err := s.entryTombstones.Sync(); err != nil {
		return fmt.Errorf("sync authorized queued replacement: %w", err)
	}
	state.authorizedQueueIncarn = entry.QueueIncarnation
	return nil
}

func (s *Storage) deleteQueuedAfterFailedReplacement(key string) error {
	err := s.queue.Delete(key)
	if err != nil && !hybrid.IsNotFound(err) {
		return fmt.Errorf("remove failed queued replacement: %w", err)
	}
	if err := s.queue.Sync(); err != nil {
		return fmt.Errorf("sync removed queued replacement: %w", err)
	}
	return nil
}

// UpdateQueue updates a queued entry
func (s *Storage) UpdateQueue(entry *Entry) error {
	return s.writeQueue(entry, false)
}

// UpdateQueueExisting updates a queued entry only if its row still exists.
// Queue workers use this instead of the upserting UpdateQueue path so a late
// progress/error update cannot recreate an entry after explicit deletion.
func (s *Storage) UpdateQueueExisting(entry *Entry) error {
	return s.writeQueue(entry, true)
}

func (s *Storage) writeQueue(entry *Entry, requireExisting bool) error {
	if entry == nil {
		return fmt.Errorf("queued entry is nil")
	}
	entry.InfoHash = normalizeMainEntryKey(entry.InfoHash)
	ref, err := s.acquireMainEntryState(entry.InfoHash)
	if err != nil {
		return err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.queueDeleting {
		return fmt.Errorf("%w: %s", ErrQueuedEntryDeleting, ref.key)
	}
	return s.writeQueueRaw(entry, requireExisting)
}

func (s *Storage) writeQueueRaw(entry *Entry, requireExisting bool) error {
	if err := s.revokeChangedHandoffAuthorization(entry); err != nil {
		return fmt.Errorf("revoke changed handoff authorization: %w", err)
	}
	entry.UpdatedAt = time.Now()

	pb := EntryToProto(entry)
	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}

	meta := &hybrid.EntryMeta{
		Category:  entry.Category,
		Provider:  entry.ActiveProvider,
		Status:    string(entry.Status),
		Name:      entry.GetFolder(), // Store computed folder name for fast listings
		TotalSize: entry.Size,
		Protocol:  string(entry.Protocol),
		Bad:       entry.Bad,
		AddedOn:   entry.AddedOn.Unix(),
	}

	key := strings.ToLower(entry.InfoHash)
	if requireExisting {
		if err := s.queue.PutExisting(key, data, meta); err != nil {
			if hybrid.IsNotFound(err) {
				return fmt.Errorf("%w: %s", ErrQueuedEntryNotFound, entry.InfoHash)
			}
			return err
		}
		return nil
	}
	return s.queue.Put(key, data, meta)
}

// GetQueued retrieves a queued entry
func (s *Storage) GetQueued(infohash string) (*Entry, error) {
	key := normalizeMainEntryKey(infohash)
	ref, err := s.acquireMainEntryState(key)
	if err != nil {
		return nil, err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.queueDeleting {
		return nil, errors.Join(
			fmt.Errorf("%w: %s", ErrQueuedEntryNotFound, key),
			fmt.Errorf("%w: %s", ErrQueuedEntryDeleting, key),
		)
	}
	return s.getQueuedRaw(key)
}

func (s *Storage) getQueuedRaw(infohash string) (*Entry, error) {
	key := normalizeMainEntryKey(infohash)
	data, err := s.queue.Get(key)
	if err != nil {
		if hybrid.IsNotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrQueuedEntryNotFound, infohash)
		}
		return nil, err
	}

	return decodeQueuedEntry(key, data)
}

// DeleteQueued removes a queued entry
func (s *Storage) DeleteQueued(infohash string, cleanup func(*Entry) error) error {
	key := normalizeMainEntryKey(infohash)
	intent, err := s.prepareQueuedDeletion(key, false, cleanup != nil, false, QueuePlacementScopeAll)
	if err != nil {
		return err
	}
	if err := s.StartQueuedDeletionCleanup(key, intent.QueueIncarnation); err != nil {
		return err
	}
	if intent.PlacementCleanupPending {
		return fmt.Errorf(
			"queued entry %s requires manager placement cleanup",
			key,
		)
	}
	if intent.UnrecoverableCleanupPending && cleanup == nil {
		return fmt.Errorf(
			"queued entry %s retains non-recoverable cleanup intent",
			key,
		)
	}
	if cleanup != nil {
		if err := cleanup(intent.Entry); err != nil {
			return fmt.Errorf("cleanup queued entry %s: %w", key, err)
		}
		if err := s.markQueuedDeletionOpaqueCleanupComplete(
			key,
			intent.QueueIncarnation,
		); err != nil {
			return err
		}
	}
	if err := s.RetireQueuedDeletionRow(key, intent.QueueIncarnation); err != nil {
		return err
	}
	return s.CompleteQueuedDeletion(key, intent.QueueIncarnation)
}

// FilterQueued returns entries matching a filter
func (s *Storage) FilterQueued(filter func(*Entry) bool) ([]*Entry, error) {
	var entries []*Entry
	if err := s.queue.ForEach(func(key string, value []byte) error {
		visible, err := s.queuedEntryVisible(key)
		if err != nil {
			return err
		}
		if !visible {
			return nil
		}
		entry, err := decodeQueuedEntry(key, value)
		if err != nil {
			return err
		}
		if filter == nil || filter(entry) {
			entries = append(entries, entry)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan queued entries: %w", err)
	}
	return entries, nil
}

// DeleteWhereQueued deletes matching queued entries
func (s *Storage) DeleteWhereQueued(predicate func(*Entry) bool, cleanup func(*Entry) error) error {
	type queuedCandidate struct {
		key   string
		entry *Entry
	}
	var candidates []queuedCandidate
	if err := s.queue.ForEach(func(key string, value []byte) error {
		entry, err := decodeQueuedEntry(key, value)
		if err != nil {
			return err
		}
		if predicate == nil || predicate(entry) {
			candidates = append(candidates, queuedCandidate{key: key, entry: entry})
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scan queued entries for deletion: %w", err)
	}

	var errs []error
	for _, candidate := range candidates {
		if err := s.DeleteQueued(candidate.key, cleanup); err != nil {
			errs = append(errs, fmt.Errorf("delete queued entry %s: %w", candidate.key, err))
		}
	}
	return errors.Join(errs...)
}

// UpdateWhereQueued updates matching queued entries
func (s *Storage) UpdateWhereQueued(filter func(*Entry) bool, updateFunc func(*Entry) bool) error {
	type update struct {
		key   string
		entry *Entry
	}
	var updates []update

	if err := s.queue.ForEach(func(key string, value []byte) error {
		entry, err := decodeQueuedEntry(key, value)
		if err != nil {
			return err
		}
		if (filter == nil || filter(entry)) && updateFunc != nil && updateFunc(entry) {
			updates = append(updates, update{key, entry})
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scan queued entries for update: %w", err)
	}

	for _, u := range updates {
		if err := s.UpdateQueueExisting(u.entry); err != nil {
			return fmt.Errorf("update queued entry %s: %w", u.key, err)
		}
	}
	return nil
}

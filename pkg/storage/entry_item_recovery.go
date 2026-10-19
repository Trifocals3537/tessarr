package storage

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
	"google.golang.org/protobuf/proto"
)

const (
	entryItemStateKey            = "entry_items"
	entryItemStateClean          = "clean-v1"
	entryItemStateDirty          = "dirty-v1"
	entryItemIntegrityStateKey   = "entry_items_integrity"
	entryItemSharedFolderVersion = "shared-folder-v1"
)

func (s *Storage) needsEntryItemSharedFolderUpgrade() (bool, error) {
	if s.storageState == nil {
		return false, fmt.Errorf("storage state store is not initialized")
	}
	state, err := s.storageState.Get(entryItemIntegrityStateKey)
	switch {
	case err == nil:
		return string(state) != entryItemSharedFolderVersion, nil
	case hybrid.IsNotFound(err):
		return true, nil
	default:
		return false, fmt.Errorf("read entry-item integrity version: %w", err)
	}
}

func (s *Storage) markEntryItemSharedFolderUpgrade() error {
	if err := s.storageState.Put(
		entryItemIntegrityStateKey,
		[]byte(entryItemSharedFolderVersion),
		nil,
	); err != nil {
		return fmt.Errorf("write entry-item integrity version: %w", err)
	}
	if err := s.storageState.Sync(); err != nil {
		return fmt.Errorf("sync entry-item integrity version: %w", err)
	}
	return nil
}

// beginEntryItemSession leaves a durable dirty marker before startup can
// mutate either the authoritative main-entry store or its name index. A clean
// shutdown clears it only after both stores have been flushed.
func (s *Storage) beginEntryItemSession() (needsRecovery bool, err error) {
	if s.storageState == nil {
		return false, fmt.Errorf("storage state store is not initialized")
	}
	state, getErr := s.storageState.Get(entryItemStateKey)
	switch {
	case getErr == nil:
		needsRecovery = string(state) != entryItemStateClean
	case hybrid.IsNotFound(getErr):
		needsRecovery = true
	default:
		return false, fmt.Errorf("read entry-item state: %w", getErr)
	}
	if err := s.storageState.Put(
		entryItemStateKey,
		[]byte(entryItemStateDirty),
		nil,
	); err != nil {
		return false, fmt.Errorf("mark entry-item state dirty: %w", err)
	}
	if err := s.storageState.Sync(); err != nil {
		return false, fmt.Errorf("sync dirty entry-item state: %w", err)
	}
	s.entryItemsDirty = true
	return needsRecovery, nil
}

// markEntryItemsDirtyLocked is called before an in-process mutation of the
// derived index. The session marker normally makes this a no-op; retaining the
// guard keeps direct and embedded storage use fail-closed.
func (s *Storage) markEntryItemsDirtyLocked() error {
	if s.entryItemsDirty {
		return nil
	}
	if s.storageState == nil {
		return fmt.Errorf("storage state store is not initialized")
	}
	if err := s.storageState.Put(
		entryItemStateKey,
		[]byte(entryItemStateDirty),
		nil,
	); err != nil {
		return fmt.Errorf("mark entry-item state dirty: %w", err)
	}
	if err := s.storageState.Sync(); err != nil {
		return fmt.Errorf("sync dirty entry-item state: %w", err)
	}
	s.entryItemsDirty = true
	return nil
}

func (s *Storage) markEntryItemsClean() error {
	if s.storageState == nil || s.entries == nil || s.entryItems == nil {
		return fmt.Errorf("entry-item recovery stores are unavailable")
	}
	s.entryItemsMu.Lock()
	defer s.entryItemsMu.Unlock()

	// The secondary mutation is always appended before its authoritative main
	// mutation. Flush in that same order before declaring the pair clean.
	if err := s.entryItems.Sync(); err != nil {
		return fmt.Errorf("sync entry-item index before clean shutdown: %w", err)
	}
	if err := s.entries.Sync(); err != nil {
		return fmt.Errorf("sync main entries before clean shutdown: %w", err)
	}
	if err := s.storageState.Put(
		entryItemStateKey,
		[]byte(entryItemStateClean),
		nil,
	); err != nil {
		return fmt.Errorf("mark entry-item state clean: %w", err)
	}
	if err := s.storageState.Sync(); err != nil {
		return fmt.Errorf("sync clean entry-item state: %w", err)
	}
	s.entryItemsDirty = false
	return nil
}

// reconcileEntryItems rebuilds membership from authoritative main entries
// while preserving a user's durable per-file Deleted flags. It runs only after
// an unclean shutdown or when upgrading a database that predates the marker.
// A crash during the repair leaves the marker dirty, so the next startup
// repeats the deterministic reconciliation before exposing storage.
func (s *Storage) reconcileEntryItems() (int, error) {
	if s.entries == nil || s.entryItems == nil {
		return 0, fmt.Errorf("entry-item recovery stores are unavailable")
	}

	expected := make(map[string]*EntryItem)
	protocols := make(map[string]config.Protocol)
	if err := s.entries.ForEach(func(key string, value []byte) error {
		if strings.HasPrefix(key, "__") {
			return nil
		}
		entry, err := decodeMainEntry(key, value)
		if err != nil {
			return err
		}
		name := entry.GetFolder()
		if name == "" {
			return nil
		}
		item := expected[name]
		if item == nil {
			item = &EntryItem{Name: name, Files: make(map[string]*File)}
			expected[name] = item
		}
		mergeEntryItemFiles(item, entry)
		protocols[name] = entry.Protocol
		return nil
	}); err != nil {
		return 0, fmt.Errorf("scan authoritative entries for item recovery: %w", err)
	}

	s.entryItemsMu.Lock()
	defer s.entryItemsMu.Unlock()
	if err := s.markEntryItemsDirtyLocked(); err != nil {
		return 0, err
	}

	existing := make(map[string]*EntryItem)
	if err := s.entryItems.ForEach(func(key string, value []byte) error {
		var pb EntryItemProto
		if err := proto.Unmarshal(value, &pb); err != nil {
			// A corrupt derived record is replaced or removed below.
			existing[key] = nil
			return nil
		}
		existing[key] = ProtoToEntryItem(&pb)
		return nil
	}); err != nil {
		return 0, fmt.Errorf("scan existing entry items: %w", err)
	}

	names := make([]string, 0, len(expected))
	for name := range expected {
		names = append(names, name)
	}
	sort.Strings(names)

	changedNames := make([]string, 0)
	changes := 0
	for _, name := range names {
		item := expected[name]
		preserveEntryItemDeletedFlags(item, existing[name])
		item.Size = item.GetSize()
		pb := EntryItemToProto(item)
		if current := existing[name]; current != nil &&
			proto.Equal(EntryItemToProto(current), pb) {
			delete(existing, name)
			continue
		}
		data, err := proto.MarshalOptions{Deterministic: true}.Marshal(pb)
		if err != nil {
			return changes, fmt.Errorf("encode recovered entry item %s: %w", name, err)
		}
		if err := s.entryItems.Put(name, data, nil); err != nil {
			return changes, fmt.Errorf("write recovered entry item %s: %w", name, err)
		}
		delete(existing, name)
		changedNames = append(changedNames, name)
		changes++
	}

	extraNames := make([]string, 0, len(existing))
	for name := range existing {
		extraNames = append(extraNames, name)
	}
	sort.Strings(extraNames)
	for _, name := range extraNames {
		if err := s.entryItems.Delete(name); err != nil &&
			!hybrid.IsNotFound(err) {
			return changes, fmt.Errorf("remove orphan entry item %s: %w", name, err)
		}
		changes++
	}
	if changes > 0 {
		if err := s.entryItems.Sync(); err != nil {
			return changes, fmt.Errorf("sync recovered entry-item index: %w", err)
		}
	}

	for _, name := range changedNames {
		s.MarkEntryDirty(name, protocols[name], "entry_item_reconciled")
	}
	for _, name := range extraNames {
		_ = s.DeleteEntryHealth(name)
	}
	return changes, nil
}

func mergeEntryItemFiles(item *EntryItem, entry *Entry) {
	if item == nil || entry == nil {
		return
	}
	if item.Files == nil {
		item.Files = make(map[string]*File)
	}
	for fileName, file := range entry.Files {
		if file == nil {
			continue
		}
		existing := item.Files[fileName]
		switch {
		case existing == nil:
			item.Files[fileName] = file
		case file.AddedOn.After(existing.AddedOn):
			item.Files[fileName] = file
		case file.AddedOn.Equal(existing.AddedOn) && file.Size != existing.Size:
			item.Files[fileName] = file
		}
		selected := item.Files[fileName]
		if existing != nil &&
			existing.Deleted &&
			selected != nil &&
			!selected.Deleted &&
			strings.EqualFold(existing.InfoHash, selected.InfoHash) {
			copyFile := *selected
			copyFile.Deleted = true
			item.Files[fileName] = &copyFile
		}
	}
}

func preserveEntryItemDeletedFlags(expected, existing *EntryItem) bool {
	if expected == nil || existing == nil {
		return false
	}
	changed := false
	for name, expectedFile := range expected.Files {
		existingFile := existing.Files[name]
		if expectedFile == nil ||
			existingFile == nil ||
			!existingFile.Deleted ||
			!strings.EqualFold(existingFile.InfoHash, expectedFile.InfoHash) {
			continue
		}
		copyFile := *expectedFile
		copyFile.Deleted = true
		expected.Files[name] = &copyFile
		changed = true
	}
	return changed
}

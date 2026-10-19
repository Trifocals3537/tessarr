package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
	"google.golang.org/protobuf/proto"
)

// RebaseStatePaths rewrites paths rooted in an old application state
// directory without changing external media or download paths. It operates on
// an offline copy and preserves entry timestamps and identities.
func RebaseStatePaths(dbPath, sourceRoot, targetRoot string) (int, error) {
	changed := 0
	for _, store := range []struct {
		name   string
		decode func(string, []byte) (*Entry, error)
	}{
		{name: "entries", decode: decodeMainEntry},
		{name: "queue", decode: decodeQueuedEntry},
	} {
		count, err := rebaseEntryStore(dbPath, store.name, sourceRoot, targetRoot, store.decode)
		if err != nil {
			return changed, err
		}
		changed += count
	}
	count, err := rebaseQueueTombstoneStore(dbPath, sourceRoot, targetRoot)
	if err != nil {
		return changed, err
	}
	return changed + count, nil
}

func rebaseEntryStore(
	dbPath, name, sourceRoot, targetRoot string,
	decode func(string, []byte) (*Entry, error),
) (int, error) {
	path := filepath.Join(dbPath, name+".db")
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("inspect %s store: %w", name, err)
	}
	store, err := hybrid.New(hybrid.Config{
		DataPath:            path,
		SyncInterval:        -1,
		CompactionThreshold: 1,
	})
	if err != nil {
		return 0, fmt.Errorf("open %s store: %w", name, err)
	}
	type update struct {
		key   string
		data  []byte
		entry *Entry
	}
	var updates []update
	iterateErr := store.ForEach(func(key string, value []byte) error {
		entry, err := decode(key, value)
		if err != nil {
			return err
		}
		if !rebaseEntryStatePaths(entry, sourceRoot, targetRoot) {
			return nil
		}
		data, err := proto.Marshal(EntryToProto(entry))
		if err != nil {
			return fmt.Errorf("encode %s entry %q: %w", name, key, err)
		}
		updates = append(updates, update{key: key, data: data, entry: entry})
		return nil
	})
	if iterateErr == nil {
		for _, item := range updates {
			if err := store.PutExisting(item.key, item.data, entryHybridMeta(item.entry)); err != nil {
				iterateErr = fmt.Errorf("rewrite %s entry %q: %w", name, item.key, err)
				break
			}
		}
	}
	if iterateErr == nil && len(updates) > 0 {
		iterateErr = store.Sync()
	}
	closeErr := store.Close()
	if iterateErr != nil {
		return 0, fmt.Errorf("rebase %s store: %w", name, iterateErr)
	}
	if closeErr != nil {
		return 0, fmt.Errorf("close rebased %s store: %w", name, closeErr)
	}
	return len(updates), nil
}

func rebaseQueueTombstoneStore(dbPath, sourceRoot, targetRoot string) (int, error) {
	path := filepath.Join(dbPath, "queue_tombstones.db")
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("inspect queue tombstone store: %w", err)
	}
	store, err := hybrid.New(hybrid.Config{
		DataPath:            path,
		SyncInterval:        -1,
		CompactionThreshold: 1,
	})
	if err != nil {
		return 0, fmt.Errorf("open queue tombstone store: %w", err)
	}
	type update struct {
		key  string
		data []byte
	}
	var updates []update
	iterateErr := store.ForEach(func(key string, value []byte) error {
		var tombstone queueDeletionTombstone
		if err := json.Unmarshal(value, &tombstone); err != nil {
			return fmt.Errorf("decode queue deletion tombstone %q: %w", key, err)
		}
		changed, err := rebaseQueueTombstoneSnapshots(key, &tombstone, sourceRoot, targetRoot)
		if err != nil || !changed {
			return err
		}
		data, err := json.Marshal(&tombstone)
		if err != nil {
			return fmt.Errorf("encode queue deletion tombstone %q: %w", key, err)
		}
		updates = append(updates, update{key: key, data: data})
		return nil
	})
	if iterateErr == nil {
		for _, item := range updates {
			if err := store.PutExisting(item.key, item.data, nil); err != nil {
				iterateErr = fmt.Errorf("rewrite queue deletion tombstone %q: %w", item.key, err)
				break
			}
		}
	}
	if iterateErr == nil && len(updates) > 0 {
		iterateErr = store.Sync()
	}
	closeErr := store.Close()
	if iterateErr != nil {
		return 0, fmt.Errorf("rebase queue tombstone store: %w", iterateErr)
	}
	if closeErr != nil {
		return 0, fmt.Errorf("close rebased queue tombstone store: %w", closeErr)
	}
	return len(updates), nil
}

func rebaseQueueTombstoneSnapshots(key string, tombstone *queueDeletionTombstone, sourceRoot, targetRoot string) (bool, error) {
	changed := false
	snapshots := append([][]byte{tombstone.Snapshot}, tombstone.PlacementSnapshots...)
	for index, snapshot := range snapshots {
		entry, err := decodeQueuedEntry(key, snapshot)
		if err != nil {
			return false, fmt.Errorf("decode queue tombstone snapshot %q: %w", key, err)
		}
		if !rebaseEntryStatePaths(entry, sourceRoot, targetRoot) {
			continue
		}
		encoded, err := proto.Marshal(EntryToProto(entry))
		if err != nil {
			return false, fmt.Errorf("encode queue tombstone snapshot %q: %w", key, err)
		}
		if index == 0 {
			tombstone.Snapshot = encoded
		} else {
			tombstone.PlacementSnapshots[index-1] = encoded
		}
		changed = true
	}
	return changed, nil
}

func rebaseEntryStatePaths(entry *Entry, sourceRoot, targetRoot string) bool {
	changed := false
	for _, value := range []*string{&entry.MountPath, &entry.SavePath, &entry.ContentPath, &entry.Magnet} {
		if rebased, ok := rebaseContainedPath(*value, sourceRoot, targetRoot); ok {
			*value = rebased
			changed = true
		}
	}
	return changed
}

func rebaseContainedPath(value, sourceRoot, targetRoot string) (string, bool) {
	if value == "" || !filepath.IsAbs(value) {
		return value, false
	}
	relative, err := filepath.Rel(sourceRoot, filepath.Clean(value))
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return value, false
	}
	return filepath.Join(targetRoot, relative), true
}

func entryHybridMeta(entry *Entry) *hybrid.EntryMeta {
	return &hybrid.EntryMeta{
		Category:  entry.Category,
		Provider:  entry.ActiveProvider,
		Status:    string(entry.Status),
		Name:      entry.GetFolder(),
		TotalSize: entry.Size,
		Protocol:  string(entry.Protocol),
		Bad:       entry.Bad,
		AddedOn:   entry.AddedOn.Unix(),
	}
}

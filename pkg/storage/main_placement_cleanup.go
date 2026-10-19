package storage

import (
	"encoding/json"
	"fmt"
)

func (s *Storage) syncMainCleanupState() error {
	if s.mainCleanupSyncForTest != nil {
		return s.mainCleanupSyncForTest()
	}
	return s.entryTombstones.Sync()
}

// PrepareMainDeletionPlacements persists the independently requested admin
// cleanup under an already installed main deletion tombstone, before queue
// retirement or provider work. Existing snapshots are immutable on retry.
func (s *Storage) PrepareMainDeletionPlacements(entry *Entry) error {
	ref, err := s.acquireMainEntryState(entry.InfoHash)
	if err != nil {
		return err
	}
	defer ref.release()
	ref.state.mu.Lock()
	defer ref.state.mu.Unlock()
	tombstone, found, err := s.loadMainEntryTombstone(ref.key)
	if err != nil {
		return err
	}
	if !found || !ref.state.deleting {
		return fmt.Errorf("main deletion does not own placement cleanup")
	}
	if len(tombstone.PlacementSnapshot) == 0 {
		snapshot, err := marshalQueueDeletionSnapshot(ref.key, entry)
		if err != nil {
			return err
		}
		tombstone.PlacementSnapshot = snapshot
		data, err := json.Marshal(tombstone)
		if err != nil {
			return err
		}
		if err := s.entryTombstones.Put(ref.key, data, nil); err != nil {
			return err
		}
	}
	return s.syncMainCleanupState()
}

// MainDeletionPlacementIntents returns only durable admin snapshots, never
// discovering or appending live main/queue placements during recovery.
func (s *Storage) MainDeletionPlacementIntents() ([]*Entry, error) {
	var entries []*Entry
	err := s.entryTombstones.ForEach(func(key string, _ []byte) error {
		tombstone, _, err := s.loadMainEntryTombstone(key)
		if err != nil {
			return err
		}
		if len(tombstone.PlacementSnapshot) == 0 {
			return nil
		}
		e, err := decodeMainEntry(key, tombstone.PlacementSnapshot)
		if err != nil {
			return err
		}
		entries = append(entries, e)
		return nil
	})
	return entries, err
}

func (s *Storage) CompleteMainDeletionPlacements(infohash string) error {
	ref, err := s.acquireMainEntryState(infohash)
	if err != nil {
		return err
	}
	defer ref.release()
	ref.state.mu.Lock()
	defer ref.state.mu.Unlock()
	tombstone, found, err := s.loadMainEntryTombstone(ref.key)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("main cleanup tombstone missing")
	}
	tombstone.PlacementSnapshot = nil
	data, err := json.Marshal(tombstone)
	if err != nil {
		return err
	}
	if err := s.entryTombstones.Put(ref.key, data, nil); err != nil {
		return err
	}
	return s.syncMainCleanupState()
}

package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
)

// These are opaque hybrid-store keys, never filesystem paths. A snapshot digest
// makes an exact retry idempotent and retains later snapshots as separate proof.
func retiredHealthArchiveKey(source *EntryHealth, owner string) (string, error) {
	snapshot := *source
	if snapshot.Archived {
		if snapshot.ArchivedFrom == "" {
			return "", ErrHealthIdentity
		}
		snapshot.EntryName = snapshot.ArchivedFrom
	}
	snapshot.Archived, snapshot.ArchivedFrom = false, ""
	snapshot.OwnerInfoHash = owner
	snapshot.UpdatedAt = time.Time{}
	data, err := json.Marshal(&snapshot)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return "\x00retired-health\x00" + owner + "\x00" + hex.EncodeToString(digest[:]), nil
}

// Local main retirement is distinct from provider absence or administrative
// cleanup authority. Missing data alone, pending replacements and live main
// rows cannot release a protected health owner. A deleting fence with no main
// row is the existing startup-recovery state after finishing local retirement.
// A running deletion may still roll back either phase, so its lifecycle must
// also be quiescent before any former-owner evidence becomes archived history.
func (s *Storage) proveRetiredHealthOwnerLocked(owner string) error {
	if !s.mainEntries.quiescentForHealth(owner) {
		return ErrHealthIdentity
	}
	if _, err := s.getMainRaw(owner); !IsEntryNotFound(err) {
		if err != nil {
			return err
		}
		return ErrHealthIdentity
	}
	intent, found, err := s.loadMainEntryTombstone(owner)
	if err != nil {
		return err
	}
	if !found || intent.Phase != mainEntryTombstoneRetired && intent.Phase != mainEntryTombstoneDeleting {
		return ErrHealthIdentity
	}
	return nil
}

func (s *Storage) syncHealthOwnerArchiveLocked(phase string) error {
	if s.healthOwnerArchiveSyncForTest != nil {
		if err := s.healthOwnerArchiveSyncForTest(phase); err != nil {
			return err
		}
	}
	if err := s.repairState.Sync(); err != nil {
		return err
	}
	if s.healthOwnerArchiveForTest != nil {
		s.healthOwnerArchiveForTest(phase)
	}
	return nil
}

// The whole former row is archived. Only the proved survivor's scoped evidence
// remains active; unscoped missing evidence stays incomplete. A third unresolved
// owner is ambiguous and must not disappear into another owner's history.
func survivorHealthState(name string, previous string, owner *Entry, source *EntryHealth, revision uint64) (*EntryHealth, error) {
	next := &EntryHealth{EntryName: name, OwnerInfoHash: owner.InfoHash, Protocol: owner.Protocol,
		Status: HealthUnknown, Dirty: true, DirtyReason: "health_owner_retired", DirtyRevision: revision + 1}
	if source.Archived {
		return next, nil // An old forwarding record carries no active proof.
	}
	next.FailedReadsIncomplete = source.FailedReadsIncomplete || missingLegacyReadEvidence(source)
	for _, failed := range source.FailedReads {
		switch failed.InfoHash {
		case owner.InfoHash:
			addFailedRead(next, failed)
		case previous:
		default:
			return nil, ErrHealthIdentity
		}
	}
	for _, broken := range source.BrokenFiles {
		switch broken.InfoHash {
		case owner.InfoHash:
			next.BrokenFiles = append(next.BrokenFiles, broken)
		case previous:
		default:
			return nil, ErrHealthIdentity
		}
	}
	for name, receipt := range source.FileRetirements {
		if receipt.InfoHash == owner.InfoHash {
			if next.FileRetirements == nil {
				next.FileRetirements = make(map[string]HealthFileRetirement)
			}
			next.FileRetirements[name] = receipt
		} else if receipt.InfoHash != previous && !receipt.Confirmed {
			return nil, ErrHealthIdentity
		}
	}
	for name, history := range source.FileRetirementHistory {
		for _, receipt := range history {
			if receipt.InfoHash == owner.InfoHash {
				archiveHealthRetirement(next, name, receipt)
			}
		}
	}
	return next, nil
}

// The caller holds entryItemsMu and entryHealthMu, excluding all main/index
// publication and health feedback. Publication order is archive -> old aliases
// -> current root, with a durability barrier before each next step.
func (s *Storage) releaseRetiredHealthOwnerLocked(name, previous string, owner *Entry, dest *EntryHealth) error {
	if err := s.proveRetiredHealthOwnerLocked(previous); err != nil {
		return err
	}
	if err := s.ensureHealthIdentityIndexLocked(); err != nil {
		return err
	}
	sourceName, err := s.healthNameLocked(name)
	if err != nil {
		return err
	}
	source, err := s.getEntryHealthLocked(sourceName)
	if err != nil {
		return err
	}
	if rangeOwner(source) != previous {
		return ErrHealthIdentity
	}
	key, err := retiredHealthArchiveKey(source, previous)
	if err != nil {
		return err
	}
	if source.Archived && source.EntryName != key {
		return ErrHealthIdentity
	}
	next, err := survivorHealthState(name, previous, owner, source, max(dest.DirtyRevision, source.DirtyRevision))
	if err != nil {
		return err
	}
	archived, err := s.getEntryHealthLocked(key)
	if hybrid.IsNotFound(err) {
		copy := *source
		copy.EntryName, copy.OwnerInfoHash = key, previous
		copy.Archived, copy.ArchivedFrom = true, sourceName
		if err := s.putEntryHealthLocked(&copy); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		storedKey, err := retiredHealthArchiveKey(archived, previous)
		if err != nil {
			return err
		}
		if !archived.Archived || archived.OwnerInfoHash != previous || storedKey != key {
			return ErrHealthIdentity
		}
	}
	// Retry barriers even when an earlier failed sync left rows in memory.
	if err := s.syncHealthOwnerArchiveLocked("archive"); err != nil {
		return err
	}
	var aliases []string
	if err := s.repairState.ForEachMeta(func(key string, meta *hybrid.IndexEntry) error {
		if key != name && meta.Status == "alias" {
			aliases = append(aliases, key)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, alias := range aliases {
		h, err := s.getEntryHealthLocked(alias)
		if err != nil {
			return err
		}
		if h.OwnerInfoHash != "" && h.OwnerInfoHash != previous {
			continue
		}
		root, err := s.healthNameLocked(alias)
		if err != nil {
			return err
		}
		if root != sourceName && root != key {
			continue
		}
		if h.CanonicalEntry == key && h.OwnerInfoHash == previous && h.DirtyRevision >= source.DirtyRevision {
			continue
		}
		h.CanonicalEntry, h.OwnerInfoHash = key, previous
		h.DirtyRevision = max(h.DirtyRevision, source.DirtyRevision)
		if err := s.putEntryHealthLocked(h); err != nil {
			return err
		}
	}
	if err := s.syncHealthOwnerArchiveLocked("aliases"); err != nil {
		return err
	}
	if err := s.putEntryHealthLocked(next); err != nil {
		return err
	}
	return s.syncHealthOwnerArchiveLocked("current")
}

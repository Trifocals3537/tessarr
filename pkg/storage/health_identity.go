package storage

import (
	"errors"
	"maps"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
)

var ErrHealthIdentity = errors.New("health owner or canonical identity is unresolved")

func itemOwnedBy(item *EntryItem, hash string) bool {
	if item == nil || len(item.Files) == 0 {
		return false
	}
	for _, file := range item.Files {
		if file == nil || file.InfoHash != hash {
			return false
		}
	}
	return true
}

// ResolveHealthEntry serializes ownership proof and binding publication with
// main/index writes. Raw main reads deliberately avoid taking lifecycle locks
// underneath entryItemsMu; this operation grants no deletion authority.
func (s *Storage) ResolveHealthEntry(name string, naming config.WebDavFolderNaming) (*EntryItem, error) {
	s.entryItemsMu.Lock()
	defer s.entryItemsMu.Unlock()
	s.entryHealthMu.Lock()
	defer s.entryHealthMu.Unlock()
	return s.resolveHealthEntryLocked(name, naming)
}

func (s *Storage) resolveHealthEntryLocked(name string, naming config.WebDavFolderNaming) (*EntryItem, error) {
	raw, err := s.getEntryHealthLocked(name)
	if err != nil && !hybrid.IsNotFound(err) {
		return nil, err
	}
	item, itemErr := s.GetEntryItem(name)
	if itemErr != nil && !hybrid.IsNotFound(itemErr) {
		return nil, itemErr
	}
	if itemErr == nil && (raw == nil || raw.CanonicalEntry == "") {
		// Bind even an unchanged canonical key when it has a unique owner, so
		// a later main-entry rename cannot discard this key's read evidence.
		for _, file := range item.Files {
			if file != nil && itemOwnedBy(item, file.InfoHash) {
				owner, err := s.getMainRaw(file.InfoHash)
				if err != nil {
					return nil, err
				}
				if GetTorrentFolder(naming, owner) != name {
					return nil, ErrHealthIdentity
				}
				if err := s.bindOwnedHealthLocked(name, name, owner.InfoHash); err != nil {
					return nil, err
				}
				break
			}
		}
		return item, nil
	}
	var owner *Entry
	if itemErr == nil {
		for _, file := range item.Files {
			if file == nil {
				return nil, ErrHealthIdentity
			}
			if owner == nil {
				owner, err = s.getMainRaw(file.InfoHash)
				if err != nil {
					return nil, err
				}
			}
			if file.InfoHash != owner.InfoHash {
				return nil, ErrHealthIdentity
			}
		}
	} else {
		// Never replace the original runtime key with an old alias target before
		// looking up its current owner. Names may legitimately move or be reused.
		if err := s.entries.ForEach(func(key string, data []byte) error {
			if strings.HasPrefix(key, "__") {
				return nil
			}
			e, err := decodeMainEntry(key, data)
			if err != nil {
				return err
			}
			if e.Name != name {
				return nil
			}
			if owner != nil {
				return ErrHealthIdentity
			}
			owner = e
			return nil
		}); err != nil {
			return nil, err
		}
		if owner == nil && raw != nil && raw.OwnerInfoHash != "" {
			owner, err = s.getMainRaw(raw.OwnerInfoHash)
			if err != nil {
				return nil, err
			}
		}
	}
	if owner == nil {
		return nil, ErrHealthIdentity
	}
	intent, _, err := s.loadMainEntryTombstone(owner.InfoHash)
	if err != nil {
		return nil, err
	}
	if intent != nil {
		return nil, ErrEntryDeleting
	}
	canonical := GetTorrentFolder(naming, owner)
	item, err = s.GetEntryItem(canonical)
	if err != nil {
		return nil, err
	}
	if !itemOwnedBy(item, owner.InfoHash) {
		return nil, ErrHealthIdentity
	}
	if err := s.bindOwnedHealthLocked(name, canonical, owner.InfoHash); err != nil {
		return nil, err
	}
	return item, nil
}

func rangeOwner(h *EntryHealth) string {
	if h == nil {
		return ""
	}
	if h.OwnerInfoHash != "" {
		return h.OwnerInfoHash
	}
	var hash string
	for _, f := range h.FailedReads {
		if hash != "" && hash != f.InfoHash {
			return ""
		}
		hash = f.InfoHash
	}
	return hash
}

func (s *Storage) bindOwnedHealthLocked(alias, canonical, owner string) error {
	raw, err := s.getEntryHealthLocked(alias)
	if hybrid.IsNotFound(err) {
		raw = &EntryHealth{EntryName: alias, Status: HealthUnknown}
	} else if err != nil {
		return err
	}
	sourceName, source := alias, raw
	previousOwner := raw.OwnerInfoHash
	if raw.CanonicalEntry != "" {
		sourceName, err = s.healthNameLocked(alias)
		if err != nil {
			return err
		}
		source, err = s.getEntryHealthLocked(sourceName)
		if hybrid.IsNotFound(err) {
			source = nil
		} else if err != nil {
			return err
		}
		if previousOwner == "" {
			previousOwner = rangeOwner(source)
		}
		if previousOwner == "" && sourceName == canonical {
			previousOwner = owner
		}
		if previousOwner == "" {
			return ErrHealthIdentity
		}
	}
	// A reused runtime name may bind to a new unique owner. Its old target's
	// evidence remains with that old owner; it must never contaminate the new one.
	transfer := previousOwner == "" || previousOwner == owner
	if transfer && source != nil {
		for _, failed := range source.FailedReads {
			if failed.InfoHash != owner {
				return ErrHealthIdentity
			}
		}
	}
	dest, err := s.getEntryHealthLocked(canonical)
	if hybrid.IsNotFound(err) {
		dest = nil
	} else if err != nil {
		return err
	}
	if dest != nil && dest.CanonicalEntry != "" {
		// A rename back to a previously used key must retain that key's
		// revision fence against probes that started before the first rename.
		dest = &EntryHealth{EntryName: canonical, Status: HealthUnknown, DirtyRevision: dest.DirtyRevision}
	}
	if dest == nil {
		dest = &EntryHealth{EntryName: canonical, Status: HealthUnknown}
	}
	if dest.OwnerInfoHash != "" && dest.OwnerInfoHash != owner && protectedHealthState(dest) {
		return ErrHealthIdentity
	}
	if (raw.CanonicalEntry == canonical || alias == canonical) && raw.OwnerInfoHash == owner && sourceName == canonical && dest.OwnerInfoHash == owner {
		return s.repairState.Sync()
	}
	if sourceName == canonical && transfer {
		dest.OwnerInfoHash = owner
		if err := s.putEntryHealthLocked(dest); err != nil {
			return err
		}
	} else if transfer && source != nil {
		key := sourceName + "\x00" + owner
		if dest.AliasRevisions == nil {
			dest.AliasRevisions = make(map[string]uint64)
		}
		if rev, ok := dest.AliasRevisions[key]; !ok || rev < source.DirtyRevision {
			for filename, receipt := range source.FileRetirements {
				if current, ok := dest.FileRetirements[filename]; ok && !current.matches(&File{InfoHash: receipt.InfoHash, ID: receipt.FileID, Size: receipt.Size, ByteRange: receipt.ByteRange}) {
					return ErrHealthIdentity
				}
			}
			dest.Dirty, dest.Cleared = true, false
			dest.Status = HealthUnknown
			dest.DirtyReason = source.DirtyReason
			if dest.DirtyReason == "" {
				dest.DirtyReason = "health_identity_changed"
			}
			dest.DirtyRevision = max(dest.DirtyRevision, source.DirtyRevision) + 1
			dest.ProbeProgress = nil
			dest.NextCheckDueAt = time.Time{}
			for _, f := range source.FailedReads {
				addFailedRead(dest, f)
			}
			dest.FailedReadsIncomplete = dest.FailedReadsIncomplete || source.FailedReadsIncomplete || missingLegacyReadEvidence(source)
			if source.Protocol != "" {
				dest.Protocol = source.Protocol
			}
			if len(source.FileRetirements) > 0 {
				if dest.FileRetirements == nil {
					dest.FileRetirements = make(map[string]HealthFileRetirement)
				}
				maps.Copy(dest.FileRetirements, source.FileRetirements)
			}
			for k, v := range source.AliasRevisions {
				dest.AliasRevisions[k] = max(dest.AliasRevisions[k], v)
			}
			dest.AliasRevisions[key] = source.DirtyRevision
		}
		dest.OwnerInfoHash = owner
		if err := s.putEntryHealthLocked(dest); err != nil {
			return err
		}
	} else {
		dest.OwnerInfoHash = owner
		if err := s.putEntryHealthLocked(dest); err != nil {
			return err
		}
	}
	if err := s.repairState.Sync(); err != nil {
		return err
	}
	if s.healthAliasSyncedForTest != nil {
		s.healthAliasSyncedForTest()
	}
	if s.healthAliasPhaseForTest != nil {
		s.healthAliasPhaseForTest("target")
	}
	if transfer && source != nil && sourceName != canonical && sourceName != alias {
		if current, err := s.GetEntryItem(sourceName); err == nil && !itemOwnedBy(current, owner) {
			// The old canonical key now serves another owner. Only its old
			// evidence moved; do not redirect the new owner's health traffic.
			if err := s.putEntryHealthLocked(&EntryHealth{EntryName: sourceName, Status: HealthUnknown, Dirty: true, DirtyReason: "health_owner_changed", DirtyRevision: source.DirtyRevision + 1}); err != nil {
				return err
			}
		} else if err != nil && !hybrid.IsNotFound(err) {
			return err
		} else {
			if err := s.putEntryHealthLocked(&EntryHealth{EntryName: sourceName, CanonicalEntry: canonical, OwnerInfoHash: owner, Status: HealthUnknown, DirtyRevision: max(source.DirtyRevision, dest.DirtyRevision)}); err != nil {
				return err
			}
		}
		if s.healthAliasPhaseForTest != nil {
			s.healthAliasPhaseForTest("source")
		}
	}
	if alias != canonical {
		if err := s.putEntryHealthLocked(&EntryHealth{EntryName: alias, CanonicalEntry: canonical, OwnerInfoHash: owner, Status: HealthUnknown, DirtyRevision: max(raw.DirtyRevision, dest.DirtyRevision)}); err != nil {
			return err
		}
	}
	if err := s.repairState.Sync(); err != nil {
		return err
	}
	if s.healthAliasPhaseForTest != nil {
		s.healthAliasPhaseForTest("alias")
	}
	return nil
}

// RecordFailedReadAt returns the actual health key for event admission. A
// runtime name is display metadata; an exact current owner/file is stronger
// routing evidence and is checked under the index/health publication locks.
func (s *Storage) RecordFailedReadAt(name string, protocol config.Protocol, reason string, failed FailedReadRange) (string, error) {
	if name == "" {
		return "", nil
	}
	s.entryItemsMu.Lock()
	defer s.entryItemsMu.Unlock()
	s.entryHealthMu.Lock()
	defer s.entryHealthMu.Unlock()
	check := &EntryHealth{}
	addFailedRead(check, failed)
	if check.FailedReadsIncomplete {
		if err := s.markEntryDirtyLocked(name, protocol, reason, &failed); err != nil {
			return "", err
		}
		return name, nil
	}
	owner, err := s.getMainRaw(failed.InfoHash)
	if err == nil {
		canonical := GetTorrentFolder(config.Get().FolderNaming, owner)
		item, itemErr := s.GetEntryItem(canonical)
		if itemErr != nil && !hybrid.IsNotFound(itemErr) {
			return "", itemErr
		}
		if itemErr == nil {
			file := item.Files[failed.FileName]
			if file != nil && file.InfoHash == failed.InfoHash {
				if failed.FileID == "" {
					failed.FileID = file.ID
				}
				resolved, resolveErr := s.resolveHealthEntryLocked(owner.Name, config.Get().FolderNaming)
				if resolveErr == nil {
					if itemOwnedBy(resolved, failed.InfoHash) {
						canonical = resolved.Name
					}
				} else if !errors.Is(resolveErr, ErrHealthIdentity) && !hybrid.IsNotFound(resolveErr) {
					return "", resolveErr
				}
				h, healthErr := s.getEntryHealthLocked(canonical)
				if healthErr != nil && !hybrid.IsNotFound(healthErr) {
					return "", healthErr
				}
				if h != nil && file.Deleted {
					if receipt, ok := h.FileRetirements[failed.FileName]; ok && receipt.Confirmed && receipt.matches(file) && (failed.FileID == "" || failed.FileID == receipt.FileID) {
						return "", nil
					}
				}
				if err := s.markEntryDirtyLocked(canonical, protocol, reason, &failed); err != nil {
					return "", err
				}
				return canonical, nil
			}
		}
	} else if !IsEntryNotFound(err) {
		return "", err
	}
	// Intake/tests may report before an owner/index is materialized. Retain the
	// signal, but do not resurrect work after a durable main-entry retirement.
	if intent, _, err := s.loadMainEntryTombstone(failed.InfoHash); err != nil {
		return "", err
	} else if intent != nil {
		return "", nil
	}
	if err := s.markEntryDirtyLocked(name, protocol, reason, &failed); err != nil {
		return "", err
	}
	return name, nil
}

func (s *Storage) healthOwnerMovedLocked(h *EntryHealth) (bool, error) {
	if h.CanonicalEntry != "" {
		return true, nil
	}
	if h.OwnerInfoHash != "" {
		if owner, err := s.getMainRaw(h.OwnerInfoHash); err == nil {
			return owner.GetFolder() != h.EntryName, nil
		} else if !IsEntryNotFound(err) {
			return false, err
		}
	}
	return false, nil
}

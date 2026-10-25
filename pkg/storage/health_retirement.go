package storage

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
	"google.golang.org/protobuf/proto"
)

// HealthFileRetirement is an exact local-file intent, never authority to
// remove a provider transfer. The intent precedes the durable index tombstone;
// only that matching tombstone can acknowledge the corresponding read evidence.
type HealthFileRetirement struct {
	InfoHash  string    `json:"info_hash"`
	FileID    string    `json:"file_id"`
	Size      int64     `json:"size"`
	ByteRange *[2]int64 `json:"byte_range,omitempty"`
	Confirmed bool      `json:"confirmed"`
}

var ErrHealthRetirementIdentity = errors.New("file retirement identity changed")

func (r HealthFileRetirement) matches(f *File) bool {
	return f != nil && r.InfoHash == f.InfoHash && r.FileID == f.ID && r.Size == f.Size &&
		(r.ByteRange == nil && f.ByteRange == nil || r.ByteRange != nil && f.ByteRange != nil && *r.ByteRange == *f.ByteRange)
}

func retirementFor(f *File) HealthFileRetirement {
	r := HealthFileRetirement{InfoHash: f.InfoHash, FileID: f.ID, Size: f.Size}
	if f.ByteRange != nil {
		copied := *f.ByteRange
		r.ByteRange = &copied
	}
	return r
}

func sameRetirement(a, b HealthFileRetirement) bool {
	return a.matches(&File{InfoHash: b.InfoHash, ID: b.FileID, Size: b.Size, ByteRange: b.ByteRange})
}

// Superseded intents are audit/feedback evidence only, never replay authority.
// Keep confirmed old identities so delayed reads cannot dirty a replacement.
func archiveHealthRetirement(h *EntryHealth, name string, receipt HealthFileRetirement) {
	h.FileRetirementHistory = maps.Clone(h.FileRetirementHistory)
	if h.FileRetirementHistory == nil {
		h.FileRetirementHistory = make(map[string][]HealthFileRetirement)
	}
	old := slices.Clone(h.FileRetirementHistory[name])
	for i, prior := range old {
		if sameRetirement(prior, receipt) {
			old[i].Confirmed = prior.Confirmed || receipt.Confirmed
			h.FileRetirementHistory[name] = old
			return
		}
	}
	h.FileRetirementHistory[name] = append(old, receipt)
}

func mergeHealthRetirements(dest, source *EntryHealth, item *EntryItem) {
	for name, history := range source.FileRetirementHistory {
		for _, receipt := range history {
			archiveHealthRetirement(dest, name, receipt)
		}
	}
	for name, receipt := range source.FileRetirements {
		if dest.FileRetirements == nil {
			dest.FileRetirements = make(map[string]HealthFileRetirement)
		}
		if prior, ok := dest.FileRetirements[name]; ok {
			if sameRetirement(prior, receipt) {
				prior.Confirmed = prior.Confirmed || receipt.Confirmed
				dest.FileRetirements[name] = prior
				continue
			}
			if !receipt.matches(item.Files[name]) {
				archiveHealthRetirement(dest, name, receipt)
				continue
			}
			archiveHealthRetirement(dest, name, prior)
		}
		dest.FileRetirements[name] = receipt
	}
}

func (s *Storage) retiredReadLocked(failed FailedReadRange) (bool, error) {
	if failed.FileID == "" {
		return false, nil // An old name alone cannot identify a retired incarnation.
	}
	if err := s.ensureHealthIdentityIndexLocked(); err != nil {
		return false, err
	}
	found := false
	for _, key := range s.repairState.FilterByProvider("health-retirements") {
		h, err := s.getEntryHealthLocked(key)
		if err != nil {
			return false, err
		}
		matches := func(receipt HealthFileRetirement) bool {
			return receipt.Confirmed && receipt.InfoHash == failed.InfoHash && receipt.FileID == failed.FileID
		}
		if receipt, ok := h.FileRetirements[failed.FileName]; ok && matches(receipt) {
			found = true
		}
		for _, receipt := range h.FileRetirementHistory[failed.FileName] {
			found = found || matches(receipt)
		}
	}
	return found, nil
}

// RetireEntryItemFile serializes the exact file snapshot with parent deletion,
// index publication and health feedback. A crash at either write boundary is
// replayed only against the same logical file identity.
func (s *Storage) RetireEntryItemFile(name, filename string, expected *File) (*EntryItem, error) {
	return s.retireEntryItemFile(name, filename, expected, false)
}

func (s *Storage) retireEntryItemFile(name, filename string, expected *File, replay bool) (*EntryItem, error) {
	if expected == nil || expected.InfoHash == "" {
		return nil, fmt.Errorf("file retirement requires an owner")
	}
	ref, err := s.acquireMainEntryState(expected.InfoHash)
	if err != nil {
		return nil, err
	}
	defer ref.release()
	ref.state.mu.Lock()
	defer ref.state.mu.Unlock()
	if ref.state.deleting || ref.state.queueDeleting || ref.state.retired {
		return nil, ErrEntryDeleting
	}
	if intent, _, err := s.loadMainEntryTombstone(expected.InfoHash); err != nil {
		return nil, err
	} else if intent != nil {
		return nil, ErrEntryDeleting
	}
	s.entryItemsMu.Lock()
	defer s.entryItemsMu.Unlock()
	s.entryHealthMu.Lock()
	defer s.entryHealthMu.Unlock()
	item, err := s.GetEntryItem(name)
	if err != nil {
		return nil, err
	}
	f := item.Files[filename]
	identity := retirementFor(expected)
	if !identity.matches(f) {
		return nil, ErrHealthRetirementIdentity
	}
	h, err := s.getEntryHealthLocked(name)
	if hybrid.IsNotFound(err) {
		h = &EntryHealth{EntryName: name, Status: HealthUnknown}
	} else if err != nil {
		return nil, err
	}
	if h.CanonicalEntry != "" {
		return nil, fmt.Errorf("file retirement health identity changed")
	}
	if itemOwnedBy(item, expected.InfoHash) && h.OwnerInfoHash == "" {
		h.OwnerInfoHash = expected.InfoHash
	}
	existing, authorized := h.FileRetirements[filename]
	if replay && (!authorized || !existing.matches(f)) {
		return nil, ErrHealthRetirementIdentity
	}
	if f.Deleted && (!authorized || !existing.matches(f)) {
		return nil, fmt.Errorf("file already deleted without matching retirement receipt")
	}
	if !authorized || !existing.matches(f) {
		if authorized {
			archiveHealthRetirement(h, filename, existing)
		}
		h.FileRetirements = maps.Clone(h.FileRetirements)
		if h.FileRetirements == nil {
			h.FileRetirements = make(map[string]HealthFileRetirement)
		}
		h.FileRetirements[filename] = identity
		h.FailedReadsIncomplete = h.FailedReadsIncomplete || missingLegacyReadEvidence(h)
		h.Dirty, h.Cleared = true, false
		h.DirtyReason = "file_retirement_pending"
		h.DirtyRevision++
		h.ProbeProgress = nil
		if err := s.putEntryHealthLocked(h); err != nil {
			return nil, err
		}
	}
	// Retry the barrier even for an intent already visible in memory after a
	// failed sync. Uncertain publication cannot authorize an index mutation.
	if err := s.repairState.Sync(); err != nil {
		return nil, err
	}
	if s.healthRetirementForTest != nil {
		s.healthRetirementForTest("intent")
	}
	if !f.Deleted {
		if err := s.markEntryItemsDirtyLocked(); err != nil {
			return nil, err
		}
		f.Deleted = true
		item.Size = item.GetSize()
		data, err := proto.Marshal(EntryItemToProto(item))
		if err != nil {
			return nil, err
		}
		if err := s.entryItems.Put(name, data, nil); err != nil {
			return nil, err
		}
	}
	if err := s.entryItems.Sync(); err != nil {
		return nil, err
	}
	if s.healthRetirementForTest != nil {
		s.healthRetirementForTest("index")
	}
	h.FailedReads = slices.DeleteFunc(h.FailedReads, func(r FailedReadRange) bool {
		return r.InfoHash == identity.InfoHash && r.FileName == filename && (r.FileID == "" || r.FileID == identity.FileID)
	})
	h.BrokenFiles = slices.DeleteFunc(h.BrokenFiles, func(b BrokenFile) bool { return b.InfoHash == identity.InfoHash && b.FileName == filename })
	identity.Confirmed = true
	h.FileRetirements[filename] = identity
	h.Dirty, h.Cleared = true, false
	h.DirtyRevision++
	h.DirtyReason = "entry_file_retired"
	h.Status = HealthUnknown
	h.LastRepairAt = time.Now()
	h.NextCheckDueAt = time.Time{}
	h.ProbeProgress = nil
	if err := s.putEntryHealthLocked(h); err != nil {
		return nil, err
	}
	if err := s.repairState.Sync(); err != nil {
		return nil, err
	}
	if s.healthRetirementForTest != nil {
		s.healthRetirementForTest("ack")
	}
	return item, nil
}

// ReconcileHealthFileRetirements is also used before a recheck, so an explicit
// operator intent can finish after a temporary local storage failure.
func (s *Storage) ReconcileHealthFileRetirements(name string) (bool, error) {
	h, err := s.GetEntryHealth(name)
	if hybrid.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(h.FileRetirements) == 0 {
		return false, nil
	}
	if _, err := s.ResolveHealthEntry(name, config.Get().FolderNaming); err != nil {
		return false, err
	}
	h, err = s.GetEntryHealth(name)
	if err != nil {
		return false, err
	}
	changed := false
	for filename, receipt := range h.FileRetirements {
		if receipt.Confirmed {
			continue
		}
		expected := &File{InfoHash: receipt.InfoHash, ID: receipt.FileID, Size: receipt.Size, ByteRange: receipt.ByteRange}
		if _, err := s.retireEntryItemFile(h.EntryName, filename, expected, true); err != nil {
			return changed, err
		}
		changed = true
	}
	return changed, nil
}

func (s *Storage) reconcileHealthRetirements() error {
	var names []string
	if err := s.ForEachEntryHealth(func(h *EntryHealth) error {
		if len(h.FileRetirements) > 0 {
			names = append(names, h.EntryName)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, name := range names {
		if _, err := s.ReconcileHealthFileRetirements(name); err != nil {
			if errors.Is(err, ErrEntryDeleting) || errors.Is(err, ErrHealthRetirementIdentity) || errors.Is(err, ErrHealthIdentity) || IsEntryNotFound(err) || hybrid.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("reconcile health file retirement %q: %w", name, err)
		}
	}
	return nil
}

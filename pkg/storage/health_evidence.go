package storage

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
)

const maxFailedReadRanges = 64

func protectedHealthState(h *EntryHealth) bool {
	return h.Dirty || len(h.FailedReads) > 0 || h.FailedReadsIncomplete ||
		h.ProbeProgress != nil || h.ActiveRunID != "" || len(h.BrokenFiles) > 0 ||
		len(h.AliasRevisions) > 0 || len(h.FileRetirements) > 0 || len(h.FileRetirementHistory) > 0
}

func missingLegacyReadEvidence(h *EntryHealth) bool {
	return h.Dirty && strings.HasPrefix(h.DirtyReason, "read_failure") && len(h.FailedReads) == 0
}

// FailedReadRange uses logical file offsets, never provider/archive offsets.
type FailedReadRange struct {
	InfoHash string `json:"info_hash"`
	FileName string `json:"file_name"`
	Offset   int64  `json:"offset"`
	Length   int64  `json:"length"`
	FileID   string `json:"file_id,omitempty"`
}

// Only complete, successful file probes are reusable. Revision and both file
// set and source fingerprints fence them against replacement and newer reads.
type HealthProbeProgress struct {
	StartedAt   time.Time         `json:"started_at"`
	Revision    uint64            `json:"revision"`
	Fingerprint string            `json:"fingerprint"`
	Mode        string            `json:"mode"`
	Verified    map[string]string `json:"verified"`
}

func addFailedRead(h *EntryHealth, f FailedReadRange) {
	if f.InfoHash == "" || f.FileName == "" || f.Offset < 0 || f.Length <= 0 || f.Offset > int64(^uint64(0)>>1)-f.Length {
		h.FailedReadsIncomplete = true
		return
	}
	if slices.Contains(h.FailedReads, f) {
		return
	}
	if len(h.FailedReads) >= maxFailedReadRanges {
		h.FailedReadsIncomplete = true
		return
	}
	h.FailedReads = append(h.FailedReads, f)
}

func (s *Storage) RecordFailedRead(entryName string, protocol config.Protocol, reason string, failed FailedReadRange) error {
	_, err := s.RecordFailedReadAt(entryName, protocol, reason, failed)
	return err
}

func (s *Storage) healthNameLocked(name string) (string, error) {
	seen := make(map[string]bool)
	for {
		if seen[name] {
			return "", fmt.Errorf("cyclic health alias")
		}
		seen[name] = true
		h, err := s.getEntryHealthLocked(name)
		if hybrid.IsNotFound(err) {
			return name, nil
		}
		if err != nil {
			return "", err
		}
		if h.CanonicalEntry == "" {
			return name, nil
		}
		name = h.CanonicalEntry
	}
}

// BindHealthAlias is replayable across either write boundary: the canonical
// signal is synced before publishing a forwarding record. Source revisions
// prevent a crash between the writes from duplicating or losing the signal.
// The manager must first prove exact ownership of the unique canonical item.
func (s *Storage) BindHealthAlias(alias, canonical string) error {
	if alias == "" || canonical == "" || alias == canonical {
		return fmt.Errorf("invalid health alias")
	}
	s.entryHealthMu.Lock()
	defer s.entryHealthMu.Unlock()
	target, err := s.healthNameLocked(canonical)
	if err != nil {
		return err
	}
	if target != canonical {
		return fmt.Errorf("canonical health is itself an alias")
	}
	source, err := s.getEntryHealthLocked(alias)
	if hybrid.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if source.CanonicalEntry != "" {
		if source.CanonicalEntry == canonical {
			return nil
		}
		return fmt.Errorf("conflicting health alias")
	}
	dest, err := s.getEntryHealthLocked(canonical)
	if hybrid.IsNotFound(err) {
		dest = &EntryHealth{EntryName: canonical, Status: HealthUnknown}
	} else if err != nil {
		return err
	}
	if dest.AliasRevisions == nil {
		dest.AliasRevisions = make(map[string]uint64)
	}
	if revision, ok := dest.AliasRevisions[alias]; !ok || revision < source.DirtyRevision {
		if source.Dirty || len(source.FailedReads) > 0 || source.FailedReadsIncomplete {
			dest.Dirty = true
			dest.DirtyReason = source.DirtyReason
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
		}
		dest.AliasRevisions[alias] = source.DirtyRevision
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
	if err := s.putEntryHealthLocked(&EntryHealth{EntryName: alias, CanonicalEntry: canonical, Status: HealthUnknown}); err != nil {
		return err
	}
	return s.repairState.Sync()
}

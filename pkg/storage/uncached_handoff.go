package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
)

func (s *Storage) syncHandoffState() error {
	if s.handoffSyncForTest != nil {
		return s.handoffSyncForTest()
	}
	return s.storageState.Sync()
}

func (s *Storage) flushPendingHandoff(key string) error {
	if _, pending := s.handoffPending.Load(key); !pending {
		return nil
	}
	if err := s.syncHandoffState(); err != nil {
		return err
	}
	s.handoffPending.Delete(key)
	return nil
}

const handoffAuthorizationPrefix = "uncached_handoff/"

// FailedUncachedHandoff requires positive failure evidence, never inferred import
// success. Legacy rows without submitting-client evidence remain non-destructive.
func FailedUncachedHandoff(e *Entry) bool {
	if e == nil || !e.IsTorrent() || !e.DownloadUncached || e.State != EntryStateStalledDL || e.Category == "" || e.ClientEndpoint == "" || e.QueueIncarnation == "" {
		return false
	}
	switch e.HandoffReason {
	case "terminal", "metadata", "no_seeds", "stalled":
	default:
		return false
	}
	p := e.GetActiveProvider()
	return p != nil && p.ID != "" && p.Provider != "" && strings.EqualFold(p.Provider, e.ActiveProvider)
}

// The durable proof binds Arr validation to this exact incarnation, active
// placement and failure/progress evidence. Any fresh progress or owner change
// invalidates it without granting a later same-hash row deletion permission.
func handoffFingerprint(e *Entry) ([]byte, error) {
	return json.Marshal(struct {
		Incarnation, Owner, Endpoint, Provider, ID, Reason string
		State                                              TorrentState
		Progress                                           float64
		LastProgress                                       *time.Time
	}{e.QueueIncarnation, e.Category, e.ClientEndpoint, e.ActiveProvider, e.GetActiveProvider().ID, e.HandoffReason, e.State, e.Progress, e.LastProgressAt})
}

// AuthorizeUncachedHandoff is called only after Arr proves unique endpoint and
// queue ownership, before its synchronous DELETE callback. No deletion lease is
// retained across the Arr network request.
func (s *Storage) AuthorizeUncachedHandoff(expected *Entry) error {
	if !FailedUncachedHandoff(expected) {
		return fmt.Errorf("uncached handoff lacks failure ownership evidence")
	}
	ref, err := s.acquireMainEntryState(expected.InfoHash)
	if err != nil {
		return err
	}
	defer ref.release()
	ref.state.mu.Lock()
	defer ref.state.mu.Unlock()
	if ref.state.queueDeleting {
		return ErrQueuedEntryDeleting
	}
	current, err := s.getQueuedRaw(normalizeMainEntryKey(expected.InfoHash))
	if err != nil {
		return err
	}
	if !FailedUncachedHandoff(current) {
		return ErrQueuedDeletionIdentityMismatch
	}
	intent, err := s.submissionRaw(ref.key)
	if err != nil {
		return err
	}
	if intent != nil && intent.QueueIncarnation == current.QueueIncarnation && intent.Recovered && intent.Provider == current.ActiveProvider && intent.ProviderID == current.GetActiveProvider().ID {
		return ErrProviderSubmissionConflict
	}
	want, err := handoffFingerprint(expected)
	if err != nil {
		return err
	}
	got, err := handoffFingerprint(current)
	if err != nil {
		return err
	}
	if !bytes.Equal(want, got) {
		return ErrQueuedDeletionIdentityMismatch
	}
	key := handoffAuthorizationPrefix + normalizeMainEntryKey(expected.InfoHash)
	if err := s.flushPendingHandoff(key); err != nil {
		return err
	}
	if err := s.storageState.Put(key, got, nil); err != nil {
		return err
	}
	s.handoffPending.Store(key, struct{}{})
	return s.flushPendingHandoff(key)
}

// revokeChangedHandoffAuthorization runs under the queue key lock before a
// changed row is persisted. Revocation is durable even if progress later regresses.
func (s *Storage) revokeChangedHandoffAuthorization(e *Entry) error {
	key := handoffAuthorizationPrefix + normalizeMainEntryKey(e.InfoHash)
	if err := s.flushPendingHandoff(key); err != nil {
		return err
	}
	proof, err := s.storageState.Get(key)
	if hybrid.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if FailedUncachedHandoff(e) {
		current, err := handoffFingerprint(e)
		if err != nil {
			return err
		}
		if bytes.Equal(current, proof) {
			return nil
		}
	}
	if err := s.storageState.Delete(key); err != nil && !hybrid.IsNotFound(err) {
		return err
	}
	s.handoffPending.Store(key, struct{}{})
	return s.flushPendingHandoff(key)
}

func (s *Storage) authorizedTransferSnapshot(e *Entry) (*Entry, error) {
	if err := s.flushPendingHandoff(handoffAuthorizationPrefix + normalizeMainEntryKey(e.InfoHash)); err != nil {
		return nil, err
	}
	if !FailedUncachedHandoff(e) {
		return nil, nil
	}
	proof, err := s.storageState.Get(handoffAuthorizationPrefix + normalizeMainEntryKey(e.InfoHash))
	if hybrid.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	current, err := handoffFingerprint(e)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(proof, current) {
		return nil, nil
	}
	snapshot := *e
	placement := *e.GetActiveProvider()
	snapshot.Providers = map[string]*ProviderEntry{e.ActiveProvider: &placement}
	if err := s.ValidateTransferCleanup(&snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

// ValidateTransferCleanup also rechecks collisions on retry/recovery. The
// durable scope must never expand to newly discovered main placements.
func (s *Storage) ValidateTransferCleanup(e *Entry) error {
	if e == nil || len(e.Providers) != 1 {
		return fmt.Errorf("invalid transfer cleanup snapshot")
	}
	p := e.GetActiveProvider()
	if p == nil || p.Provider == "" || p.ID == "" {
		return fmt.Errorf("invalid active transfer placement")
	}
	// Provider IDs can back library symlinks even for a different hash. Scan
	// raw main snapshots rather than reentering this key's lifecycle lock.
	return s.entries.ForEach(func(key string, data []byte) error {
		if key == "__migration_status__" {
			return nil
		}
		main, err := decodeEntryForKey(key, data, ErrQueuedDeletionIdentityMismatch, "main collision check")
		if err != nil {
			return err
		}
		for _, other := range main.Providers {
			if other != nil && strings.EqualFold(other.Provider, p.Provider) && other.ID == p.ID {
				return fmt.Errorf("failed transfer placement is shared with main storage; manual review required")
			}
		}
		return nil
	})
}

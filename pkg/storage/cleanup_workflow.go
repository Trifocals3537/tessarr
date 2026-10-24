package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Trifocals3537/tessarr/internal/workflow"
	"github.com/google/uuid"
)

// ErrCleanupIntentObsolete identifies only an authoritative missing/replaced
// intent. Corrupt snapshot identity errors must still fail admission closed.
var ErrCleanupIntentObsolete = errors.New("cleanup intent has completed or been replaced")

// CleanupIntent reads the current immutable intent for an explicit retry. The
// subsequent claim still checks its incarnation/revision under the queue gate.
func (s *Storage) CleanupIntent(key string) (*QueueDeletionIntent, error) {
	ref, err := s.acquireMainEntryState(key)
	if err != nil {
		return nil, err
	}
	defer ref.release()
	ref.state.mu.Lock()
	defer ref.state.mu.Unlock()
	t, found, err := s.loadQueueDeletionTombstone(ref.key)
	if err != nil || !found {
		return nil, err
	}
	return queueDeletionIntentFromTombstone(ref.key, t)
}

type CleanupAdminIntent struct {
	Entry    *Entry
	snapshot []byte
}

func (s *Storage) loadCleanupAdmin(key string) (*CleanupAdminIntent, error) {
	if err := s.syncMainCleanupState(); err != nil {
		return nil, err
	}
	t, found, err := s.loadMainEntryTombstone(key)
	if err != nil || !found {
		return nil, err
	}
	if len(t.PlacementSnapshot) == 0 {
		return nil, nil
	}
	entry, err := decodeMainEntry(key, t.PlacementSnapshot)
	if err != nil {
		return nil, err
	}
	return &CleanupAdminIntent{Entry: entry, snapshot: bytes.Clone(t.PlacementSnapshot)}, nil
}

// PendingAdministrativeCleanup supplies account-binding metadata at first
// adoption. Actual execution/completion still requires the queue claim token.
func (s *Storage) PendingAdministrativeCleanup(key string) (*CleanupAdminIntent, error) {
	ref, err := s.acquireMainEntryState(key)
	if err != nil {
		return nil, err
	}
	defer ref.release()
	ref.state.mu.Lock()
	defer ref.state.mu.Unlock()
	return s.loadCleanupAdmin(ref.key)
}

func (s *Storage) withCleanupClaim(i *QueueDeletionIntent, action func(string) error) error {
	if i == nil || i.Work == nil {
		return workflow.ErrClaim
	}
	ref, err := s.acquireMainEntryState(i.InfoHash)
	if err != nil {
		return err
	}
	defer ref.release()
	ref.state.mu.Lock()
	defer ref.state.mu.Unlock()
	t, found, err := s.loadQueueDeletionTombstone(ref.key)
	if err != nil {
		return err
	}
	if !found || t.QueueIncarnation != i.QueueIncarnation {
		return ErrQueuedDeletionIdentityMismatch
	}
	if err := cleanupToken(t, i.Work.Token); err != nil {
		return err
	}
	return action(ref.key)
}

// CleanupAdminIntent loads only the independent, previously authorized main
// snapshot. It never discovers additional placements in the current main row.
func (s *Storage) CleanupAdminIntent(i *QueueDeletionIntent) (*CleanupAdminIntent, error) {
	var result *CleanupAdminIntent
	err := s.withCleanupClaim(i, func(key string) error {
		var err error
		result, err = s.loadCleanupAdmin(key)
		return err
	})
	return result, err
}

func (s *Storage) CompleteCleanupAdmin(i *QueueDeletionIntent, expected *CleanupAdminIntent) error {
	if expected == nil || len(expected.snapshot) == 0 {
		return workflow.ErrClaim
	}
	return s.withCleanupClaim(i, func(key string) error {
		if err := s.syncMainCleanupState(); err != nil {
			return err
		}
		t, found, err := s.loadMainEntryTombstone(key)
		if err != nil {
			return err
		}
		if !found || !bytes.Equal(t.PlacementSnapshot, expected.snapshot) {
			return workflow.ErrClaim
		}
		t.PlacementSnapshot = nil
		data, err := json.Marshal(t)
		if err != nil {
			return err
		}
		if err := s.entryTombstones.Put(key, data, nil); err != nil {
			return err
		}
		return s.syncMainCleanupState()
	})
}

func firstCleanupToken(tokens []string) string {
	if len(tokens) == 1 {
		return tokens[0]
	}
	return ""
}
func cleanupToken(t *queueDeletionTombstone, token string) error {
	if t.Work == nil {
		if token == "" {
			return nil
		}
		return workflow.ErrClaim
	}
	return t.Work.Check(token)
}

// mutateCleanupWork publishes only after fsync. The queue incarnation and work
// revision live in the same authoritative record as immutable cleanup scope.
func (s *Storage) mutateCleanupWork(key, incarnation string, edit func(*queueDeletionTombstone) error) error {
	ref, err := s.acquireMainEntryState(key)
	if err != nil {
		return err
	}
	defer ref.release()
	ref.state.mu.Lock()
	defer ref.state.mu.Unlock()
	t, found, err := s.loadQueueDeletionTombstone(ref.key)
	if err != nil {
		return err
	}
	if !found || incarnation == "" || t.QueueIncarnation != incarnation {
		return ErrQueuedDeletionIdentityMismatch
	}
	if err := edit(t); err != nil {
		return err
	}
	if err := t.Work.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if err := s.queueTombstones.PutExistingSynced(ref.key, data, nil); err != nil {
		return fmt.Errorf("checkpoint cleanup workflow: %w", err)
	}
	return nil
}

func (s *Storage) ClaimCleanup(key, incarnation, owner string, accounts map[string]string, now time.Time, restart bool) (*QueueDeletionIntent, error) {
	token := uuid.NewString()
	var claimed *QueueDeletionIntent
	err := s.mutateCleanupWork(key, incarnation, func(t *queueDeletionTombstone) error {
		if t.Work == nil {
			t.Work = workflow.NewRecord(now)
			// Old cleanup_started rows may already have sent a provider request.
			t.Work.EffectStarted = t.PlacementCleanupPending && t.Phase != queueDeletionPrepared
			t.Work.Accounts = accounts
		}
		if t.Work.Attempt == 0 && t.Work.Accounts == nil {
			t.Work.Accounts = accounts
		}
		if err := t.Work.Claim(owner, token, now, restart); err != nil {
			return err
		}
		var err error
		claimed, err = queueDeletionIntentFromTombstone(normalizeMainEntryKey(key), t)
		return err
	})
	if err == ErrQueuedDeletionIdentityMismatch {
		return nil, errors.Join(ErrCleanupIntentObsolete, err)
	}
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

func (s *Storage) CleanupEffectStarted(key, incarnation, token string, now time.Time) error {
	return s.mutateCleanupWork(key, incarnation, func(t *queueDeletionTombstone) error {
		if err := cleanupToken(t, token); err != nil {
			return err
		}
		t.Work.EffectStarted = true
		t.Work.Revision++
		t.Work.UpdatedAt = now
		return nil
	})
}
func (s *Storage) ReleaseCleanup(key, incarnation, token, reason string, blocked bool, now time.Time) error {
	return s.mutateCleanupWork(key, incarnation, func(t *queueDeletionTombstone) error { return t.Work.Release(token, reason, blocked, now) })
}
func (s *Storage) ResolveCleanup(key, incarnation string, revision uint64, id, action string, now time.Time) error {
	if err := s.DurabilityError(); err != nil {
		return err
	}
	err := s.mutateCleanupWork(key, incarnation, func(t *queueDeletionTombstone) error {
		if t.Work == nil {
			if revision != 0 {
				return workflow.ErrClaim
			}
			t.Work = workflow.NewRecord(now)
			revision = t.Work.Revision
			t.Work.EffectStarted = t.PlacementCleanupPending && t.Phase != queueDeletionPrepared
		}
		return t.Work.Resolve(revision, id, action, now)
	})
	if err != ErrQueuedDeletionIdentityMismatch {
		return err
	}
	// A retried HTTP request may arrive after the job completed. Its receipt
	// acknowledges only that exact old incarnation/decision, never a replacement.
	digest := sha256.Sum256([]byte(normalizeMainEntryKey(key) + "\x00" + incarnation))
	data, readErr := s.storageState.Get(fmt.Sprintf("cleanup_receipt/%x", digest))
	if readErr != nil {
		return err
	}
	var receipt CleanupReceipt
	if json.Unmarshal(data, &receipt) != nil || receipt.Key != normalizeMainEntryKey(key) || receipt.Incarnation != incarnation || receipt.Work == nil {
		return err
	}
	for _, decision := range receipt.Work.Decisions {
		if decision.ID == id && decision.Action == action {
			return nil
		}
	}
	return err
}

func (s *Storage) CleanupAdminPending(key string) (bool, error) {
	ref, err := s.acquireMainEntryState(key)
	if err != nil {
		return false, err
	}
	defer ref.release()
	ref.state.mu.Lock()
	defer ref.state.mu.Unlock()
	if err := s.syncMainCleanupState(); err != nil {
		return false, err
	}
	t, found, err := s.loadMainEntryTombstone(ref.key)
	if err != nil {
		return false, err
	}
	return found && len(t.PlacementSnapshot) != 0, nil
}
func (s *Storage) StartCleanup(i *QueueDeletionIntent) error {
	return s.updateQueuedDeletion(i.InfoHash, i.QueueIncarnation, func(t *queueDeletionTombstone) error {
		if t.Phase == queueDeletionPrepared {
			t.Phase = queueDeletionCleanupStarted
		}
		return nil
	}, i.Work.Token)
}
func (s *Storage) RetireCleanup(i *QueueDeletionIntent) error {
	return s.retireQueuedDeletionRow(i.InfoHash, i.QueueIncarnation, i.Work.Token)
}
func (s *Storage) CleanupPlacementsClean(i *QueueDeletionIntent) error {
	return s.updateQueuedDeletion(i.InfoHash, i.QueueIncarnation, func(t *queueDeletionTombstone) error { t.PlacementCleanupPending = false; return nil }, i.Work.Token)
}

type CleanupReceipt struct {
	Key          string           `json:"key"`
	Incarnation  string           `json:"incarnation"`
	Work         *workflow.Record `json:"work"`
	CheckpointAt time.Time        `json:"checkpoint_at"`
	// A receipt is written before removing the intent. Callers must inspect
	// IntentPresent; a receipt alone never asserts that completion committed.
	IntentPresent bool `json:"intent_present"`
}

func (s *Storage) checkpointCleanupReceipt(key string, t *queueDeletionTombstone) error {
	receipt := CleanupReceipt{Key: key, Incarnation: t.QueueIncarnation, Work: t.Work, CheckpointAt: time.Now()}
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	id := sha256.Sum256([]byte(key + "\x00" + t.QueueIncarnation))
	if err := s.storageState.Put(fmt.Sprintf("cleanup_receipt/%x", id), data, nil); err != nil {
		return err
	}
	if err := s.storageState.Sync(); err != nil {
		return err
	}
	if s.cleanupReceiptForTest != nil {
		s.cleanupReceiptForTest()
	}
	return nil
}
func (s *Storage) CompleteCleanup(i *QueueDeletionIntent, local func() error) error {
	if local == nil {
		return s.completeQueuedDeletion(i.InfoHash, i.QueueIncarnation, i.Work.Token)
	}
	return s.completeQueuedDeletion(i.InfoHash, i.QueueIncarnation, i.Work.Token, local)
}
func (s *Storage) CleanupReceipts(limit int) ([]CleanupReceipt, error) {
	if limit < 1 || limit > 100 {
		limit = 100
	}
	result := []CleanupReceipt{}
	err := s.storageState.ForEach(func(key string, data []byte) error {
		if len(result) >= limit || len(key) < 16 || key[:16] != "cleanup_receipt/" {
			return nil
		}
		var r CleanupReceipt
		if err := json.Unmarshal(data, &r); err != nil {
			return err
		}
		t, found, err := s.loadQueueDeletionTombstone(r.Key)
		if err != nil {
			return err
		}
		r.IntentPresent = found && t.QueueIncarnation == r.Incarnation
		result = append(result, r)
		return nil
	})
	return result, err
}
